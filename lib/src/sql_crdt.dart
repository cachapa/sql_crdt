import 'dart:async';

import 'package:crdt/crdt.dart';

import 'crdt_executor.dart';
import 'database_api.dart';
import 'sql_util.dart';

typedef Query = (String sql, List<Object?> args);

abstract class SqlCrdt extends Crdt {
  final DatabaseApi _db;

  // final Map<String, Iterable<String>> _tables;
  final _watches = <StreamController<List<Map<String, dynamic>>>, _Query>{};

  /// Make sure you run [init] after instantiation.
  SqlCrdt(this._db);

  /// Initialize this CRDT
  Future<void> init() async {
    // Read the canonical time from database, or generate a new node id if empty
    canonicalTime = await _getLastModified() ?? Hlc.zero(generateNodeId());
  }

  /// Returns all the user tables in this database.
  Future<Iterable<String>> getTables();

  /// Returns all the keys for the specified table.
  Future<Iterable<String>> getTableKeys(String table);

  /// Performs a SQL query with optional [args] and returns the result as a list
  /// of column maps.
  /// Use "?" placeholders for parameters to avoid injection vulnerabilities:
  ///
  /// ```
  /// final result = await crdt.query(
  ///   'SELECT id, name FROM users WHERE id = ?1', [1]);
  /// print(result.isEmpty ? 'User not found' : result.first['name']);
  /// ```
  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]) =>
      _db.query(sql, args);

  /// Executes a SQL query with optional [args].
  /// Use "?" placeholders for parameters to avoid injection vulnerabilities:
  ///
  /// ```
  /// await crdt.execute(
  ///   'INSERT INTO users (id, name) Values (?1, ?2)', [1, 'John Doe']);
  /// ```
  Future<void> execute(String sql, [List<Object?>? args]) async {
    final executor = CrdtExecutor(_db, canonicalTime.increment());
    await executor.execute(sql, args);
    await onDatasetChanged(executor.affectedTables, executor.hlc);
  }

  /// Initiates a transaction in this database.
  /// Caution: calls to the parent crdt inside a transaction block will result
  /// in a deadlock.
  ///
  /// ```
  /// await database.transaction((txn) async {
  ///   // OK
  ///   await txn.execute('SELECT * FROM users');
  ///
  ///   // NOT OK: calls to the parent crdt in a transaction
  ///   // The following code will deadlock
  ///   await crdt.execute('SELECT * FROM users');
  /// });
  Future<void> transaction(
      Future<void> Function(CrdtExecutor txn) action) async {
    late final CrdtExecutor executor;
    await _db.transaction((txn) async {
      executor = CrdtExecutor(txn, canonicalTime.increment());
      await action(executor);
    });
    if (executor.affectedTables.isNotEmpty) {
      await onDatasetChanged(executor.affectedTables, executor.hlc);
    }
  }

  /// Performs a live SQL query with optional [args] and returns the result as a
  /// list of column maps.
  ///
  /// See [query].
  Stream<List<Map<String, Object?>>> watch(String sql,
      [List<Object?> Function()? args]) {
    late final StreamController<List<Map<String, Object?>>> controller;
    controller = StreamController<List<Map<String, Object?>>>(
      onListen: () {
        final query = _Query(sql, args);
        _watches[controller] = query;
        _emitQuery(controller, query);
      },
      onCancel: () {
        _watches.remove(controller);
        controller.close();
      },
    );

    return controller.stream;
  }

  @override
  Future<CrdtChangeset> getChangeset({
    Map<String, Query>? customQueries,
    Iterable<String>? onlyTables,
    String? onlyNodeId,
    String? exceptNodeId,
    Hlc? modifiedOn,
    Hlc? modifiedAfter,
  }) async {
    assert(onlyNodeId == null || exceptNodeId == null);
    assert(modifiedOn == null || modifiedAfter == null);

    // Modified times use the local node id
    modifiedOn = modifiedOn?.apply(nodeId: nodeId);
    modifiedAfter = modifiedAfter?.apply(nodeId: nodeId);

    var tables = onlyTables ?? await getTables();

    if (customQueries != null) {
      // Filter out any tables not explicitly mentioned by custom queries
      tables = tables.toSet().intersection(customQueries.keys.toSet());
    } else {
      // Use default changeset queries if none are provided
      customQueries = {
        for (final table in tables) table: ('SELECT * FROM $table', [])
      };
    }

    return {
      for (final table in tables)
        table: await _db.query(
            SqlUtil.addChangesetClauses(
              table,
              customQueries[table]!.$1,
              onlyNodeId: onlyNodeId,
              exceptNodeId: exceptNodeId,
              modifiedOn: modifiedOn,
              modifiedAfter: modifiedAfter,
            ),
            customQueries[table]!.$2),
    };
  }

  @override
  Future<void> merge(CrdtChangeset changeset) async {
    if (changeset.recordCount == 0) return;

    // Validate changeset and highest hlc therein
    final hlc = validateChangeset(changeset);

    // Fetch keys for all affected tables
    final tableKeys = {
      for (final table in changeset.keys) table: await getTableKeys(table)
    };

    // Merge records
    await _db.executeBatch((executor) async {
      for (final entry in changeset.entries) {
        final table = entry.key;
        final records = entry.value;
        final keys = tableKeys[table]!.join(', ');

        for (final record in records) {
          // Extract node id from the record's hlc
          record['node_id'] = (record['hlc'] is String
                  ? (record['hlc'] as String).toHlc
                  : (record['hlc'] as Hlc))
              .nodeId;
          // Ensure hlc and modified are strings
          record['hlc'] = record['hlc'].toString();
          record['modified'] = hlc.toString();

          final columns = record.keys.join(', ');
          final placeholders =
              List.generate(record.length, (i) => '?${i + 1}').join(', ');

          var i = 1;
          final updateStatement =
              record.keys.map((e) => '$e = ?${i++}').join(', \n');

          final sql = '''
            INSERT INTO $table ($columns)
              VALUES ($placeholders)
            ON CONFLICT ($keys) DO
              UPDATE SET $updateStatement
            WHERE excluded.hlc > $table.hlc
          ''';
          await executor.execute(sql, record.values.toList());
        }
      }
    });

    await onDatasetChanged(changeset.keys, hlc);
  }

  /// Changes the node id.
  /// This can be useful e.g. when the user logs out and logs in with a new
  /// account without resetting the database - id avoids synchronization issues
  /// where the existing entries do not get correctly propagated to the new
  /// user id.
  Future<void> resetNodeId() async {
    final oldNodeId = canonicalTime.nodeId;
    final newNodeId = generateNodeId();
    await _db.executeBatch(
      (txn) async {
        for (final table in await getTables()) {
          await txn.execute(
            'UPDATE $table SET modified = REPLACE(modified, ?1, ?2)',
            [oldNodeId, newNodeId],
          );
        }
      },
    );
    canonicalTime = canonicalTime.apply(nodeId: newNodeId);
  }

  @override
  Future<void> onDatasetChanged(
      Iterable<String> affectedTables, Hlc hlc) async {
    super.onDatasetChanged(affectedTables, hlc);

    for (final entry in _watches.entries.toList()) {
      final controller = entry.key;
      final query = entry.value;
      if (affectedTables
          .firstWhere((e) => query.affectedTables.contains(e), orElse: () => '')
          .isNotEmpty) {
        await _emitQuery(controller, query);
      }
    }
  }

  @override
  Future<Hlc> getLastModified(
          {String? onlyNodeId, String? exceptNodeId}) async =>
      (await _getLastModified(
          onlyNodeId: onlyNodeId, exceptNodeId: exceptNodeId)) ??
      Hlc.zero(nodeId);

  Future<Hlc?> _getLastModified(
      {String? onlyNodeId, String? exceptNodeId}) async {
    assert(onlyNodeId == null || exceptNodeId == null);

    final tables = await getTables();
    if (tables.isEmpty) return null;

    final whereStatement = onlyNodeId != null
        ? 'WHERE node_id = ?1'
        : exceptNodeId != null
            ? 'WHERE node_id != ?1'
            : '';
    final tableStatements = tables.map((table) =>
        'SELECT max(modified) AS modified FROM $table $whereStatement');
    final result = await _db.query('''
      SELECT max(modified) AS modified FROM (
        ${tableStatements.join('\nUNION ALL\n')}
      ) all_tables
    ''', [
      if (onlyNodeId != null) onlyNodeId,
      if (exceptNodeId != null) exceptNodeId,
    ]);
    return (result.firstOrNull?['modified'] as String?)?.toHlc;
  }

  Future<void> _emitQuery(
      StreamController<List<Map<String, dynamic>>> controller,
      _Query query) async {
    final result = await _db.query(query.sql, query.args?.call());
    if (!controller.isClosed) {
      controller.add(result);
    }
  }
}

class _Query {
  final String sql;
  final List<Object?> Function()? args;
  final Set<String> affectedTables;

  _Query(this.sql, this.args) : affectedTables = SqlUtil.getAffectedTables(sql);
}

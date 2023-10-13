part of 'base_crdt.dart';

typedef Query = (String sql, List<Object?> args);

abstract class SqlCrdt extends TimestampedCrdt with Crdt {
  final _watches = <StreamController<List<Map<String, dynamic>>>, _Query>{};

  /// Returns all the user tables in this database.
  late Future<Iterable<String>> allTables = _db.getTables();

  /// Make sure you run [init] after instantiation.
  /// Use [changesetQueries] if you want to specify a custom query to generate
  /// changesets.
  /// Defaults to a simple `SELECT *` for each table in the database.
  SqlCrdt(super.db);

  /// Initialize this CRDT
  Future<void> init() async {
    // Read the canonical time from database, or generate a new node id if empty
    await _getLastModified();
    canonicalTime = await _getLastModified() ?? Hlc.zero(generateNodeId());
  }

  /// migrate an existing database to support drift_crdt
  Future<void> migrate() async {
    canonicalTime = Hlc.zero(generateNodeId());
    final tables = await _db.getTables();
    if (tables.isEmpty) return;

    // write a query that adds CRDT columns to tables
    final tableStatements = [];
    for (var table in tables) {
      tableStatements.add('ALTER TABLE $table ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0;');
      tableStatements.add('ALTER TABLE $table ADD COLUMN hlc TEXT NOT NULL DEFAULT \'${canonicalTime.toString()}\';');
      tableStatements.add('ALTER TABLE $table ADD COLUMN node_id TEXT NOT NULL DEFAULT \'${canonicalTime.nodeId}\';');
      tableStatements.add('ALTER TABLE $table ADD COLUMN modified TEXT NOT NULL DEFAULT \'${canonicalTime.toString()}\';');
    }

    // run the query on the database as batch
    await _db.transaction((txn) async {
      for (final statement in tableStatements) {
        await txn.execute(statement);
      }
    });
  }

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = canonicalTime.increment();
    await super._insert(statement, args, hlc);
    await onDatasetChanged([statement.table.tableName], hlc);
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = canonicalTime.increment();
    await super._update(statement, args, hlc);
    await onDatasetChanged([statement.table.tableName], hlc);
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = canonicalTime.increment();
    await super._delete(statement, args, hlc);
    await onDatasetChanged([statement.table.tableName], hlc);
  }

  @override
  Future<int> _rawInsert(InsertStatement statement, [List<Object?>? args, Hlc? hlc]) async {
    final hlc = canonicalTime.increment();
    final result = await super._rawInsert(statement, args);
    canonicalTime = hlc;
    return result;
  }

  @override
  Future<int> _rawUpdate(UpdateStatement statement, [List<Object?>? args, Hlc? hlc]) async {
    final hlc = canonicalTime.increment();
    final result = await super._rawUpdate(statement, args);
    canonicalTime = hlc;
    return result;
  }

  @override
  Future<int> _rawDelete(DeleteStatement statement, [List<Object?>? args, Hlc? hlc]) async {
    final hlc = canonicalTime.increment();
    final result = await super._rawDelete(statement, args);
    canonicalTime = hlc;
    return result;
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

    var tables = onlyTables ?? await allTables;

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

    // Validate changeset and get new canonical time
    final hlc = validateChangeset(changeset);

    // Merge records
    await _db.transaction((txn) async {
      for (final entry in changeset.entries) {
        final table = entry.key;
        final records = entry.value;
        final keys = (await txn.getPrimaryKeys(table)).join(', ');

        for (final record in records) {
          // Convert record's HLC from String if necessary
          record['node_id'] = (record['hlc'] is String
                  ? (record['hlc'] as String).toHlc
                  : (record['hlc'] as Hlc))
              .nodeId;
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
          await txn.execute(sql, record.values.map(_convert).toList());
        }
      }
    });

    await onDatasetChanged(changeset.keys, hlc);
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
      Future<void> Function(TransactionCrdt txn) action) async {
    late final TransactionCrdt transaction;
    await _db.transaction((txn) async {
      transaction = TransactionCrdt(txn, canonicalTime.increment());
      await action(transaction);
    });
    // Notify on changes
    if (transaction.affectedTables.isNotEmpty) {
      await onDatasetChanged(
          transaction.affectedTables, transaction.canonicalTime);
    }
  }

  /// Changes the node id.
  /// This can be useful e.g. when the user logs out and logs in with a new
  /// account without resetting the database - id avoids synchronization issues
  /// where the existing entries do not get correctly propagated to the new
  /// user id.
  Future<void> resetNodeId() async {
    final oldNodeId = canonicalTime.nodeId;
    final newNodeId = generateNodeId();
    await _db.transaction(
      (txn) async {
        for (final table in await txn.getTables()) {
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

    final tables = await _db.getTables();
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
    final result = await this.query(query.sql, query.args?.call());
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

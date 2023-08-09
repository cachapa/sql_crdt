part of 'base_crdt.dart';

String _uuid() => Uuid().v4();

abstract class SqlCrdt extends TimestampedCrdt {
  late Hlc _canonicalTime;
  late final _onTablesChangedController =
      StreamController<({Hlc hlc, Iterable<String> tables})>.broadcast();

  final _watches = <StreamController<List<Map<String, dynamic>>>, _Query>{};

  /// Changes the node id.
  /// This can be useful e.g. when the user logs out and logs in with a new
  /// account without resetting the database - id avoids synchronization issues
  /// where the existing entries do not get correctly propagated to the new
  /// user id.
  Future<void> resetNodeId() async {
    final oldNodeId = _canonicalTime.nodeId;
    final newNodeId = _uuid();
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
    _canonicalTime = _canonicalTime.apply(nodeId: newNodeId);
  }

  @override
  Hlc get canonicalTime => _canonicalTime;

  /// Returns all the user tables in this database.
  Future<Iterable<String>> get allTables async => await _db.getTables();

  /// Emits a list of the tables affected by changes in the database and the
  /// timestamp at which they happened.
  /// Useful for guaranteeing atomic merges across multiple tables.
  Stream<({Hlc hlc, Iterable<String> tables})> get onTablesChanged =>
      _onTablesChangedController.stream;

  /// Returns the last modified timestamp, optionally filtering for or against a
  /// specific node id.
  /// Useful to get "modified since" timestamps for synchronization.
  /// Returns [Hlc.zero] if no timestamp is found.
  Future<Hlc> lastModified({String? onlyNodeId, String? excludeNodeId}) async =>
      await _lastModified(
          onlyNodeId: onlyNodeId, excludeNodeId: excludeNodeId) ??
      Hlc.zero(nodeId);

  Future<Hlc?> _lastModified(
      {String? onlyNodeId, String? excludeNodeId}) async {
    assert(onlyNodeId == null || excludeNodeId == null);

    final tables = await _db.getTables();
    if (tables.isEmpty) return null;

    final whereStatement = onlyNodeId != null
        ? 'WHERE node_id = ?1'
        : excludeNodeId != null
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
      if (excludeNodeId != null) excludeNodeId,
    ]);
    return (result.firstOrNull?['modified'] as String?)?.toHlc;
  }

  /// Make sure you run [init] after instantiation.
  SqlCrdt(super.db);

  /// Compute and cache the last modified date.
  Future<void> init() async {
    // Generate a node id if there are no existing records
    _canonicalTime = await _lastModified() ?? Hlc.zero(_uuid());
  }

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = _canonicalTime.increment();
    await super._insert(statement, args, hlc);
    await _onDbChanged([statement.table.tableName], hlc);
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = _canonicalTime.increment();
    await super._update(statement, args, hlc);
    await _onDbChanged([statement.table.tableName], hlc);
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = _canonicalTime.increment();
    await super._delete(statement, args, hlc);
    await _onDbChanged([statement.table.tableName], hlc);
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

  /// Returns all CRDT records in the database.
  /// Use [fromTables] to specify from which tables to read, returns all tables if null.
  /// Use [modifiedSince] to fetch only recently changed records.
  /// Set [onlyModifiedBy] to get only records changed by the specified node id.
  /// Set [exceptModifiedBy] to ignore records changed by the specified node id.
  Future<Map<String, Iterable<Map<String, Object?>>>> getChangeset({
    Iterable<String>? fromTables,
    Hlc? modifiedSince,
    String? onlyModifiedBy,
    String? exceptModifiedBy,
  }) async {
    // Ensure we're using the local node id for comparisons
    modifiedSince = modifiedSince?.apply(nodeId: nodeId);

    var i = 1;
    final conditions = [
      if (modifiedSince != null) 'modified > ?${i++}',
      if (onlyModifiedBy != null) 'node_id = ?${i++}',
      if (exceptModifiedBy != null) 'node_id != ?${i++}',
    ];
    final conditionClause =
        conditions.isEmpty ? '' : 'WHERE ${conditions.join(' AND ')}';

    return {
      for (final table in fromTables ?? await _db.getTables())
        table: await _db.query('SELECT * FROM $table $conditionClause', [
          if (modifiedSince != null) modifiedSince.toString(),
          if (onlyModifiedBy != null) onlyModifiedBy,
          if (exceptModifiedBy != null) exceptModifiedBy,
        ])
    }..removeWhere((_, records) => records.isEmpty);
  }

  /// Returns all CRDT records in the database.
  /// Use [fromTables] to specify from which tables to read, returns all tables if null.
  /// Use [modifiedSince] to fetch only recently changed records.
  /// Set [onlyModifiedHere] to get only records changed in this node.
  Stream<Map<String, Iterable<Map<String, Object?>>>> watchChangeset({
    Iterable<String>? fromTables,
    Hlc? Function()? modifiedSince,
    String? onlyModifiedBy,
    String? exceptModifiedBy,
  }) {
    // Build a synthetic query to watch [fromTables]
    return (fromTables != null
            ? Stream.value(fromTables)
            : _db.getTables().asStream())
        .asyncExpand((tables) => watch(
                '${tables.map((e) => 'SELECT is_deleted FROM $e').join('\nUNION ALL\n')} LIMIT 1')
            .asyncMap((_) => getChangeset(
                  fromTables: fromTables,
                  // Ensure we're using the local node id for comparisons
                  modifiedSince: modifiedSince?.call(),
                  onlyModifiedBy: onlyModifiedBy,
                  exceptModifiedBy: exceptModifiedBy,
                )));
  }

  /// Merge [changeset] into database
  Future<void> merge(
      Map<String, Iterable<Map<String, Object?>>> changeset) async {
    if (changeset.recordCount == 0) return;

    var hlc = _canonicalTime;
    await _db.transaction((txn) async {
      // Iterate through all the remote timestamps to
      // 1. Check for invalid entries (throws exception)
      // 2. Update local canonical time if needed
      changeset.forEach((table, records) {
        for (final record in records) {
          try {
            hlc = hlc.merge((record['hlc'] as String).toHlc);
          } catch (e) {
            throw MergeError(e, table, record);
          }
        }
      });

      for (final entry in changeset.entries) {
        final table = entry.key;
        final records = entry.value;
        final keys = (await txn.getPrimaryKeys(table)).join(', ');

        for (final record in records) {
          record['node_id'] = (record['hlc'] as String).toHlc.nodeId;
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
          await txn.execute(sql, record.values.toList());
        }
      }
    });

    await _onDbChanged(changeset.keys, hlc);
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
      transaction = TransactionCrdt(txn, _canonicalTime.increment());
      await action(transaction);
    });
    // Notify on changes
    if (transaction.affectedTables.isNotEmpty) {
      await _onDbChanged(transaction.affectedTables, transaction.canonicalTime);
    }
  }

  Future<void> _onDbChanged(Iterable<String> affectedTables, Hlc hlc) async {
    // Bump canonical time if the new timestamp is higher
    if (hlc > _canonicalTime) _canonicalTime = hlc;

    _onTablesChangedController.add((hlc: hlc, tables: affectedTables));

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

  Future<void> _emitQuery(
      StreamController<List<Map<String, dynamic>>> controller,
      _Query query) async {
    final result = await this.query(query.sql, query.args?.call());
    if (!controller.isClosed) {
      controller.add(result);
    }
  }
}

/// Thrown on merge errors. Contains the failed payload to help with debugging
/// large datasets.
class MergeError {
  final Object error;
  final String table;
  final Map<String, Object?> record;

  MergeError(this.error, this.table, this.record);

  @override
  String toString() => '$error\n$table: $record';
}

class _Query {
  final String sql;
  final List<Object?> Function()? args;
  final Set<String> affectedTables;

  _Query(this.sql, this.args) : affectedTables = SqlUtil.getAffectedTables(sql);
}

extension MapX on Map<String, Iterable<Map<String, Object?>>> {
  /// Convenience method to get number of records in a changeset
  int get recordCount => values.fold<int>(0, (prev, e) => prev + e.length);
}

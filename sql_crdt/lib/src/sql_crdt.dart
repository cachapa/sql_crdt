part of 'base_crdt.dart';

class SqlCrdt extends TimestampedCrdt {
  Hlc _canonicalTime;

  final _watches = <StreamController<List<Map<String, dynamic>>>, _Query>{};

  @override
  Hlc get canonicalTime => _canonicalTime;

  /// Returns the last modified timestamp from other peers
  Future<Hlc?> get peerLastModified => lastModified(excludeNodeId: nodeId);

  /// Returns the last modified timestamp, optionally filtering for or against a
  /// specific node id.
  /// Useful to get "modified since" timestamps for synchronization.
  Future<Hlc?> lastModified({String? onlyNodeId, String? excludeNodeId}) =>
      _lastModified(_db, onlyNodeId: onlyNodeId, excludeNodeId: excludeNodeId);

  static Future<Iterable<String>> _getTables(DatabaseApi db) => db.getTables();

  static Future<Iterable<String>> _getKeys(
          DatabaseApi executor, String table) =>
      executor.getPrimaryKeys(table);

  static Future<Hlc?> _lastModified(DatabaseApi db,
      {String? onlyNodeId, String? excludeNodeId}) async {
    assert(onlyNodeId == null || excludeNodeId == null);

    final tables = await _getTables(db);
    if (tables.isEmpty) return null;

    final whereStatement = onlyNodeId != null
        ? "WHERE hlc LIKE '%' || ?1"
        : excludeNodeId != null
            ? "WHERE hlc NOT LIKE '%' || ?1"
            : '';
    final tableStatements = tables.map((table) =>
        'SELECT max(modified) AS modified FROM $table $whereStatement');
    final result = await db.query('''
      SELECT max(modified) AS modified FROM (
        ${tableStatements.join('\nUNION ALL\n')}
      ) all_tables
    ''', [
      if (onlyNodeId != null) onlyNodeId,
      if (excludeNodeId != null) excludeNodeId,
    ]);
    return (result.first['modified'] as String?)?.toHlc;
  }

  SqlCrdt(super.db, this._canonicalTime);

  static Future<SqlCrdt> open(DatabaseApi db) async {
    // Get existing node id, or generate one
    final canonicalTime = await _lastModified(db);
    return SqlCrdt(db, canonicalTime ?? Hlc.zero(Uuid().v4()));
  }

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = _canonicalTime.increment();
    await super._insert(statement, args, hlc);
    _canonicalTime = hlc;
    await _onDbChanged([statement.table.tableName]);
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = _canonicalTime.increment();
    await super._update(statement, args, hlc);
    _canonicalTime = hlc;
    await _onDbChanged([statement.table.tableName]);
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final hlc = _canonicalTime.increment();
    await super._delete(statement, args, hlc);
    _canonicalTime = hlc;
    await _onDbChanged([statement.table.tableName]);
  }

  Stream<List<Map<String, Object?>>> watch(String sql,
      [List<Object?> Function()? args]) {
    late final StreamController<List<Map<String, Object?>>> controller;
    controller = StreamController<List<Map<String, Object?>>>(
      onListen: () {
        final query = _Query(sql, args?.call());
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
  /// Set [onlyModifiedHere] to get only records changed in this node.
  Future<Map<String, Iterable<Map<String, Object?>>>> getChangeset(
      {Iterable<String>? fromTables,
      Hlc? modifiedSince,
      bool onlyModifiedHere = false}) async {
    final conditions = [
      if (modifiedSince != null) "modified > '$modifiedSince'",
      if (onlyModifiedHere) "hlc LIKE '%$nodeId'",
    ];
    final conditionClause =
        conditions.isEmpty ? '' : 'WHERE ${conditions.join(' AND ')}';

    return {
      for (final table in fromTables ?? await _getTables(_db))
        table: await _db.query('SELECT * FROM $table $conditionClause')
    }..removeWhere((_, records) => records.isEmpty);
  }

  /// Returns all CRDT records in the database.
  /// Use [fromTables] to specify from which tables to read, returns all tables if null.
  /// Use [modifiedSince] to fetch only recently changed records.
  /// Set [onlyModifiedHere] to get only records changed in this node.
  Stream<Map<String, Iterable<Map<String, Object?>>>> watchChangeset(
      {Iterable<String>? fromTables,
      Hlc? Function()? modifiedSince,
      bool onlyModifiedHere = false}) {
    // Build a synthetic query to watch [fromTables]
    return (fromTables != null
            ? Stream.value(fromTables)
            : _getTables(_db).asStream())
        .asyncExpand((tables) => watch(
                '${tables.map((e) => 'SELECT is_deleted FROM $e').join('\nUNION ALL\n')} LIMIT 1')
            .asyncMap((_) => getChangeset(
                fromTables: fromTables,
                modifiedSince: modifiedSince?.call(),
                onlyModifiedHere: onlyModifiedHere)));
  }

  /// Merge [changeset] into database
  Future<void> merge(
      Map<String, Iterable<Map<String, Object?>>> changeset) async {
    var hlc = _canonicalTime;
    await _db.transaction((txn) async {
      // Iterate through all the remote timestamps to
      // 1. Check for invalid entries (throws exception)
      // 2. Update local canonical time if needed
      for (final records in changeset.values) {
        hlc = records.fold<Hlc>(
            hlc, (hlc, record) => hlc.merge((record['hlc'] as String).toHlc));
      }

      for (final entry in changeset.entries) {
        final table = entry.key;
        final records = entry.value;
        final keys = (await _getKeys(txn, table)).join(', ');

        for (final record in records) {
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

    await _onDbChanged(changeset.keys);
    _canonicalTime = hlc;
  }

  Future<void> transaction(
      Future<void> Function(TransactionCrdt txn) action) async {
    late final TransactionCrdt transaction;
    await _db.transaction((txn) async {
      transaction = TransactionCrdt(txn, _canonicalTime.increment());
      await action(transaction);
    });
    _canonicalTime = transaction.canonicalTime;
    await _onDbChanged(transaction.affectedTables);
  }

  Future<void> _onDbChanged(Iterable<String> affectedTables) async {
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
    final result = await this.query(query.sql, query.args);
    if (!controller.isClosed) {
      controller.add(result);
    }
  }
}

class _Query {
  final String sql;
  final List<Object?>? args;
  final Set<String> affectedTables;

  _Query(this.sql, this.args) : affectedTables = SqlUtil.getAffectedTables(sql);
}

extension MapX on Map<String, Iterable<Map<String, Object?>>> {
  /// Convenience method to get number of records in a changeset
  int get recordCount => values.fold<int>(0, (prev, e) => prev + e.length);
}

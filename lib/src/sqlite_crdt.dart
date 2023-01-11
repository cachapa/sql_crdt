part of 'base_crdt.dart';

final _sqlEngine = SqlEngine();

class SqliteCrdt extends TimestampedCrdt {
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
      _lastModified(_executor,
          onlyNodeId: onlyNodeId, excludeNodeId: excludeNodeId);

  static Future<Iterable<String>> _getTables(DatabaseExecutor db) async =>
      (await db.rawQuery('''
        SELECT name FROM sqlite_schema
        WHERE type ='table' AND name NOT LIKE 'sqlite_%'
      ''')).map((e) => e['name'] as String);

  static Future<Iterable<String>> _getKeys(
          DatabaseExecutor executor, String table) async =>
      (await executor.rawQuery('''
         SELECT name FROM pragma_table_info("$table")
         WHERE pk > 0
       ''')).map((e) => e['name'] as String);

  static Future<Hlc?> _lastModified(DatabaseExecutor db,
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
    final result = await db.rawQuery(
      '''
      SELECT max(modified) AS modified FROM (
        ${tableStatements.join('\nUNION ALL\n')}
      )
    ''',
      [
        if (onlyNodeId != null) onlyNodeId,
        if (excludeNodeId != null) excludeNodeId,
      ],
    );
    return (result.first['modified'] as String?)?.toHlc;
  }

  SqliteCrdt(super.executor, this._canonicalTime);

  static Future<SqliteCrdt> open(
    String basePath,
    String name, {
    bool singleInstance = true,
    int? version,
    FutureOr<void> Function(BaseCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(BaseCrdt crdt, int from, int to)? onUpgrade,
  }) =>
      _open(
          basePath, name, false, singleInstance, version, onCreate, onUpgrade);

  static Future<SqliteCrdt> openInMemory({
    bool singleInstance = true,
    int? version,
    FutureOr<void> Function(BaseCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(BaseCrdt crdt, int from, int to)? onUpgrade,
  }) =>
      _open(null, null, true, singleInstance, version, onCreate, onUpgrade);

  static Future<SqliteCrdt> _open(
    String? basePath,
    String? name,
    bool inMemory,
    bool singleInstance,
    int? version,
    FutureOr<void> Function(BaseCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(BaseCrdt crdt, int from, int to)? onUpgrade,
  ) async {
    assert((basePath != null && name != null) ^ inMemory);

    // Initialize FFI
    sqfliteFfiInit();
    if (Platform.isLinux) {
      await databaseFactoryFfi.setDatabasesPath('.');
    }

    final db = await databaseFactoryFfi.openDatabase(
      inMemory ? inMemoryDatabasePath : '$basePath/$name.db',
      options: SqfliteOpenDatabaseOptions(
        singleInstance: singleInstance,
        version: version,
        onCreate: onCreate == null
            ? null
            : (db, version) => onCreate.call(BaseCrdt(db), version),
        onUpgrade: onUpgrade == null
            ? null
            : (db, from, to) => onUpgrade.call(BaseCrdt(db), from, to),
      ),
    );

    // Get existing node id, or generate one
    final canonicalTime = await _lastModified(db);
    return SqliteCrdt(db, canonicalTime ?? Hlc.zero(Uuid().v4()));
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
      for (final table in fromTables ?? await _getTables(_executor))
        table: await _executor.rawQuery('SELECT * FROM $table $conditionClause')
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
            : _getTables(_executor).asStream())
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
    await (_executor as Database).transaction((txn) async {
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
    await (_executor as Database).transaction((txn) async {
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

class SqlUtil {
  SqlUtil._();

  static Set<String> getAffectedTables(String sql) =>
      _getAffectedTables(_sqlEngine.parse(sql).rootNode as BaseSelectStatement);

  static Set<String> _getAffectedTables(AstNode node) {
    if (node is TableReference) return {node.tableName};
    return node.allDescendants
        .fold({}, (prev, e) => prev..addAll(_getAffectedTables(e)));
  }
}

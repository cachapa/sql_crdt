import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqflite_common/sqlite_api.dart';

// ignore: implementation_imports
import 'package:sqflite_common/src/open_options.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:sqlite_crdt/src/util/uuid.dart';

import 'hlc.dart';

class SqliteCrdt {
  final Database _db;
  Hlc canonicalTime;
  late final Map<String, Iterable<_Column>> _schemas;

  final _allChanges = StreamController<void>.broadcast();
  final _watches = <StreamController<List<Map<String, dynamic>>>, _Query>{};

  String get nodeId => canonicalTime.nodeId;

  /// Returns the last modified timestamp from other peers
  Future<Hlc?> get peerLastModified => lastModified(excludeNodeId: nodeId);

  Stream<void> get allChanges => _allChanges.stream;

  Iterable<String> get tables => _schemas.keys;

  /// Returns the last modified timestamp, optionally filtering for or against a
  /// specific node id.
  /// Useful to get "modified since" timestamps for synchronization.
  Future<Hlc?> lastModified({String? onlyNodeId, String? excludeNodeId}) =>
      _latestModified(_db, tables,
          onlyNodeId: onlyNodeId, excludeNodeId: excludeNodeId);

  static Future<Hlc?> _latestModified(Database db, Iterable<String> tables,
      {String? onlyNodeId, String? excludeNodeId}) async {
    assert(onlyNodeId == null || excludeNodeId == null);

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

  SqliteCrdt._(this._db, this.canonicalTime);

  static Future<SqliteCrdt> open(
    String basePath,
    String name,
    Iterable<String> tables, {
    bool inMemory = false,
    int? version,
    OnDatabaseCreateFn? onCreate,
    OnDatabaseVersionChangeFn? onUpgrade,
  }) async {
    // Initialize FFI
    sqfliteFfiInit();
    if (Platform.isLinux) {
      await databaseFactoryFfi.setDatabasesPath('.');
    }

    final db = await databaseFactoryFfi.openDatabase(
      inMemory ? inMemoryDatabasePath : '$basePath/$name.db',
      options: SqfliteOpenDatabaseOptions(
        version: version,
        onCreate: onCreate,
        onUpgrade: onUpgrade,
      ),
    );

    // Get existing node id, or generate one
    final canonicalTime = await _latestModified(db, tables);
    final crdt = SqliteCrdt._(db, canonicalTime ?? Hlc.zero(uuid()));

    // Read schemas directly from database
    crdt._schemas = {
      for (final table in tables) table: await _getTableColumns(db, table)
    };

    return crdt;
  }

  // TODO Maybe remove this method?
  Iterable<String> getPrimaryKeys(String table) =>
      _schemas[table]!.where((e) => e.isPrimaryKey).map((e) => e.name);

  // TODO Check statements for INSERT, UPDATE, DELETE and trigger watches
  // Future<void> execute(String sql, [List<Object?>? args]) =>
  //     _db.execute(sql, args);

  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]) =>
      _db.rawQuery(sql, args?.map(_encode).toList());

  Stream<List<Map<String, Object?>>> watch(String sql, [List<Object?>? args]) {
    late final StreamController<List<Map<String, dynamic>>> controller;
    controller = StreamController<List<Map<String, dynamic>>>(
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

  /// Insert a new record in the database.
  /// See [insertAll], [insertAllTable], [update].
  Future<void> insert(String table, Map<String, Object?> record) =>
      insertAllTable(table, [record]);

  /// Insert new records in the database.
  /// See [insert], [insertAllTable], [update].
  Future<void> insertAll(
      Map<String, Iterable<Map<String, Object?>>> records) async {
    final count = records.values.fold<int>(0, (prev, e) => prev + e.length);
    if (count == 0) return;

    await beginTransaction();
    canonicalTime = Hlc.send(canonicalTime);
    for (final entry in records.entries) {
      final table = entry.key;
      final records = entry.value;

      final columns =
          [...records.first.keys, 'is_deleted', 'hlc', 'modified'].join(', ');
      final placeholders =
          List.generate(records.first.length + 3, (i) => '?${i + 1}')
              .join(', ');

      for (final record in records) {
        final values = [...record.values, false, canonicalTime, canonicalTime]
            .map(_encode)
            .toList();
        final sql = '''
          INSERT INTO $table ($columns)
          VALUES ($placeholders)
        ''';
        await _db.execute(sql, values);
      }
    }
    await commitTransaction();
    await _onDbChanged();
  }

  /// Insert new records into a table in the database.
  /// See [insert], [insertAll], [update].
  Future<void> insertAllTable(
      String table, Iterable<Map<String, Object?>> records) async {
    if (records.isEmpty) return;
    return insertAll({table: records});
  }

  /// Update [fields] in an existing value with [ids].
  /// Set [isDeleted] to true if the value is to be marked as deleted.
  /// Note: data is never actually deleted from the database since CRDT deletions need to be propagated.
  /// Fields need to be overwritten if purging data is required.
  Future<void> update(
      String table, Iterable<Object> ids, Map<String, Object?> fields,
      [bool isDeleted = false]) async {
    // Find primary key fields
    final keyCols =
        _schemas[table]!.where((e) => e.isPrimaryKey).map((e) => e.name);
    assert(keyCols.length == ids.length);

    canonicalTime = Hlc.send(canonicalTime);
    final crdtFields = {
      ...fields,
      'is_deleted': isDeleted,
      'hlc': canonicalTime,
      'modified': canonicalTime,
    };

    var i = 1;
    final updateStatement =
        crdtFields.keys.map((e) => '"$e" = ?${i++}').join(', \n');
    final whereStatement = keyCols.map((e) => '"$e" = ?${i++}').join(' AND \n');

    final sql = '''
      UPDATE "$table" SET
        $updateStatement
      WHERE
        $whereStatement
    ''';

    final values = [...crdtFields.values, ...ids].map(_encode).toList();
    await _db.execute(sql, values);
    await _onDbChanged();
  }

  /// Marks record as deleted in the CRDT. Set [isDeleted] to false to restore.
  /// Convenience method for [update].
  Future<void> setDeleted(String table, List<Object> ids,
          [bool isDeleted = true]) =>
      update(table, ids, {}, isDeleted);

  /// Returns all CRDT records in database.
  /// Use [modifiedSince] to fetch only recently changed records.
  /// Set [onlyModifiedHere] to get only records changed in this node.
  Future<Map<String, Iterable<Map<String, Object?>>>> getChangeset(
          {Iterable<String>? fromTables,
          Hlc? modifiedSince,
          bool onlyModifiedHere = false}) async =>
      {
        for (final table in fromTables ?? tables)
          table: await getTableChangeset(
            table,
            modifiedSince: modifiedSince,
            onlyModifiedHere: onlyModifiedHere,
          )
      }..removeWhere((_, records) => records.isEmpty);

  /// Returns all records in [table].
  /// See [getChangeset].
  Future<Iterable<Map<String, Object?>>> getTableChangeset(String table,
      {Hlc? modifiedSince, bool onlyModifiedHere = false}) async {
    final conditions = [
      if (modifiedSince != null) "modified > '$modifiedSince'",
      if (onlyModifiedHere) "hlc LIKE '%$nodeId'",
    ];
    final conditionClause =
        conditions.isEmpty ? '' : 'WHERE ${conditions.join(' AND ')}';

    return await _db.rawQuery('SELECT * FROM $table $conditionClause');
  }

  /// Merge [changeset] into all tables.
  Future<void> merge(
      Map<String, Iterable<Map<String, Object?>>> changeset) async {
    await beginTransaction();

    // Iterate through all the remote timestamps to
    // 1. Check for invalid entries (throws exception)
    // 2. Update local canonical time if needed
    var hlc = canonicalTime;
    for (final records in changeset.values) {
      hlc = records.fold<Hlc>(hlc,
          (hlc, record) => Hlc.recv(hlc, Hlc.parse(record['hlc'] as String)));
    }
    canonicalTime = hlc;

    for (final entry in changeset.entries) {
      final table = entry.key;
      final records = entry.value;

      final columnNames = _schemas[table]!.map((e) => e.name).toSet();
      for (final record in records) {
        record['modified'] = canonicalTime;
        record.removeWhere((key, _) => !columnNames.contains(key));

        final columns = record.keys.join(', ');
        final placeholders =
            List.generate(record.length, (i) => '?${i + 1}').join(', ');
        final values = record.values.map(_encode).toList();

        var i = 1;
        final updateStatement =
            record.keys.map((e) => '$e = ?${i++}').join(', \n');

        final sql = '''
          INSERT INTO $table ($columns)
            VALUES ($placeholders)
          ON CONFLICT DO
            UPDATE SET $updateStatement
          WHERE excluded.hlc > $table.hlc
        ''';
        await _db.execute(sql, values);
      }
    }

    await commitTransaction();
    await _onDbChanged();
  }

  /// Merge [changeset] into [table].
  /// See [merge].
  Future<void> mergeTable(
          String table, Iterable<Map<String, Object?>> changeset) =>
      merge({table: changeset});

  var _transactionCount = 0;

  Future<void> beginTransaction() async {
    if (_transactionCount == 0) await _db.execute('BEGIN TRANSACTION');
    _transactionCount++;
  }

  Future<void> commitTransaction() async {
    _transactionCount--;
    if (_transactionCount == 0) await _db.execute('COMMIT');
  }

  Future<void> _onDbChanged() async {
    _allChanges.add(null);
    for (final entry in _watches.entries.toList()) {
      await _emitQuery(entry.key, entry.value);
    }
  }

  Future<void> _emitQuery(
      StreamController<List<Map<String, dynamic>>> controller,
      _Query query) async {
    final result =
        await _db.rawQuery(query.sql, query.args?.map(_encode).toList());
    if (!controller.isClosed) {
      controller.add(result);
    }
  }
}

Object? _encode(Object? value) {
  if (value == null) return null;
  if (value is Map) return jsonEncode(value);
  if (value is Enum) return value.name;

  switch (value.runtimeType) {
    case String:
    case int:
    case double:
      return value;
    case bool:
      return (value as bool) ? 1 : 0;
    case DateTime:
      return (value as DateTime).toUtc().toIso8601String();
    case Hlc:
      return value.toString();
    default:
      throw 'Unsupported type: ${value.runtimeType}';
  }
}

Future<Iterable<_Column>> _getTableColumns(Database db, String table) async =>
    (await db.rawQuery('SELECT name, pk FROM pragma_table_info(?1)', [table]))
        .map((e) => _Column(e['name'] as String, e['pk'] != 0));

class _Column {
  final String name;
  final bool isPrimaryKey;

  _Column(this.name, this.isPrimaryKey);

  @override
  String toString() => '$name${isPrimaryKey ? ' [PK]' : ''}';
}

class _Query {
  final String sql;
  final List<Object?>? args;

  _Query(this.sql, this.args);
}

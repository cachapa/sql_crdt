import 'package:sqflite_common/sqlite_api.dart';
import 'package:sql_crdt/sql_crdt.dart';

class SqliteApi extends DatabaseApi {
  final DatabaseExecutor _db;

  SqliteApi(this._db);

  @override
  Future<Iterable<String>> getTables() async => (await _db.rawQuery('''
        SELECT name FROM sqlite_schema
        WHERE type ='table' AND name NOT LIKE 'sqlite_%'
      ''')).map((e) => e['name'] as String);

  @override
  Future<Iterable<String>> getPrimaryKeys(String table) async =>
      (await _db.rawQuery('''
         SELECT name FROM pragma_table_info(?1)
         WHERE pk > 0
       ''', [table])).map((e) => e['name'] as String);

  @override
  Future<void> execute(String sql, [List<Object?>? args]) =>
      _db.execute(sql, args);

  @override
  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]) =>
      _db.rawQuery(sql, args);

  @override
  Future<void> transaction(
      Future<void> Function(DatabaseApi txn) action) async {
    assert(_db is Database, 'Cannot start a transaction within a transaction');
    return (_db as Database).transaction((t) => action(SqliteApi(t)));
  }
}

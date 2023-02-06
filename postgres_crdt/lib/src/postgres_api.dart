import 'package:postgres/postgres.dart';
import 'package:sql_crdt/sql_crdt.dart';

class PostgresApi extends DatabaseApi {
  final PostgreSQLExecutionContext _db;

  PostgresApi(this._db);

  @override
  Future<Iterable<String>> getTables() async => (await query('''
    SELECT table_name FROM information_schema.tables
    WHERE table_type='BASE TABLE' AND table_schema='public'
  ''')).map((e) => e['table_name'] as String?).whereType<String>();

  @override
  Future<Iterable<String>> getPrimaryKeys(String table) async =>
      (await query('''
        SELECT a.attname AS name
        FROM
          pg_class AS c
          JOIN pg_index AS i ON c.oid = i.indrelid AND i.indisprimary
          JOIN pg_attribute AS a ON c.oid = a.attrelid AND a.attnum = ANY(i.indkey)
        WHERE c.oid = ?1::regclass
      ''', [table])).map((e) => e['name'] as String);

  @override
  Future<void> execute(String sql, [List<Object?>? args]) => _db.execute(
        sql.replaceAll('?', '@'),
        substitutionValues: args?.toArgsMap,
      );

  @override
  Future<List<Map<String, Object?>>> query(String sql,
          [List<Object?>? args]) async =>
      (await _db.query(
        sql.replaceAll('?', '@'),
        substitutionValues: args?.toArgsMap,
      ))
          .map((e) => e.toColumnMap())
          .toList();

  @override
  Future<void> transaction(Future<void> Function(DatabaseApi txn) action) {
    assert(_db is PostgreSQLConnection,
        'Cannot start a transaction within a transaction');
    return (_db as PostgreSQLConnection)
        .transaction((t) => action(PostgresApi(t)));
  }
}

extension on List<Object?> {
  Map<String, Object?> get toArgsMap =>
      {for (var i = 0; i < length; i++) '${i + 1}': this[i]};
}

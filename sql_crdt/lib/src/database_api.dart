abstract class DatabaseApi {
  Future<void> execute(String sql, [List<Object?>? args]);

  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]);

  Future<Iterable<String>> getTables();

  Future<Iterable<String>> getPrimaryKeys(String table);

  Future<void> transaction(Future<void> Function(DatabaseApi txn) action);
}

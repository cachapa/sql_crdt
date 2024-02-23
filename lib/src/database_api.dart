import 'dart:async';

/// Interface representing a "standard" database backend capable of read, write
/// and transaction operations.
abstract class DatabaseApi implements ReadWriteApi {
  /// Initiates a transaction in this database.
  /// Caution: calls to the parent crdt inside a transaction block will result
  /// in a deadlock.
  ///
  /// await database.transaction((txn) async {
  ///   // OK
  ///   await txn.execute('SELECT * FROM users');
  ///
  ///   // NOT OK: calls to the parent crdt in a transaction
  ///   // The following code will deadlock
  ///   await crdt.execute('SELECT * FROM users');
  /// });
  Future<void> transaction(Future<void> Function(ReadWriteApi api) actions);

  /// Executes multiple writes atomically in this database, important for
  /// merging large datasets efficiently.
  ///
  /// Defaults to using transactions but can be extended if a more appropriate
  /// method is available, e.g. Sqlite batches or Postgres prepared statements.
  Future<void> executeBatch(Future<void> Function(WriteApi api) actions) =>
      transaction(actions);
}

/// Interface implementing read and write operations on the underlying database.
abstract class ReadWriteApi implements ReadApi, WriteApi {}

/// Interface implementing read operations on the underlying database.
abstract class ReadApi {
  /// Performs a SQL query with optional [args] and returns the result as a list
  /// of column maps.
  /// Use "?" placeholders for parameters to avoid injection vulnerabilities:
  ///
  /// ```
  /// final result = await crdt.query(
  ///   'SELECT id, name FROM users WHERE id = ?1', [1]);
  /// print(result.isEmpty ? 'User not found' : result.first['name']);
  /// ```
  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]);
}

/// Interface implementing write operations on the underlying database.
abstract class WriteApi {
  /// Executes a SQL query with optional [args].
  /// Use "?" placeholders for parameters to avoid injection vulnerabilities:
  ///
  /// ```
  /// await crdt.execute(
  ///   'INSERT INTO users (id, name) Values (?1, ?2)', [1, 'John Doe']);
  /// ```
  FutureOr<void> execute(String sql, [List<Object?>? args]);
}

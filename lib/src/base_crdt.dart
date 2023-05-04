import 'dart:async';

import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/utils/node_to_text.dart';
import 'package:uuid/uuid.dart';

import 'database_api.dart';
import 'hlc.dart';
import 'sql_util.dart';

part 'sql_crdt.dart';

part 'timestamped_crdt.dart';

part 'transaction_crdt.dart';

/// Intercepts CREATE TABLE queries to assist with table creation and updates
class BaseCrdt {
  final DatabaseApi _db;
  final _sqlEngine = SqlEngine();

  BaseCrdt(this._db);

  /// Executes a SQL query with an optional [args] list.
  /// Use "?" placeholders for parameters to avoid injection vulnerabilities:
  ///
  /// ```
  /// await crdt.execute(
  ///   'INSERT INTO users (id, name) Values (?1, ?2)', [1, 'John Doe']);
  /// ```
  Future<void> execute(String sql, [List<Object?>? args]) async {
    final result = _sqlEngine.parse(sql);

    // Warn if the query can't be parsed
    if (result.rootNode is InvalidStatement) {
      print('Warning: unable to parse SQL statement.');
      if (sql.contains(';')) {
        print('The parser can only interpret single statements.');
      }
      print(sql);
    }

    // Bail on "manual" transaction statements
    if (result.rootNode is BeginTransactionStatement ||
        result.rootNode is CommitStatement) {
      throw 'Unsupported statement: $sql.\nUse SqliteCrdt.transaction() instead.';
    }

    if (result.rootNode is CreateTableStatement) {
      await _createTable(result.rootNode as CreateTableStatement, args);
    } else if (result.rootNode is InsertStatement) {
      await _insert(result.rootNode as InsertStatement, args);
    } else if (result.rootNode is UpdateStatement) {
      await _update(result.rootNode as UpdateStatement, args);
    } else if (result.rootNode is DeleteStatement) {
      await _delete(result.rootNode as DeleteStatement, args);
    } else {
      // Run the query unchanged
      await _db.execute(sql, args?.map(_convert).toList());
    }
  }

  Future<void> _execute(Statement statement, [List<Object?>? args]) =>
      _db.execute(statement.toSql(), args?.map(_convert).toList());

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
      _db.query(sql, args?.map(_convert).toList());

  Future<void> _createTable(
      CreateTableStatement statement, List<Object?>? args) async {
    final newStatement = CreateTableStatement(
      tableName: statement.tableName,
      columns: [
        ...statement.columns,
        ColumnDefinition(
          columnName: 'is_deleted',
          typeName: 'BOOLEAN',
          constraints: [Default(null, BooleanLiteral(false))],
        ),
        ColumnDefinition(
          columnName: 'hlc',
          typeName: 'TEXT',
          constraints: [NotNull(null)],
        ),
        ColumnDefinition(
          columnName: 'node_id',
          typeName: 'TEXT',
          constraints: [NotNull(null)],
        ),
        ColumnDefinition(
          columnName: 'modified',
          typeName: 'TEXT',
          constraints: [NotNull(null)],
        ),
      ],
      tableConstraints: statement.tableConstraints,
      ifNotExists: statement.ifNotExists,
      isStrict: statement.isStrict,
      withoutRowId: statement.withoutRowId,
    );

    await _db.execute(newStatement.toSql(), args);
  }

  Future<void> _insert(InsertStatement statement, List<Object?>? args) =>
      _execute(statement, args);

  Future<void> _update(UpdateStatement statement, List<Object?>? args) =>
      _execute(statement, args);

  Future<void> _delete(DeleteStatement statement, List<Object?>? args) =>
      _execute(statement, args);

  Object? _convert(Object? value) => (value is Hlc) ? value.toString() : value;
}

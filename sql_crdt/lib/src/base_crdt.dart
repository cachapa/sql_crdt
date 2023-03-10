import 'dart:async';
import 'dart:convert';

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
          typeName: 'INTEGER',
          constraints: [Default(null, NumericLiteral(0))],
        ),
        ColumnDefinition(
          columnName: 'hlc',
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

  Object? _convert(Object? value) {
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
}

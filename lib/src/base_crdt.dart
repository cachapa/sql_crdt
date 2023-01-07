import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqflite_common/sqlite_api.dart';

// ignore: implementation_imports
import 'package:sqflite_common/src/open_options.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/utils/node_to_text.dart';
import 'package:uuid/uuid.dart';

import 'hlc.dart';

part 'sqlite_crdt.dart';

part 'timestamped_crdt.dart';

part 'transaction_crdt.dart';

/// Only intercepts CREATE TABLE queries to assist with onCreate and onUpdate.
class BaseCrdt {
  final DatabaseExecutor _executor;
  final _sqlEngine = SqlEngine();

  BaseCrdt(this._executor);

  Future<void> execute(String sql, [List<Object?>? args]) async {
    final result = _sqlEngine.parse(sql);

    // Bail if the query can't be parsed
    if (result.rootNode is InvalidStatement) {
      throw 'Unable to parse SQL statement\n$sql';
    }
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
    }
  }

  Future<void> _execute(Statement statement, [List<Object?>? args]) =>
      _executor.execute(statement.toSql(), args?.map(_convert).toList());

  Future<List<Map<String, Object?>>> query(String sql, [List<Object?>? args]) =>
      _executor.rawQuery(sql, args?.map(_convert).toList());

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

    await _executor.execute(newStatement.toSql(), args);
  }

  Future<void> _insert(InsertStatement statement, List<Object?>? args,
          [Hlc? hlc]) =>
      _execute(statement, args);

  Future<void> _update(UpdateStatement statement, List<Object?>? args,
          [Hlc? hlc]) =>
      _execute(statement, args);

  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
          [Hlc? hlc]) =>
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

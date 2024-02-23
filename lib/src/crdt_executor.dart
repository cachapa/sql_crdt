import 'package:crdt/crdt.dart';
import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/utils/node_to_text.dart';

import 'database_api.dart';

final _sqlEngine = SqlEngine();

/// Intercepts CREATE TABLE queries to assist with table creation and updates.
/// Does not affect any other query types.
class CrdtTableExecutor {
  final WriteApi _db;

  CrdtTableExecutor(this._db);

  /// Executes a SQL query with an optional [args] list.
  /// Use "?" placeholders for parameters to avoid injection vulnerabilities:
  ///
  /// ```
  /// await crdt.execute(
  ///   'INSERT INTO users (id, name) Values (?1, ?2)', [1, 'John Doe']);
  /// ```
  Future<void> execute(String sql, [List<Object?>? args]) async {
    // Break query into individual statements
    final statements =
        (_sqlEngine.parseMultiple(sql).rootNode as SemicolonSeparatedStatements)
            .statements;
    assert(statements.length == 1,
        'This package does not support compound statements:\n$sql');

    final statement = statements.first;

    // Bail on "manual" transaction statements
    if (statement is BeginTransactionStatement ||
        statement is CommitStatement) {
      throw 'Unsupported statement: ${statement.toSql()}.\nUse transaction() instead.';
    }

    await _executeStatement(statement, args);
  }

  Future<void> _executeStatement(Statement statement, List<Object?>? args) =>
      statement is CreateTableStatement
          ? _createTable(statement, args)
          : _execute(statement, args);

  Future<String> _createTable(
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

    await _execute(newStatement, args);

    return newStatement.tableName;
  }

  Future<String?> _execute(Statement statement, List<Object?>? args) async {
    final sql = statement is InvalidStatement
        ? statement.span?.text
        : statement.toSql();
    if (sql == null) return null;

    await _db.execute(sql, args);
    return null;
  }
}

class CrdtExecutor extends CrdtTableExecutor {
  final Hlc hlc;
  late final _hlcString = hlc.toString();

  final affectedTables = <String>{};

  CrdtExecutor(super._db, this.hlc);

  @override
  Future<void> _executeStatement(
      Statement statement, List<Object?>? args) async {
    final table = await switch (statement) {
      CreateTableStatement statement => _createTable(statement, args),
      InsertStatement statement => _insert(statement, args),
      UpdateStatement statement => _update(statement, args),
      DeleteStatement statement => _delete(statement, args),
      // Else, run the query unchanged
      _ => _execute(statement, args)
    };
    if (table != null) affectedTables.add(table);
  }

  Future<String> _insert(InsertStatement statement, List<Object?>? args) async {
    // Force explicit column description in insert statements
    assert(statement.targetColumns.isNotEmpty,
        'Unsupported statement: target columns must be explicitly stated.\n${statement.toSql()}');

    // Disallow star select statements
    assert(
        statement.source is! SelectInsertSource ||
            ((statement.source as SelectInsertSource).stmt as SelectStatement)
                .columns
                .whereType<StarResultColumn>()
                .isEmpty,
        'Unsupported statement: select columns must be explicitly stated.\n${statement.toSql()}');

    final argCount = args?.length ?? 0;
    final source = switch (statement.source) {
      ValuesSource s => ValuesSource([
          Tuple(expressions: [
            ...s.values.first.expressions,
            NumberedVariable(argCount + 1),
            NumberedVariable(argCount + 2),
            NumberedVariable(argCount + 3),
          ])
        ]),
      SelectInsertSource s => SelectInsertSource(SelectStatement(
          withClause: (s.stmt as SelectStatement).withClause,
          distinct: (s.stmt as SelectStatement).distinct,
          columns: [
            ...(s.stmt as SelectStatement).columns,
            ExpressionResultColumn(expression: NumberedVariable(argCount + 1)),
            ExpressionResultColumn(expression: NumberedVariable(argCount + 2)),
            ExpressionResultColumn(expression: NumberedVariable(argCount + 3)),
          ],
          from: (s.stmt as SelectStatement).from,
          where: (s.stmt as SelectStatement).where,
          groupBy: (s.stmt as SelectStatement).groupBy,
          windowDeclarations: (s.stmt as SelectStatement).windowDeclarations,
          orderBy: (s.stmt as SelectStatement).orderBy,
          limit: (s.stmt as SelectStatement).limit,
        )),
      _ => throw UnimplementedError(
          'Unsupported data source: ${statement.source.runtimeType}, please file an issue in the sql_crdt project.')
    };

    final newStatement = InsertStatement(
      mode: statement.mode,
      upsert: statement.upsert,
      returning: statement.returning,
      withClause: statement.withClause,
      table: statement.table,
      targetColumns: [
        ...statement.targetColumns,
        Reference(columnName: 'hlc'),
        Reference(columnName: 'node_id'),
        Reference(columnName: 'modified'),
      ],
      source: source,
    );

    // Touch
    if (statement.upsert is UpsertClause) {
      final action = statement.upsert!.entries.first.action;
      if (action is DoUpdate) {
        action.set.addAll([
          SingleColumnSetComponent(
            column: Reference(columnName: 'hlc'),
            expression: NumberedVariable(argCount + 1),
          ),
          SingleColumnSetComponent(
            column: Reference(columnName: 'node_id'),
            expression: NumberedVariable(argCount + 2),
          ),
          SingleColumnSetComponent(
            column: Reference(columnName: 'modified'),
            expression: NumberedVariable(argCount + 3),
          ),
        ]);
      }
    }

    args = [...args ?? [], _hlcString, hlc.nodeId, _hlcString];
    await _execute(newStatement, args);

    return newStatement.table.tableName;
  }

  Future<String> _update(UpdateStatement statement, List<Object?>? args) async {
    final argCount = args?.length ?? 0;
    final newStatement = UpdateStatement(
      withClause: statement.withClause,
      returning: statement.returning,
      from: statement.from,
      or: statement.or,
      table: statement.table,
      set: [
        ...statement.set,
        SingleColumnSetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(argCount + 1),
        ),
        SingleColumnSetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(argCount + 2),
        ),
        SingleColumnSetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(argCount + 3),
        ),
      ],
      where: statement.where,
    );

    args = [...args ?? [], _hlcString, hlc.nodeId, _hlcString];
    await _execute(newStatement, args);

    return newStatement.table.tableName;
  }

  Future<String> _delete(DeleteStatement statement, List<Object?>? args) async {
    final argCount = args?.length ?? 0;
    final newStatement = UpdateStatement(
      returning: statement.returning,
      withClause: statement.withClause,
      table: statement.table,
      set: [
        SingleColumnSetComponent(
          column: Reference(columnName: 'is_deleted'),
          expression: NumberedVariable(argCount + 1),
        ),
        SingleColumnSetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(argCount + 2),
        ),
        SingleColumnSetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(argCount + 3),
        ),
        SingleColumnSetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(argCount + 4),
        ),
      ],
      where: statement.where,
    );

    args = [...args ?? [], 1, _hlcString, hlc.nodeId, _hlcString];
    await _execute(newStatement, args);

    return newStatement.table.tableName;
  }
}

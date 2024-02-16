part of 'base_crdt.dart';

abstract class TimestampedCrdt extends BaseCrdt {
  Hlc get canonicalTime;

  String get nodeId => canonicalTime.nodeId;

  TimestampedCrdt(super.db);

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
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

    hlc ??= canonicalTime;
    args = [...args ?? [], hlc, hlc.nodeId, hlc];
    await _execute(newStatement, args);
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
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

    hlc ??= canonicalTime;
    args = [...args ?? [], hlc, hlc.nodeId, hlc];
    await _execute(newStatement, args);
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
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

    hlc ??= canonicalTime;
    args = [...args ?? [], 1, hlc, hlc.nodeId, hlc];
    await _execute(newStatement, args);
  }
}

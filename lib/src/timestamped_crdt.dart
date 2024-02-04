part of 'base_crdt.dart';

abstract class TimestampedCrdt extends BaseCrdt {
  Hlc get canonicalTime;

  String get nodeId => canonicalTime.nodeId;

  TimestampedCrdt(super.db);

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    final argCount = args?.length ?? 0;
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
      source: ValuesSource([
        Tuple(expressions: [
          ...(statement.source as ValuesSource).values.first.expressions,
          NumberedVariable(argCount + 1),
          NumberedVariable(argCount + 2),
          NumberedVariable(argCount + 3),
        ])
      ]),
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

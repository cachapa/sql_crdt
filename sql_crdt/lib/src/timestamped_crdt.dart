part of 'base_crdt.dart';

abstract class TimestampedCrdt extends BaseCrdt {
  Hlc get canonicalTime;

  String get nodeId => canonicalTime.nodeId;

  TimestampedCrdt(super.db);

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    var argCount = args?.length ?? 0;
    final newStatement = InsertStatement(
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
          NumberedVariable(++argCount),
          NumberedVariable(++argCount),
          NumberedVariable(++argCount),
        ])
      ]),
    );

    hlc ??= canonicalTime;
    args?.addAll([hlc, hlc.nodeId, hlc]);
    await _execute(newStatement, args);
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    var argCount = args?.length ?? 0;
    final newStatement = UpdateStatement(
      table: statement.table,
      set: [
        ...statement.set,
        SetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(++argCount),
        ),
        SetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(++argCount),
        ),
        SetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(++argCount),
        ),
      ],
      where: statement.where,
    );

    hlc ??= canonicalTime;
    args?.addAll([hlc, hlc.nodeId, hlc]);
    await _execute(newStatement, args);
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    var argCount = args?.length ?? 0;
    final newStatement = UpdateStatement(
      table: statement.table,
      set: [
        SetComponent(
          column: Reference(columnName: 'is_deleted'),
          expression: NumberedVariable(++argCount),
        ),
        SetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(++argCount),
        ),
        SetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(++argCount),
        ),
        SetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(++argCount),
        ),
      ],
      where: statement.where,
    );

    hlc ??= canonicalTime;
    args = [...args ?? [], 1, hlc, hlc.nodeId, hlc];
    await _execute(newStatement, args);
  }
}

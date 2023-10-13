part of 'base_crdt.dart';

abstract class TimestampedCrdt extends BaseCrdt {
  Hlc get canonicalTime;

  String get nodeId => canonicalTime.nodeId;

  TimestampedCrdt(super.db);

  List prepareInsert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) {
    final argCount = args?.length ?? 0;
    transformAutomaticExplicit(statement);
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
          SetComponent(
            column: Reference(columnName: 'hlc'),
            expression: NumberedVariable(argCount + 1),
          ),
          SetComponent(
            column: Reference(columnName: 'node_id'),
            expression: NumberedVariable(argCount + 2),
          ),
          SetComponent(
            column: Reference(columnName: 'modified'),
            expression: NumberedVariable(argCount + 3),
          ),
        ]);
      }
    }

    hlc ??= canonicalTime;
    return [newStatement, [...(args??[]), ...[hlc, hlc.nodeId, hlc]]];
  }

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    List transformedArgs = prepareInsert(statement, args, hlc);
    await _execute(transformedArgs[0], transformedArgs[1]);
  }

  /// function takes a SQL statement and a list of arguments
  /// transforms the SQL statement to change parameters with automatic index
  /// into parameters with explicit index
  transformAutomaticExplicit(Statement statement) {
    statement.allDescendants.whereType<NumberedVariable>().forEachIndexed((i, ref) {
      ref.explicitIndex ??= i + 1;
    });
  }

  List prepareUpdate(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) {
    final argCount = args?.length ?? 0;
    transformAutomaticExplicit(statement);
    final newStatement = UpdateStatement(
      withClause: statement.withClause,
      returning: statement.returning,
      from: statement.from,
      or: statement.or,
      table: statement.table,
      set: [
        ...statement.set,
        SetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(argCount + 1),
        ),
        SetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(argCount + 2),
        ),
        SetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(argCount + 3),
        ),
      ],
      where: statement.where,
    );

    hlc ??= canonicalTime;
    return [newStatement, [...(args??[]), ...[hlc, hlc.nodeId, hlc]]];
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    List transformedArgs = prepareUpdate(statement, args, hlc);
    await _execute(transformedArgs[0], transformedArgs[1]);
  }

  _listToBinaryExpression (List<Expression> expressions, Token token) {
    if (expressions.length == 1) {
      return expressions.first;
    }
    return BinaryExpression(expressions.first, token, _listToBinaryExpression(expressions.sublist(1), token));
  }

  List prepareSelect(SelectStatement statement, List<Object?>? args) {
    transformAutomaticExplicit(statement);
    var fakeSpan = SourceFile.fromString('fakeSpan').span(0);
    var andToken = Token(TokenType.and, fakeSpan);
    var equalToken = Token(TokenType.equal, fakeSpan);

    List<Expression> deletedExpr = [];
    statement.from?.allDescendants.whereType<TableReference>().forEachIndexed((index, reference) {
      if (reference.as != null) {
        deletedExpr.add(BinaryExpression(
            Reference(columnName: 'is_deleted', entityName: reference.as, schemaName: reference.schemaName),
            equalToken,
            NumericLiteral(0)
        ));
        print(reference.tableName);
      }
    });
    if (deletedExpr.isEmpty) {
      deletedExpr.add(BinaryExpression(
          Reference(columnName: 'is_deleted'),
          equalToken,
          NumericLiteral(0)
      ));
    }


    if (statement.where != null) {
      statement.where = BinaryExpression(
          statement.where!,
          Token(TokenType.and, fakeSpan),
          _listToBinaryExpression(deletedExpr, andToken)
      );
    } else {
      statement.where = _listToBinaryExpression(deletedExpr, andToken);
    }

    return [statement, args];
  }

  List prepareDelete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) {
    final argCount = args?.length ?? 0;
    transformAutomaticExplicit(statement);
    final newStatement = UpdateStatement(
      returning: statement.returning,
      withClause: statement.withClause,
      table: statement.table,
      set: [
        SetComponent(
          column: Reference(columnName: 'is_deleted'),
          expression: NumberedVariable(argCount + 1),
        ),
        SetComponent(
          column: Reference(columnName: 'hlc'),
          expression: NumberedVariable(argCount + 2),
        ),
        SetComponent(
          column: Reference(columnName: 'node_id'),
          expression: NumberedVariable(argCount + 3),
        ),
        SetComponent(
          column: Reference(columnName: 'modified'),
          expression: NumberedVariable(argCount + 4),
        ),
      ],
      where: statement.where,
    );

    hlc ??= canonicalTime;
    args = [...args ?? [], 1, hlc, hlc.nodeId, hlc];
    return [newStatement, args];
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) async {
    List transformedArgs = prepareDelete(statement, args, hlc);
    await _execute(transformedArgs[0], transformedArgs[1]);
  }

  @override
  Future<List<Map<String, Object?>>> _rawQuery(SelectStatement statement,
      [List<Object?>? args]) {
    List transformedArgs = prepareSelect(statement, args);
    return super._rawQuery(transformedArgs[0], transformedArgs[1]);
  }

  @override
  Future<int> _rawInsert(InsertStatement statement, [List<Object?>? args, Hlc? hlc]) async {
    List transformedArgs = prepareInsert(statement, args, hlc);
    return super._rawInsert(transformedArgs[0], transformedArgs[1]);
  }

  @override
  Future<int> _rawUpdate(UpdateStatement statement, [List<Object?>? args, Hlc? hlc]) async {
    List transformedArgs = prepareUpdate(statement, args, hlc);
    return super._rawUpdate(transformedArgs[0], transformedArgs[1]);
  }

  @override
  Future<int> _rawDelete(DeleteStatement statement, [List<Object?>? args, Hlc? hlc]) async {
    List transformedArgs = prepareDelete(statement, args, hlc);
    return super._rawUpdate(transformedArgs[0], transformedArgs[1]);
  }
}

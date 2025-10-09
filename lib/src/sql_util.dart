import 'package:collection/collection.dart';
import 'package:crdt/crdt.dart';
import 'package:source_span/source_span.dart';
import 'package:sqlparser/sqlparser.dart';
import 'package:sqlparser/utils/node_to_text.dart';

class SqlUtil {
  // https://github.com/simolus3/drift/discussions/2560#discussioncomment-6709055
  static final _span = SourceFile.fromString('fake').span(0);
  static final _sqlEngine = SqlEngine();

  SqlUtil._();

  /// Identifies affected tables in a given SQL statement.
  static Set<String> getAffectedTables(String sql) {
    try {
      return _getAffectedTables(
          _sqlEngine.parse(sql).rootNode as BaseSelectStatement);
    } catch (_) {
      print('Error parsing statement: $sql');
      rethrow;
    }
  }

  static Set<String> _getAffectedTables(AstNode node) {
    if (node is TableReference) return {node.tableName};
    return node.allDescendants
        .fold({}, (prev, e) => prev..addAll(_getAffectedTables(e)));
  }

  /// function takes a SQL [statement]
  /// transforms the SQL statement to change parameters from automatic
  /// index into parameters with explicit index
  static void transformAutomaticExplicit(Statement statement) {
    statement.allDescendants
        .whereType<NumberedVariable>()
        .forEachIndexed((i, ref) {
      ref.explicitIndex ??= i + 1;
    });
  }

  static String transformAutomaticExplicitSql(String sql) {
    final statement = _sqlEngine.parse(sql).rootNode as Statement;

    // if statement is of InvalidStatement type, return the original SQL string
    if (statement is InvalidStatement) return sql;

    transformAutomaticExplicit(statement);
    return statement.toSql();
  }

  static String addChangesetClauses(
    String table,
    String sql, {
    String? onlyNodeId,
    String? exceptNodeId,
    Hlc? modifiedOn,
    Hlc? modifiedAfter,
  }) {
    assert(onlyNodeId == null || exceptNodeId == null);
    assert(modifiedOn == null || modifiedAfter == null);

    final statement = _sqlEngine.parse(sql).rootNode as SelectStatement;

    final clauses = [
      if (onlyNodeId != null)
        _createClause(table, 'node_id', TokenType.equal, onlyNodeId),
      if (exceptNodeId != null)
        _createClause(
            table, 'node_id', TokenType.exclamationEqual, exceptNodeId),
      if (modifiedOn != null)
        _createClause(
            table, 'modified', TokenType.equal, modifiedOn.toString()),
      if (modifiedAfter != null)
        _createClause(
            table, 'modified', TokenType.more, modifiedAfter.toString()),
      if (statement.where != null) statement.where!,
    ];

    if (clauses.isNotEmpty) {
      statement.where =
          clauses.reduce((left, right) => _joinClauses(left, right));
    }

    return statement.toSql();
  }

  static BinaryExpression _createClause(
          String table, String column, TokenType operator, String value) =>
      BinaryExpression(
        Reference(columnName: column),
        Token(operator, _span),
        StringLiteral(value),
      );

  static BinaryExpression _joinClauses(Expression left, Expression right) =>
      BinaryExpression(left, Token(TokenType.and, _span), right);

  /// Checks if a SQL statement is an INSERT with RETURNING clause
  static bool isInsertWithReturning(String sql) {
    // Quick string-based pre-check to avoid parsing non-INSERT statements
    final trimmed = sql.trim().toLowerCase();
    if (!trimmed.startsWith('insert')) return false;
    if (!trimmed.contains('returning')) return false;

    // Only parse if it looks like INSERT...RETURNING
    try {
      final statement = _sqlEngine.parse(sql).rootNode as Statement;
      return statement is InsertStatement && statement.returning != null;
    } catch (_) {
      return false;
    }
  }
}

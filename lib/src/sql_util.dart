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
}

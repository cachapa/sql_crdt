import 'package:sql_crdt/sql_crdt.dart';
import 'package:sql_crdt/src/sql_util.dart';
import 'package:test/test.dart';

Set<String> getAffectedTables(String sql) => SqlUtil.getAffectedTables(sql);

void main() {
  group('Affected tables', () {
    test('Simple query', () {
      expect(getAffectedTables('SELECT * FROM users'), {'users'});
    });

    test('Join', () {
      expect(getAffectedTables('''
        SELECT * FROM users
        JOIN purchases ON users.id = purchases.user_id
      '''), {'users', 'purchases'});
    });

    test('Union', () {
      expect(getAffectedTables('''
        SELECT * FROM naughty
        UNION ALL
        SELECT * FROM nice
      '''), {'naughty', 'nice'});
    });

    test('Intersect', () {
      expect(getAffectedTables('''
        SELECT * FROM naughty
        INTERSECT
        SELECT * FROM nice
      '''), {'naughty', 'nice'});
    });

    test('Subselect', () {
      expect(getAffectedTables('''
        SELECT * FROM (SELECT * FROM users)
      '''), {'users'});
    });

    test('All together!', () {
      expect(getAffectedTables('''
        SELECT * FROM table1
        JOIN table2 ON id1 = id2
        UNION
        SELECT * FROM (
          SELECT * FROM table3 JOIN table4 ON id3 = id4
          INTERSECT
          SELECT * FROM (SELECT * FROM table5)
        )
        INTERSECT
        SELECT * FROM table6
      '''), {'table1', 'table2', 'table3', 'table4', 'table5', 'table6'});
    });
  });

  group('Add changeset clauses', () {
    test('Simple query', () {
      final sql = SqlUtil.addChangesetClauses('test', 'SELECT * FROM test',
          exceptNodeId: 'node_id', modifiedAfter: Hlc.zero('node_id'));
      expect(sql,
          "SELECT * FROM test WHERE node_id != 'node_id' AND modified > '1970-01-01T00:00:00.000Z-0000-node_id'");
    });

    test('Simple query with where clause', () {
      final sql = SqlUtil.addChangesetClauses(
          'test', 'SELECT * FROM test WHERE a != ?1 and b = ?2',
          exceptNodeId: 'node_id', modifiedAfter: Hlc.zero('node_id'));
      expect(sql,
          "SELECT * FROM test WHERE node_id != 'node_id' AND modified > '1970-01-01T00:00:00.000Z-0000-node_id' AND a != ?1 AND b = ?2");
    });
  });
}

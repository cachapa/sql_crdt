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
}

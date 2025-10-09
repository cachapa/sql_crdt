import 'package:sql_crdt/sql_crdt.dart';
import 'package:test/test.dart';

class MockDatabase implements ReadWriteApi {
  final List<String> executedSql = [];
  final List<List<Object?>?> executedArgs = [];
  final List<String> queriedSql = [];
  final List<List<Object?>?> queriedArgs = [];

  List<Map<String, Object?>> _queryResult = [];

  void setQueryResult(List<Map<String, Object?>> result) {
    _queryResult = result;
  }

  @override
  Future<void> execute(String sql, [List<Object?>? args]) async {
    executedSql.add(sql);
    executedArgs.add(args);
  }

  @override
  Future<List<Map<String, Object?>>> query(String sql,
      [List<Object?>? args]) async {
    queriedSql.add(sql);
    queriedArgs.add(args);
    return _queryResult;
  }

  void reset() {
    executedSql.clear();
    executedArgs.clear();
    queriedSql.clear();
    queriedArgs.clear();
    _queryResult = [];
  }
}

void main() {
  group('CrdtExecutor INSERT INTO...SELECT tests', () {
    late MockDatabase mockDb;
    late CrdtExecutor executor;
    late Hlc hlc;

    setUp(() {
      mockDb = MockDatabase();
      hlc = Hlc.now('test-node');
      executor = CrdtExecutor(mockDb, hlc);
    });

    test('INSERT INTO...SELECT without RETURNING uses execute', () async {
      await executor.execute(
          'INSERT INTO target (name, value) SELECT name, value FROM source WHERE active = 1');

      expect(mockDb.executedSql.length, 1);
      expect(mockDb.queriedSql.length, 0);

      // Should transform to add CRDT columns
      final executedSql = mockDb.executedSql.first;
      expect(executedSql, contains('hlc'));
      expect(executedSql, contains('node_id'));
      expect(executedSql, contains('modified'));
    });

    test('INSERT INTO...SELECT with RETURNING should use query', () async {
      // Set up mock result
      mockDb.setQueryResult([
        {'id': 1, 'name': 'test', 'hlc': hlc.toString()}
      ]);

      final result = await executor.query(
          'INSERT INTO target (name, value) SELECT name, value FROM source WHERE active = 1 RETURNING id, name');

      expect(mockDb.queriedSql.length, 1);
      expect(mockDb.executedSql.length, 0);
      expect(result.length, 1);
      expect(result.first['name'], 'test');

      // Should transform to add CRDT columns
      final queriedSql = mockDb.queriedSql.first;
      expect(queriedSql, contains('hlc'));
      expect(queriedSql, contains('node_id'));
      expect(queriedSql, contains('modified'));
    });

    test('Regular INSERT with RETURNING should use query', () async {
      mockDb.setQueryResult([
        {'id': 1, 'name': 'test'}
      ]);

      final result = await executor.query(
          'INSERT INTO users (name, age) VALUES (?, ?) RETURNING id, name',
          ['John', 25]);

      expect(mockDb.queriedSql.length, 1);
      expect(mockDb.executedSql.length, 0);
      expect(result.length, 1);
    });

    test('Regular SELECT continues to use query', () async {
      mockDb.setQueryResult([
        {'name': 'test', 'age': 30}
      ]);

      final result =
          await executor.query('SELECT name, age FROM users WHERE active = 1');

      expect(mockDb.queriedSql.length, 1);
      expect(mockDb.executedSql.length, 0);
      expect(result.length, 1);
    });
  });
}

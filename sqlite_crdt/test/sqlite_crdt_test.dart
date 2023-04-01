import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:test/test.dart';

void main() {
  group('Basic', () {
    late SqlCrdt crdt;

    setUp(() async {
      crdt = await SqliteCrdt.openInMemory(
        singleInstance: false,
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    test('Node ID', () {
      expect(crdt.nodeId.isEmpty, false);
    });

    test('Canonical time', () async {
      expect(crdt.canonicalTime.millis, 0);

      await _insertUser(crdt, 1, 'John Doe');
      final result = await crdt.query('SELECT * FROM users');
      final hlc = (result.first['hlc'] as String).toHlc;
      expect(crdt.canonicalTime, hlc);

      await _insertUser(crdt, 2, 'Jane Doe');
      final newResult = await crdt.query('SELECT * FROM users');
      final newHlc = (newResult.last['hlc'] as String).toHlc;
      expect(newHlc > hlc, isTrue);
      expect(crdt.canonicalTime, newHlc);
    });

    test('Create table', () async {
      await crdt.execute('''
        CREATE TABLE test (
          id INTEGER NOT NULL,
          name TEXT,
          PRIMARY KEY (id)
        )
      ''');
      final result = await crdt.query('SELECT * FROM test');
      expect(result, []);
    });

    test('Insert', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'John Doe');
    });

    test('Replace', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await crdt.execute(
          'REPLACE INTO users (id, name) VALUES (?1, ?2)', [1, 'Jane Doe']);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Upsert', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await crdt.execute('''
        INSERT INTO users (id, name) VALUES (?1, ?2)
        ON CONFLICT (id) DO UPDATE SET name = ?2
      ''', [1, 'Jane Doe']);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Update', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final insertHlc =
          (await crdt.query('SELECT hlc FROM users')).first['hlc'] as String;
      await _updateUser(crdt, 1, 'Jane Doe');
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'Jane Doe');
      expect((result.first['hlc'] as String).compareTo(insertHlc), 1);
    });

    test('Delete', () async {
      await _insertUser(crdt, 1, 'John Doe');
      await crdt.execute('''
        DELETE FROM users
        WHERE id = ?1
      ''', [1]);
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'John Doe');
      expect(result.first['is_deleted'], 1);
    });

    test('Transaction', () async {
      await crdt.transaction((txn) async {
        await _insertUser(txn, 1, 'John Doe');
        await _insertUser(txn, 2, 'Jane Doe');
      });
      final result = await crdt.query('SELECT * FROM users');
      expect(result.length, 2);
      expect(result.first['hlc'], result.last['hlc']);
    });

    test('Changeset', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final result = await crdt.getChangeset();
      expect(result['users']!.first['name'], 'John Doe');
    });

    test('Merge', () async {
      final hlc = Hlc.now('test_node_id').toString();
      await crdt.merge({
        'users': [
          {
            'id': 1,
            'name': 'John Doe',
            'hlc': hlc,
          },
        ],
      });
      final result = await crdt.query('SELECT * FROM users');
      expect(result.first['name'], 'John Doe');
      expect(result.first['hlc'], hlc);
    });
  });

  group('Watch', () {
    late SqlCrdt crdt;

    setUp(() async {
      crdt = await SqliteCrdt.openInMemory(
        singleInstance: false,
        version: 1,
        onCreate: (db, version) async {
          await db.execute('''
            CREATE TABLE users (
              id INTEGER NOT NULL,
              name TEXT,
              PRIMARY KEY (id)
            )
          ''');
          await db.execute('''
            CREATE TABLE purchases (
              id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              price REAL NOT NULL,
              PRIMARY KEY (id)
            )
          ''');
        },
      );
    });

    test('Emit on watch', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emits((List<Map<String, Object?>> e) => e.first['name'] == 'John Doe'),
      );
      await streamTest;
    });

    test('Emit on insert', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
        ]),
      );
      await _insertUser(crdt, 1, 'John Doe');
      await streamTest;
    });

    test('Emit on update', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
          (List<Map<String, Object?>> e) => e.first['name'] == 'Jane Doe',
        ]),
      );
      await _updateUser(crdt, 1, 'Jane Doe');
      await streamTest;
    });

    test('Emit on delete', () async {
      await _insertUser(crdt, 1, 'John Doe');
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users WHERE is_deleted = 0'),
        emitsInOrder([
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
          [],
        ]),
      );
      await _deleteUser(crdt, 1);
      await streamTest;
    });

    test('Emit on transaction', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) =>
              e.first['name'] == 'John Doe' && e.last['name'] == 'Jane Doe',
        ]),
      );
      await crdt.transaction((txn) async {
        await _insertUser(txn, 1, 'John Doe');
        await _insertUser(txn, 2, 'Jane Doe');
      });
      await streamTest;
    });

    test('Emit on merge', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
        ]),
      );
      await crdt.merge({
        'users': [
          {
            'id': 1,
            'name': 'John Doe',
            'hlc': Hlc.now('test_node_id').toString(),
          },
        ],
      });
      await streamTest;
    });

    test('Emit only on selected table', () async {
      final streamTest = expectLater(
        crdt.watch('SELECT * FROM users'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) => e.first['name'] == 'John Doe',
        ]),
      );
      await _insertPurchase(crdt, 1, 1, 12.3);
      await _insertUser(crdt, 1, 'John Doe');
      await streamTest;
    });

    test('Emit on all selected tables', () async {
      final streamTest = expectLater(
        crdt.watch(
            'SELECT users.name, price FROM users LEFT JOIN purchases ON users.id = user_id'),
        emitsInOrder([
          [],
          (List<Map<String, Object?>> e) =>
              e.first['name'] == 'John Doe' && e.first['price'] == null,
          (List<Map<String, Object?>> e) =>
              e.first['name'] == 'John Doe' && e.first['price'] == 12.3,
        ]),
      );
      await _insertUser(crdt, 1, 'John Doe');
      await _insertPurchase(crdt, 1, 1, 12.3);
      await streamTest;
    });
  });
}

Future<void> _insertUser(TimestampedCrdt crdt, int id, String name) =>
    crdt.execute('''
      INSERT INTO users (id, name)
      VALUES (?1, ?2)
    ''', [id, name]);

Future<void> _updateUser(TimestampedCrdt crdt, int id, String name) =>
    crdt.execute('''
      UPDATE users SET name = ?2
      WHERE id = ?1
    ''', [id, name]);

Future<void> _deleteUser(TimestampedCrdt crdt, int id) =>
    crdt.execute('DELETE FROM users WHERE id = ?1', [id]);

Future<void> _insertPurchase(
        TimestampedCrdt crdt, int id, int userId, double price) =>
    crdt.execute('''
      INSERT INTO purchases (id, user_id, price)
      VALUES (?1, ?2, ?3)
    ''', [id, userId, price]);

import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:uuid/uuid.dart';

Future<void> main() async {
  // Create or load the database
  final crdt = await SqliteCrdt.openInMemory(
    version: 1,
    onCreate: (db, version) async {
      // Create a table
      await db.execute('''
        CREATE TABLE users (
          id INTEGER NOT NULL,
          name TEXT,
          PRIMARY KEY (id)
        )
      ''');
    },
  );

  // Insert an entry into the database
  await crdt.execute('''
    INSERT INTO users (id, name)
    VALUES (?1, ?2)
  ''', [1, 'John Doe']);

  // Delete it
  await crdt.execute('DELETE FROM users WHERE id = ?1', [1]);

  // Merge a remote dataset
  await crdt.merge({
    'users': [
      {
        'id': 2,
        'name': 'Jane Doe',
        'hlc': Hlc.now(Uuid().v4()).toString(),
      },
    ],
  });

  // Queries are simple SQL statements, but note:
  // 1. The CRDT columns: hlc, modified, is_deleted
  // 2. Mr. Doe appears in the results with is_deleted = 1
  final result = await crdt.query('SELECT * FROM users');
  printRecords('SELECT * FROM users', result);

  // Perhaps a better query would be
  final betterResult =
      await crdt.query('SELECT id, name FROM users WHERE is_deleted = 0');
  printRecords('SELECT id, name FROM users WHERE is_deleted = 0', betterResult);

  // We can also watch for results to a specific query, but be aware that this
  // can be inefficient since it reruns watched queries on every database change
  crdt.watch('SELECT id, name FROM users WHERE is_deleted = 0').listen((e) =>
      printRecords(
          'Watch: SELECT id, name FROM users WHERE is_deleted = 0', e));

  // Update the database
  await crdt.execute('''
    UPDATE users SET name = ?1
    WHERE id = ?2
  ''', ['Jane Doe 👍', 2]);

  // Because entries are just marked as deleted, undoing deletes is trivial
  await crdt.execute('''
    UPDATE users SET is_deleted = ?1
    WHERE id = ?2
  ''', [1, 1]);

  // Perform multiple writes inside a transaction so they get the same timestamp
  await crdt.transaction((txn) async {
    // Make sure you use the transaction object (txn)
    // Using [crdt] here will cause a deadlock
    await txn.execute('''
      INSERT INTO users (id, name)
      VALUES (?1, ?2)
    ''', [3, 'Uncle Doe']);
    await txn.execute('''
      INSERT INTO users (id, name)
      VALUES (?1, ?2)
    ''', [4, 'Grandma Doe']);
  });
  final timestamps =
      await crdt.query('SELECT id, hlc, modified FROM users WHERE id > 2');
  printRecords('SELECT id, hlc, modified FROM users WHERE id > 2', timestamps);

  // Create a changeset to synchronize with another node
  final changeset = await crdt.getChangeset();
  print('> Changeset size: ${changeset.recordCount} records');
  changeset.forEach((key, value) {
    print(key);
    for (var e in value) {
      print('  $e');
    }
  });
}

void printRecords(String title, List<Map<String, Object?>> records) {
  print('> $title');
  records.forEach(print);
}

import 'dart:convert';
import 'dart:io';

import 'package:sqlite_crdt/sqlite_crdt.dart';
import 'package:sqlite_crdt/src/util/uuid.dart';

Future<void> main() async {
  // Cleanup past runs
  final file = File('store/sqlite_crdt_test.db');
  if (file.existsSync()) file.deleteSync();

  // Create or load the database
  final crdt = await SqliteCrdt.open(
    'store',
    'sqlite_crdt_test',
    ['users'],
    version: 1,
    onCreate: (db, version) {
      // Use [createCrdtTable] to automatically add the CRDT columns
      db.createCrdtTable('''
        CREATE TABLE users (
          id INTEGER NOT NULL,
          name TEXT,
          PRIMARY KEY (id)
        )
      ''');

      // You can also create non-crdt tables, they will be ignored
      db.execute('''
        CREATE TABLE not_a_crdt (
          id TEXT NOT NULL,
          count INTEGER,
          PRIMARY KEY (id)
        )
      ''');
    },
  );

  // Insert data into the database
  await crdt.insert('users', {
    'id': 1,
    'name': 'John Doe',
  });

  // Delete it
  await crdt.setDeleted('users', [1]);

  // Or merge a remote dataset
  await crdt.merge({
    'users': [
      {
        'id': 2,
        'name': 'Jane Doe',
        'hlc': Hlc.now(uuid()).toString(),
      },
    ],
  });

  // Queries are simple SQL statements, but notice:
  // 1. the CRDT columns: hlc, modified, is_deleted
  // 2. Mr. John Doe appears in the results with is_deleted = 1
  final result = await crdt.query('SELECT * FROM users');
  printRecords('SELECT * FROM users', result);

  // Perhaps a better query would be
  final betterResult =
      await crdt.query('SELECT id, name FROM users WHERE is_deleted = 0');
  printRecords('SELECT id, name FROM users WHERE is_deleted = 0', betterResult);

  // We can also watch for results to a specific query, but be aware that this
  // can be inefficient since it reruns watched queries on every database change
  crdt.watch('SELECT id, name FROM users WHERE is_deleted = 0').listen((e) =>
      printRecords('Watch: SELECT id, name FROM users WHERE is_deleted = 0', e));

  // Update the database
  await crdt.update('users', [2], {'name': 'Jane Doe 👍'});

  // Undelete Mr. Doe
  await crdt.setDeleted('users', [1], false);

  // Create a changeset to synchronize with another node
  final changeset = await crdt.getChangeset();
  print('> Changeset');
  print(changeset);
}

void printRecords(String title, List<Map<String, Object?>> records) {
  print('> $title');
  records.forEach(print);
  print('');
}

> ⚠ This package is still under development and may not be stable

Dart implementation of Conflict-free Replicated Data Types (CRDTs) using a Sqlite database for data storage.  
This project is a continuation of the [crdt](https://github.com/cachapa/crdt) package and may depend on it in the future.

## Notes

`sqlite_crdt` has no intention of being an ORM - on the contrary, ideally it should look like using a normal SQL database. Unfortunately that's not possible at this time because CRDTs have a few properties that need to be guaranteed:

* Every change needs to be associated to `hlc` and `modified` timestamps
* There are different rules to generating the timestamps depending on whether data is being inserted or merged
* Timestamp generation require non-trivial checks, e.g. clock drift, duplicate nodes, etc.
* Records may not be deleted, but rather marked by setting the `is_deleted` flag

For those reasons, while the raw sqlite database is exposed, changes to the database should only be made using the provided methods `insert` `update` `merge` `setDeleted` and their siblings.

## Usage

```dart
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

// Perhaps a better query would be
final betterResult =
    await crdt.query('SELECT id, name FROM users WHERE is_deleted = 0');

// We can also watch for results to a specific query, but be aware that this
// can be inefficient since it reruns watched queries on every database change
crdt.watch('SELECT id, name FROM users WHERE is_deleted = 0').listen(print);

// Update the database
await crdt.update('users', [2], {'name': 'Jane Doe 👍'});

// Undelete Mr. Doe
await crdt.setDeleted('users', [1], false);

// Create a changeset to synchronize with another node
final changeset = await crdt.getChangeset();
```

## Usage: Reading Data

Queries are standard SQL fare with three columns required for the CRDT functionality: `hlc` `modified` `is_deleted`


* In most cases you'll want to filter out deleted data by adding `WHERE is_deleted = 0` to your queries
* There's a `watch` method that emits results whenever the database changes, but it is inefficient since it just reruns the query whenever the database changes

## Features and bugs

Please file feature requests and bugs at the [issue tracker](https://github.com/cachapa/sqlite_crdt/issues).

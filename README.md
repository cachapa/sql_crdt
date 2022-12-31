Dart implementation of Conflict-free Replicated Data Types (CRDTs) using a Sqlite database for data storage.  
This project is a continuation of the [crdt](https://github.com/cachapa/crdt) package and may depend on it in the future.

> ⚠ This package is still under development and may not be stable. The API may break at any time.

## Notes

`sqlite_crdt` has no intention of being an ORM, so the API is essentially that of a plain old SQL database with a few behavioural changes:

* Every table gets 3 columns automatically added: `is_deleted`, `hlc`, and `modified`
* Deleted records aren't actually removed but rather flagged in the `is_deleted` column
* Two methods `getChangeset` and `merge` to simplify syncing with remote nodes
* A reactive `watch` method to subscribe to database changes

> ⚠ Because deleted records are only flagged as deleted, they may need to be sanitized in order to be compliant with GDPR and similar legislation.

## Setup

This package uses [sqflite](https://pub.dev/packages/sqflite). There's a bit of extra setup necessary depending on where you intend to run your code:

### Android & iOS

`sqlite_crdt` uses recent Sqlite features that may not be available in every system's embedded libraries.

To get around this, import the [sqlite3_flutter_libs](https://pub.dev/packages/sqlite3_flutter_libs) package into your project:

```yaml
sqlite3_flutter_libs: ^0.5.12
```

### Desktop, Server

On the desktop and server, Sqflite uses the system libraries so make sure those are installed.

On Debian, Raspbian, Ubuntu, etc:

```bash
sudo apt install libsqlite3 libsqlite3-dev
```

On Fedora:

```bash
sudo dnf install sqlite-devel
```

Otherwise check the instructions on [sqflite_common_ffi](https://pub.dev/packages/sqflite_common_ffi).

## Usage

The `sqlite_crdt` API is intentionally kept simple with a few methods:

* `execute` to run non-select SQL queries, e.g. inserts, updates, etc.
* `query` to perform a one-time query
* `watch` to receive query results whenever the database changes
* `getChangeset` to generate a changeset of all local changes
* `merge` to apply a remote changeset to the local database

Check [example.dart](https://github.com/cachapa/sqlite_crdt/blob/master/example/example.dart) for more details.

## Features and bugs

Please file feature requests and bugs in the [issue tracker](https://github.com/cachapa/sqlite_crdt/issues).

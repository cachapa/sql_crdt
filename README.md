Dart implementation of Conflict-free Replicated Data Types (CRDTs) using SQL databases.

This project is heavily influenced by James Long's talk [CRTDs for Mortals](https://www.dotconferences.com/2019/12/james-long-crdts-for-mortals) and includes a Dart-native implementation of Hybrid Local Clocks (HLC) based on the paper [Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases](https://cse.buffalo.edu/tech-reports/2014-04.pdf).

`sql_crdt` is based on [crdt](https://github.com/cachapa/crdt) and the learnings from [Libra](https://libra-app.eu), [StoryArk](https://storyark.eu) and [tudo](https://github.com/cachapa/tudo).  

This package is is abstract and implements the base functionality for CRDTs backed by a relational database. Check [sqlite_crdt](https://github.com/cachapa/sqlite_crdt.git) and [postgres_crdt](https://github.com/cachapa/postgres_crdt.git) for usable implementations.

See also [tudo](https://github.com/cachapa/tudo) for a real-world FOSS implementation.

This package is compatible with [crdt_sync](https://github.com/cachapa/crdt_sync), a turnkey approach for real-time network synchronization of CRDT nodes.

## Notes

`sql_crdt` is not an ORM. The API is essentially that of a plain old SQL database with a few behavioural changes:

* Every table gets 3 columns automatically added: `is_deleted`, `hlc`, and `modified`.
* Deleted records aren't actually removed but rather flagged in the `is_deleted` column.
* A reactive `watch` method to subscribe to database queries.
* Convenience methods `getChangeset` and `merge` inherited from the `crdt` package to sync with remote nodes.

> ⚠ Because deleted records are only flagged as deleted, they may need to be sanitized in order to be compliant with GDPR and similar legislation.

## API

The API is intentionally kept simple with a few methods:

* `execute` to run non-select SQL queries, e.g. inserts, updates, etc.
* `query` to perform a one-time query.
* `watch` to receive query results whenever the database changes.
* `getChangeset` to generate a serializable changeset of the local database.
* `merge` to apply a remote changeset to the local database.
* `transaction` a blocking mechanism that avoids running simultaneous transactions in async code.

Check the examples in [sqlite_crdt](https://github.com/cachapa/sqlite_crdt/blob/master/example/example.dart) and [postgres_crdt](https://github.com/cachapa/postgres_crdt/blob/master/example/example.dart) for more details.

## Features and bugs

Please file feature requests and bugs in the [issue tracker](https://github.com/cachapa/sql_crdt/issues).

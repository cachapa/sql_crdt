Dart implementation of Conflict-free Replicated Data Types (CRDTs) using SQL databases.  
This project is a continuation of the [crdt](https://github.com/cachapa/crdt) package and may depend on it in the future.

`sql_crdt` is based on the learnings from [Libra](https://libra-app.eu), [StoryArk](https://storyark.eu) and [tudo](https://github.com/cachapa/crdt).  

This package contains the base functionality. Check [sqlite_crdt](https://github.com/cachapa/sqlite_crdt.git) and [postgres_crdt](https://github.com/cachapa/postgres_crdt.git) for usable implementations.  
Check [tudo](https://github.com/cachapa/tudo) for a real-world example.

> ⚠ This package is still under development and may not be stable. The API may break at any time.

## Notes

`sql_crdt` is not an ORM. The API is essentially that of a plain old SQL database with a few behavioural changes:

* Every table gets 3 columns automatically added: `is_deleted`, `hlc`, and `modified`
* Deleted records aren't actually removed but rather flagged in the `is_deleted` column
* Features a reactive `watch` method to subscribe to database queries
* Adds convenience methods `getChangeset`, `watchChangeset` and `merge` to simplify syncing with remote nodes

> ⚠ Because deleted records are only flagged as deleted, they may need to be sanitized in order to be compliant with GDPR and similar legislation.

## API

The API is intentionally kept simple with a few methods:

* `execute` to run non-select SQL queries, e.g. inserts, updates, etc.
* `query` to perform a one-time query
* `watch` to receive query results whenever the database changes
* `getChangeset` to generate a serializable changeset of the local database
* `watchChangeset` a reactive alternative to get the changeset
* `merge` to apply a remote changeset to the local database
* `transaction` a blocking mechanism that avoids running simultaneous transactions in async code

Check the examples in [sqlite_crdt](https://github.com/cachapa/sqlite_crdt/blob/master/example/example.dart) and [postgres_crdt](https://github.com/cachapa/postgres_crdt/blob/master/example/example.dart) for more details.

## Features and bugs

Please file feature requests and bugs in the [issue tracker](https://github.com/cachapa/sql_crdt/issues).


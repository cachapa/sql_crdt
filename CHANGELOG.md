## 2.1.7

- Add support for inserts from select queries

## 2.1.6

- Upgrade sqlparser

## 2.1.5+1

- Remove echoing unrecognized statements

## 2.1.5

- Remove unnecessary warnings

## 2.1.4

- Fix failed inserts and updates with null argument lists

## 2.1.3

- Automatically convert String HLCs when merging records

## 2.1.2

- Fix table filtering

## 2.1.1

- Filter custom query tables when sending delta changesets

## 2.1.0

- Update to latest `crdt` version

## 2.0.0

This version introduces a major refactor which results in multiple breaking changes in line with `crdt` v5.

This makes this package compatible with all other packages in the [crdt](https://github.com/cachapa/crdt) family, which enables seamles synchronization between them using [crdt_sync](https://github.com/cachapa/crdt_sync).

Changes:
- Simplify code and API
- Allow specifying custom queries when generating changesets
- Remove superfluous method `watchChangeset`

## 1.1.1+1

- Add `CrdtChangeset` type alias

## 1.1.1

- Fix change notifications when no changes were made
- Guard against a possible race condition when setting the canonical time

## 1.1.0

- Breaking: return Hlc.zero instead of null in `lastModified`
- Breaking: allow specifying nodeIds in `getChangeset`
- Add getter for all tables in database
- Allow watching changed tables to enable atomic sync operations
- Removed convenience getter `peerLastModified`
- Do not merge empty changesets

## 1.0.3

- Update to Dart 3.0
- Fix watch argument generator not being re-run on every query

## 1.0.2

- Print malformed queries when using watch()

## 1.0.1

- Unbreaking: revert to int type for is_deleted column

## 1.0.0

- Breaking: allow using boolean type for is_deleted column
- Make SqlCrdt abstract

## 0.0.9+2

- Add example file

## 0.0.9+1

- Add documentation

## 0.0.9

- Add support for upsert and replace statements

## 0.0.8+1

- Fix the getChangeset query

## 0.0.8

- Dedicated node_id column to improve query performance
- Add method to reset node id
- Improve merge error reporting

## 0.0.7

- Initial version.

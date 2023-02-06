## 0.0.7

* Move database-agnostic code to sql_crdt

## 0.0.6

* Refactor path instantiation

## 0.0.5+1

* Fix UPSERTs on Raspbian due to older Sqlite version

## 0.0.4

* Warn instead of bailing on unparseable queries

## 0.0.3

* Refactor entire project to fix transaction deadlocks

## 0.0.2

* Remove ORM-like write methods
* Intercept SQL write queries and perform CRDT magic transparently
* Add watchChangeset method
* Refactor Hlc
* Add Hlc tests

## 0.0.1

* Initial release.

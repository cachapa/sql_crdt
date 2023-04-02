Dart implementation of Conflict-free Replicated Data Types (CRDTs) using Sqlite.  
This package implements [sql_crdt](https://github.com/cachapa/sql_crdt).

## Setup

Awaiting async functions is extremely important and not doing so can result in all sorts of weird behaviour.  
You can avoid them by activating the `unawaited_futures` linter warning in *analysis_options.yaml*:

```yaml
linter:
  rules:
    unawaited_futures: true
```

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

### Web

This package has experimental support for Flutter Web, thanks to [sqflite_common_ffi_web](https://pub.dev/packages/sqflite_common_ffi_web).

In order to use this feature you'll need to install the Sqlite3 web binaries by running the following command from the project's root:

```bash
dart run sqflite_common_ffi_web:setup
```

## Usage

Check [example.dart](https://github.com/cachapa/sqlite_crdt/blob/master/example/example.dart) for more details.

## Features and bugs

Please file feature requests and bugs in the [issue tracker](https://github.com/cachapa/sqlite_crdt/issues).

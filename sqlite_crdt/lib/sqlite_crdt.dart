library sqlite_crdt;

import 'dart:async';
import 'dart:io';

import 'package:sqflite_common/sqlite_api.dart';

// ignore: implementation_imports
import 'package:sqflite_common/src/open_options.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:sql_crdt/sql_crdt.dart';
import 'package:sqlite_crdt/src/sqlite_api.dart';

export 'package:sqflite_common/sqlite_api.dart';
export 'package:sql_crdt/sql_crdt.dart';

class SqliteCrdt {
  SqliteCrdt._();

  static Future<SqlCrdt> open(
    String path, {
    bool singleInstance = true,
    int? version,
    FutureOr<void> Function(BaseCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(BaseCrdt crdt, int from, int to)? onUpgrade,
  }) =>
      _open(path, false, singleInstance, version, onCreate, onUpgrade);

  static Future<SqlCrdt> openInMemory({
    bool singleInstance = true,
    int? version,
    FutureOr<void> Function(BaseCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(BaseCrdt crdt, int from, int to)? onUpgrade,
  }) =>
      _open(null, true, singleInstance, version, onCreate, onUpgrade);

  static Future<SqlCrdt> _open(
    String? path,
    bool inMemory,
    bool singleInstance,
    int? version,
    FutureOr<void> Function(BaseCrdt crdt, int version)? onCreate,
    FutureOr<void> Function(BaseCrdt crdt, int from, int to)? onUpgrade,
  ) async {
    assert((path != null) ^ inMemory);

    // Initialize FFI
    sqfliteFfiInit();
    if (Platform.isLinux) {
      await databaseFactoryFfi.setDatabasesPath('.');
    }

    final db = await databaseFactoryFfi.openDatabase(
      inMemory ? inMemoryDatabasePath : path!,
      options: SqfliteOpenDatabaseOptions(
        singleInstance: singleInstance,
        version: version,
        onCreate: onCreate == null
            ? null
            : (db, version) => onCreate.call(BaseCrdt(SqliteApi(db)), version),
        onUpgrade: onUpgrade == null
            ? null
            : (db, from, to) =>
                onUpgrade.call(BaseCrdt(SqliteApi(db)), from, to),
      ),
    );

    return SqlCrdt.open(SqliteApi(db));
  }
}

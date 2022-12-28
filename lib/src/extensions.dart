import 'package:sqflite_common/sqlite_api.dart';

extension DatabaseX on Database {
  /// Runs a create table statement and adds CRDT columns to it.
  /// See [crdtfyTable].
  Future<void> createCrdtTable(String createStatement) async {
    // Extract table name from statement
    final tokens = createStatement.split(' ');
    final i = tokens.indexWhere((e) => e.startsWith('('));
    final table = tokens[i - 1];

    await execute(createStatement);
    await crdtfyTable(table);
  }

  /// Runs a create table statement and adds CRDT columns to it.
  /// See [crdtfyTable].
  Future<void> crdtfyTable(String table) => execute('''
    ALTER TABLE $table ADD COLUMN is_deleted INTEGER DEFAULT 0;
    ALTER TABLE $table ADD COLUMN hlc TEXT NOT NULL;
    ALTER TABLE $table ADD COLUMN modified TEXT NOT NULL;
  ''');
}

extension MapX on Map<String, Iterable<Map<String, Object?>>> {
  int get recordCount => values.fold<int>(0, (prev, e) => prev + e.length);
}

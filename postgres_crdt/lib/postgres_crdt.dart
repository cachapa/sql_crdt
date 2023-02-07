library postgres_crdt;

import 'package:postgres/postgres.dart';
import 'package:sql_crdt/sql_crdt.dart';

import 'src/postgres_api.dart';

export 'package:sql_crdt/sql_crdt.dart';

class PostgresCrdt {
  PostgresCrdt._();

  static Future<SqlCrdt> open(
    String host,
    String databaseName, {
    String? username,
    String? password,
    int port = 5432,
  }) async {
    final db = PostgreSQLConnection(host, port, databaseName,
        username: username, password: password);
    await db.open();

    return SqlCrdt.open(PostgresApi(db));
  }
}

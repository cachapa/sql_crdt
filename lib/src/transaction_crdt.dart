part of 'base_crdt.dart';

class TransactionCrdt extends TimestampedCrdt {
  @override
  final Hlc canonicalTime;

  final affectedTables = <String>{};

  TransactionCrdt(super.executor, this.canonicalTime);

  @override
  Future<void> _insert(InsertStatement statement, List<Object?>? args,
      [Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    return super._insert(statement, args).onError((error, stackTrace) => rollback(error));
  }

  @override
  Future<void> _update(UpdateStatement statement, List<Object?>? args,
      [Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    return super._update(statement, args).onError((error, stackTrace) => rollback(error));
  }

  @override
  Future<void> _delete(DeleteStatement statement, List<Object?>? args,
      [Hlc? hlc]) {
    affectedTables.add(statement.table.tableName);
    return super._delete(statement, args).onError((error, stackTrace) => rollback(error));
  }

  FutureOr<void> rollback(dynamic object) async {
    await databaseApi.execute('ROLLBACK TRANSACTION');
    throw object;
  }
}

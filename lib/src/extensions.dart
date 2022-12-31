import 'package:sqflite_common/sqlite_api.dart';

extension MapX on Map<String, Iterable<Map<String, Object?>>> {
  int get recordCount => values.fold<int>(0, (prev, e) => prev + e.length);
}

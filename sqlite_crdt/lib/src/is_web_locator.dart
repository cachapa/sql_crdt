import 'is_web_io.dart' if (dart.library.html) 'is_web_web.dart' as test;

bool get sqliteCrdtIsWeb => test.sqliteCrdtIsWeb;

bool get sqliteCrdtIsLinux => test.sqliteCrdtIsLinux;

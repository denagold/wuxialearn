import 'package:hsk_learner/services/database_service.dart';
import 'package:sqflite/sqflite.dart' as sql;
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

bool _ffiFactoryInstalled = false;

/// Points sqflite's default factory at the FFI implementation.
void useInMemoryFfiFactory() {
  if (_ffiFactoryInstalled) return;
  sqfliteFfiInit();
  sql.databaseFactory = databaseFactoryFfi;
  _ffiFactoryInstalled = true;
}

Future<sql.Database> openInMemoryDatabase() async {
  useInMemoryFfiFactory();

  return sql.openDatabase(sql.inMemoryDatabasePath);
}

class InMemoryDbService implements DatabaseServiceBase {
  final sql.Database db;

  InMemoryDbService(this.db);

  @override
  Future<sql.Database> get database async => db;

  @override
  Future<sql.Database> initDatabase() async => db;

  @override
  Future<void> init() async {}

  @override
  Future<void> close() async {}

  @override
  Future<bool> tableExists(String table) async => false;

  @override
  Future<bool> columnExists(String table, String column) async => false;

  @override
  Future<void> reloadDatabaseFromFile() async {}
}

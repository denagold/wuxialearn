import 'package:sqflite/sqflite.dart';
import '../sql/sql_helper.dart';

abstract class DatabaseServiceBase {
  Future<Database> get database;

  Future<Database> initDatabase();

  Future<void> init();

  Future<void> close();

  Future<bool> tableExists(String table);

  Future<bool> columnExists(String table, String column);

  Future<void> reloadDatabaseFromFile();
}

class DatabaseServiceImpl implements DatabaseServiceBase {
  Database? _database;

  @override
  Future<Database> get database async {
    if (_database != null) return _database!;
    _database = await initDatabase();
    return _database!;
  }

  @override
  Future<Database> initDatabase() async {
    final db = await SQLHelper.db();
    return db;
  }

  @override
  Future<void> init() async {
    await database;
  }

  @override
  Future<void> close() async {
    final db = _database;
    if (db != null) {
      _database = null;
      await db.close();
    }
  }

  @override
  Future<bool> tableExists(String table) async {
    final db = await database;
    return await SQLHelper.tableExists(table, db);
  }

  @override
  Future<bool> columnExists(String table, String column) async {
    final db = await database;
    return await SQLHelper.columnExists(table, column, db);
  }

  @override
  Future<void> reloadDatabaseFromFile() async {
    await SQLHelper.refreshDB();
    _database = null;
  }
}

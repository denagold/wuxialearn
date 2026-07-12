import 'package:sqflite/sqflite.dart';
import '../sql/sql_helper.dart';

class DatabaseService {
  Database? _database;

  Future<Database> get database async {
    if (_database != null) return _database!;
    _database = await initDatabase();
    return _database!;
  }

  Future<Database> initDatabase() async {
    final db = await SQLHelper.db();
    return db;
  }

  Future<void> init() async {
    await database;
  }

  Future<void> close() async {
    final db = _database;
    if (db != null) {
      _database = null;
      await db.close();
    }
  }

  Future<bool> tableExists(String table) async {
    final db = await database;
    return await SQLHelper.tableExists(table, db);
  }

  Future<bool> columnExists(String table, String column) async {
    final db = await database;
    return await SQLHelper.columnExists(table, column, db);
  }

  Future<void> reloadDatabaseFromFile() async {
    await SQLHelper.refreshDB();
    _database = null; // Force reinitialization
  }
}
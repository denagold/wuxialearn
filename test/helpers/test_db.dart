import 'dart:io';

import 'package:hsk_learner/services/database_service.dart';
import 'package:path/path.dart' as p;
import 'package:sqflite/sqflite.dart' as sql;
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

bool _ffiFactoryInstalled = false;

/// Points sqflite's default factory at the FFI implementation.
///
/// The sqflite factory setter is documented as a set-once call and prints a
/// warning on every reassignment, so this guards against repeated calls.
/// Each test file runs in its own isolate, so the guard is per-file.
///
/// Works without the Flutter engine (no path_provider, no plugin channels),
/// so it can be used from plain Dart unit tests.
void useFfiFactory() {
  if (_ffiFactoryInstalled) return;
  sqfliteFfiInit();
  sql.databaseFactory = databaseFactoryFfi;
  _ffiFactoryInstalled = true;
}

/// Tables holding seed data that tests rely on. They keep their rows when
/// [TestDb.open] clears the content tables.
const Set<String> _seedTables = {'preferences', 'review_rating'};

/// A throw-away copy of the production asset database (`assets/example.db`).
///
/// The asset DB is the source of truth for the production schema: it already
/// contains every table — including `review` and `review_rating` with the
/// default ratings. Opening a copy of it instead of hand-written
/// `CREATE TABLE` statements keeps tests running against the real schema, so
/// they cannot drift from production (see .agents/state.md, 1.3 / 8.1).
///
/// The sqflite FFI factory has no "open from bytes" API, so the copy lives in
/// a temp directory (plain dart:io, no plugin mocking) and is removed by
/// [close].
class TestDb {
  final sql.Database db;
  final Directory _dir;

  TestDb._(this.db, this._dir);

  /// Opens a copy of the asset database.
  ///
  /// By default all content tables (`courses`, `unihan`, `stats`, `review`,
  /// ...) are emptied so tests control the data; the seed tables
  /// (`preferences`, `review_rating`) keep their shipped rows.
  /// Pass `clearContent: false` to get the asset database exactly as shipped.
  static Future<TestDb> open({bool clearContent = true}) async {
    useFfiFactory();

    // Relative to the package root; `flutter test` runs from there.
    final asset = File('assets/example.db');
    if (!asset.existsSync()) {
      throw StateError(
        'Asset database not found at ${asset.path}. '
        'Run tests from the package root (e.g. `flutter test`).',
      );
    }

    final dir = await Directory.systemTemp.createTemp('hsk_test_db');
    final file = File(p.join(dir.path, 'test.db'));
    await file.writeAsBytes(await asset.readAsBytes());

    final db = await databaseFactoryFfi.openDatabase(file.path);
    if (clearContent) {
      final rows = await db.rawQuery(
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
      );
      for (final row in rows) {
        final table = row['name'] as String;
        if (_seedTables.contains(table)) continue;
        await db.execute('DELETE FROM "$table"');
      }
    }
    return TestDb._(db, dir);
  }

  /// Closes the database and removes the temp directory.
  Future<void> close() async {
    await db.close();
    if (_dir.existsSync()) {
      await _dir.delete(recursive: true);
    }
  }
}

/// [DatabaseServiceBase] for repositories under test, backed by a [TestDb].
///
/// The database lifetime is owned by the [TestDb], so [close] is a no-op.
class TestDbService implements DatabaseServiceBase {
  final sql.Database db;

  TestDbService(this.db);

  @override
  Future<sql.Database> get database async => db;

  @override
  Future<sql.Database> initDatabase() async => db;

  @override
  Future<void> init() async {}

  @override
  Future<void> close() async {}

  @override
  Future<bool> tableExists(String table) async {
    final rows = await db.rawQuery(
      "SELECT count(*) AS count FROM sqlite_master "
      "WHERE type IN ('table', 'view') AND name = ?",
      [table],
    );
    return rows.first['count'] != 0;
  }

  @override
  Future<bool> columnExists(String table, String column) async {
    final rows = await db.rawQuery(
      'SELECT COUNT(*) AS exist FROM pragma_table_info(?) WHERE name = ?',
      [table, column],
    );
    return rows.first['exist'] != 0;
  }

  @override
  Future<void> reloadDatabaseFromFile() async {}
}

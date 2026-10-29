import 'dart:io';

import 'package:flutter/services.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/repositories/app_preferences_repository.dart';
import 'package:hsk_learner/services/preferences_service.dart';
import 'package:hsk_learner/sql/preferences_sql.dart';
import 'package:hsk_learner/sql/sql_helper.dart';
import 'package:path/path.dart' as p;
import 'package:shared_preferences/shared_preferences.dart';
import 'package:shared_preferences_platform_interface/in_memory_shared_preferences_async.dart';
import 'package:shared_preferences_platform_interface/shared_preferences_async_platform_interface.dart';
import 'package:sqflite/sqflite.dart' as sql;
import 'package:sqflite_common_ffi/sqflite_ffi.dart';

/// Tests running against the original DB
void main() {
  TestWidgetsFlutterBinding.ensureInitialized();

  late sql.Database db;
  late Directory tempDir;

  setUpAll(() async {
    sqfliteFfiInit();
    sql.databaseFactory = databaseFactoryFfi;

    // SQLHelper.db() resolves the database path through the path_provider
    // plugin on desktop, which has no implementation in tests.
    // TODO check for a better possible abstraction layer
    tempDir = await Directory.systemTemp.createTemp('wuxialearn_test');
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(
      const MethodChannel('plugins.flutter.io/path_provider'),
      (call) async => tempDir.path,
    );

    db = await SQLHelper.db();
  });

  tearDownAll(() async {
    SharedPreferencesAsyncPlatform.instance = null;
    await db.close();
    await sql.deleteDatabase(
      p.join(tempDir.path, 'demo_asset_example.db'),
    );
    TestDefaultBinaryMessengerBinding.instance.defaultBinaryMessenger
        .setMockMethodCallHandler(
      const MethodChannel('plugins.flutter.io/path_provider'),
      null,
    );
    await tempDir.delete(recursive: true);
  });

  /// After moving to fully shared preferences, this might be dropped
  group('PreferencesSql', () {
    tearDown(() async {
      // Drop any probe rows so each test sees the seeded table.
      await db.rawDelete("DELETE FROM preferences WHERE name LIKE 'probe%'");
    });

    test('getPreferences returns the seeded rows', () async {
      final rows = await PreferencesSql.getPreferences();
      expect(rows, hasLength(9));

      final byName = {for (final row in rows) row['name'] as String: row};
      expect(byName['showTranslations']!['value'], '1');
      expect(byName['showTranslations']!['type'], 'bool');
      expect(byName['default_course']!['value'], 'hsk');
      expect(byName['default_course']!['type'], 'string');
      expect(byName['courses']!['value'], '["hsk","wuxia"]');
      expect(byName['courses']!['type'], 'json');
    });

    test('insertPreference adds a row', () async {
      await PreferencesSql.insertPreference(
        name: 'probe_insert',
        value: 'some value',
        type: 'string',
      );

      final rows = await db.rawQuery(
        "SELECT value, type FROM preferences WHERE name = 'probe_insert'",
      );
      expect(rows, hasLength(1));
      expect(rows.first['value'], 'some value');
      expect(rows.first['type'], 'string');
    });

    test('setPreference updates an existing row', () async {
      await PreferencesSql.insertPreference(
        name: 'probe_update',
        value: 'original',
        type: 'string',
      );
      await PreferencesSql.setPreference(
        name: 'probe_update',
        value: 'updated',
        type: 'int',
      );

      final rows = await db.rawQuery(
        "SELECT value, type FROM preferences WHERE name = 'probe_update'",
      );
      expect(rows, hasLength(1));
      expect(rows.first['value'], 'updated');
      expect(rows.first['type'], 'int');
    });

    test('setPreference for an unknown name changes nothing', () async {
      final before = await PreferencesSql.getPreferences();

      await PreferencesSql.setPreference(
        name: 'probe_missing',
        value: 'value',
        type: 'string',
      );

      final after = await PreferencesSql.getPreferences();
      expect(after, hasLength(before.length));
    });
  });

  group('AppPreferencesRepositoryImpl', () {
    late PreferencesServiceBase service;
    late AppPreferencesRepositoryBase repo;

    Future<SharedPreferencesWithCache> createCache() {
      return SharedPreferencesWithCache.create(
        cacheOptions: const SharedPreferencesWithCacheOptions(allowList: null),
      );
    }

    setUp(() async {
      // In the app the shared_preferences_linux plugin registers the real
      // platform implementation. No plugin registers in tests, so point the
      // platform at an in-memory store instead.
      SharedPreferencesAsyncPlatform.instance =
          InMemorySharedPreferencesAsync.empty();

      service = PreferencesService(await createCache());
      await service.init();
      repo = AppPreferencesRepositoryImpl(service);
    });

    test('reads seeded preferences from the database', () {
      // Both are seeded with '1' in the preferences table.
      expect(repo.showTranslations, isTrue);
      expect(repo.showSentences, isTrue);
    });

    test('falls back to defaults for keys that are not seeded', () {
      expect(repo.theme, 'system');
      expect(repo.defaultHomePage, '');
      expect(repo.showPinyinByDefaultInReview, isFalse);
      expect(repo.showLiteralMeaningInUnitLearn, isFalse);
      expect(repo.isCharacterStrokeDataDownloaded, isFalse);
    });

    test('set then get round-trips bool preferences', () {
      repo.showTranslations = false;
      repo.showSentences = false;
      repo.isCharacterStrokeDataDownloaded = true;

      expect(repo.showTranslations, isFalse);
      expect(repo.showSentences, isFalse);
      expect(repo.isCharacterStrokeDataDownloaded, isTrue);
    });

    test('set then get round-trips string preferences', () {
      repo.theme = 'dark';
      repo.defaultHomePage = 'review';

      expect(repo.theme, 'dark');
      expect(repo.defaultHomePage, 'review');
    });

    test('writes reach the underlying preference store', () {
      expect(service.hasPreference(PreferenceConstants.theme), isFalse);

      repo.showTranslations = false;
      repo.theme = 'dark';

      expect(
        service.getPreferenceBool(key: PreferenceConstants.showTranslations),
        isFalse,
      );
      expect(
        service.getPreferenceString(key: PreferenceConstants.theme),
        'dark',
      );
    });

    test('a new repository instance sees values persisted by another', () {
      repo.theme = 'dark';

      final otherRepo = AppPreferencesRepositoryImpl(service);
      expect(otherRepo.theme, 'dark');
    });

    test('values persist in the store across a restart', () async {
      repo.theme = 'dark';

      // A fresh cache reloads from the same store and init() re-seeds from
      // the database. 'theme' is not a seeded key, so it survives.
      final restartedService = PreferencesService(await createCache());
      await restartedService.init();
      final restartedRepo = AppPreferencesRepositoryImpl(restartedService);

      expect(restartedRepo.theme, 'dark');
      // Seeded keys are reloaded from the database on init.
      expect(restartedRepo.showTranslations, isTrue);
    });
  });

  group('SQLHelper.columnExists', () {
    test('valid table and column are found', () async {
      expect(await SQLHelper.columnExists('review', 'rating_id', db), isTrue);
      expect(await SQLHelper.columnExists('review', 'no_such_column', db),
          isFalse);
    });

    test('a non-existent table finds no column', () async {
      expect(
        await SQLHelper.columnExists('no_such_table', 'rating_id', db),
        isFalse,
      );
    });
  });
}

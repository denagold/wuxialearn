import 'package:flutter_test/flutter_test.dart';

import '../helpers/test_db.dart';

/// Guards the assumptions the test suite makes about the production asset
/// database (`assets/example.db`): it must contain the full production
/// schema — including the `review` tables and their seed data — so tests run
/// against the real schema. If the asset is ever regenerated without them,
/// this fails loudly instead of the repository tests failing cryptically.
void main() {
  group('TestDb fixture', () {
    late TestDb testDb;

    setUp(() async {
      testDb = await TestDb.open();
    });

    tearDown(() async {
      await testDb.close();
    });

    test('contains the production tables', () async {
      for (final table in [
        'courses',
        'stats',
        'review',
        'review_rating',
        'preferences',
        'unihan',
        'sentences',
        'units',
        'subunits',
        'unit_info',
        'subunit_info',
      ]) {
        expect(
          await testDb.db.rawQuery(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = ?",
            [table],
          ),
          hasLength(1),
          reason: 'missing production table: $table',
        );
      }
    });

    test('review_rating keeps its seeded default ratings', () async {
      final rows = await testDb.db.rawQuery(
        'SELECT rating_name, rating_duration_start, rating_duration_end '
        'FROM review_rating ORDER BY rating_duration_start',
      );
      expect(rows.map((r) => r['rating_name']).toList(), [
        'again',
        'hard',
        'good',
        'easy',
        'perfect',
      ]);
      expect(rows.first['rating_duration_start'], 60);
      expect(rows.last['rating_duration_end'], 2592000);
    });

    test('preferences keeps its seeded rows', () async {
      final rows = await testDb.db.rawQuery(
        'SELECT name FROM preferences ORDER BY name',
      );
      expect(rows, isNotEmpty);
      final names = rows.map((r) => r['name'] as String).toSet();
      expect(
        names,
        containsAll(['showTranslations', 'default_course', 'courses']),
      );
    });

    test('content tables are cleared by default', () async {
      for (final table in [
        'courses',
        'unihan',
        'sentences',
        'stats',
        'review',
      ]) {
        final rows = await testDb.db.rawQuery(
          'SELECT count(1) AS c FROM $table',
        );
        expect(rows.first['c'], 0, reason: '$table not cleared');
      }
    });

    test('clearContent: false keeps the shipped data', () async {
      final shipped = await TestDb.open(clearContent: false);
      try {
        final rows = await shipped.db.rawQuery(
          'SELECT count(1) AS c FROM courses',
        );
        expect(rows.first['c'], greaterThan(0));
      } finally {
        await shipped.close();
      }
    });

    test('TestDbService reports tables and columns correctly', () async {
      final service = TestDbService(testDb.db);
      expect(await service.tableExists('review_rating'), isTrue);
      expect(await service.tableExists('no_such_table'), isFalse);
      expect(await service.columnExists('review', 'rating_id'), isTrue);
      expect(await service.columnExists('review', 'no_such_column'), isFalse);
    });
  });
}

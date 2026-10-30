import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/repositories/review_rating_repository.dart';
import 'package:hsk_learner/repositories/review_repository.dart';

import '../helpers/test_db.dart';

void main() {
  group('ReviewRepository', () {
    late TestDb testDb;
    late ReviewRepositoryBase repo;

    setUp(() async {
      // Real production schema (courses, unihan, review, review_rating,
      // stats) from the asset DB copy; content tables are emptied, the
      // default ratings keep their seeded rows.
      testDb = await TestDb.open();
      final db = testDb.db;

      await db.insert('courses', {
        'id': 1,
        'hanzi': '天',
        'pinyin': 'tiān',
        'translations0': 'sky',
        'subunit': 1,
        'unit': 1,
        'hsk': 1,
      });
      await db.insert('courses', {
        'id': 2,
        'hanzi': '好',
        'pinyin': 'hǎo',
        'translations0': 'good',
        'subunit': 1,
        'unit': 1,
        'hsk': 1,
      });
      await db.insert('courses', {
        'id': 3,
        'hanzi': '学',
        'pinyin': 'xué',
        'translations0': 'study',
        'subunit': 2,
        'unit': 2,
        'hsk': 2,
      });
      await db.insert('unihan', {
        'hanzi': '天',
        'pinyin': 'tiān',
        'translation': 'sky; day',
      });
      await db.insert('unihan', {
        'hanzi': '好',
        'pinyin': 'hǎo',
        'translation': 'good',
      });
      await db.insert('unihan', {
        'hanzi': '学',
        'pinyin': 'xué',
        'translation': 'study',
      });

      // All words are due (show_next in the past) and uncategorized.
      await db.insert('review', {
        'id': 1,
        'deck': 'hsk',
        'show_next': 1000,
        'rating_id': null,
      });
      await db.insert('review', {
        'id': 2,
        'deck': 'hsk',
        'show_next': 1000,
        'rating_id': null,
      });
      await db.insert('review', {
        'id': 3,
        'deck': 'wuxia',
        'show_next': 1000,
        'rating_id': null,
      });

      // Word 1: 2 right, 1 wrong -> score 1.
      // Word 2: 0 right, 2 wrong -> score -2.
      // Word 3: 1 right, 0 wrong -> score 1.
      await db.insert('stats', {'wordid': 1, 'date': 100, 'value': 1});
      await db.insert('stats', {'wordid': 1, 'date': 200, 'value': 1});
      await db.insert('stats', {'wordid': 1, 'date': 300, 'value': 0});
      await db.insert('stats', {'wordid': 2, 'date': 100, 'value': 0});
      await db.insert('stats', {'wordid': 2, 'date': 200, 'value': 0});
      await db.insert('stats', {'wordid': 3, 'date': 100, 'value': 1});

      // Real implementations on the shared test database; no fakes.
      final dbService = TestDbService(db);
      repo = ReviewRepositoryImpl(
        dbService,
        ratingRepo: ReviewRatingRepositoryImpl(dbService),
      );
    });

    tearDown(() async {
      await testDb.close();
    });

    group('getReview', () {
      test('allow-listed sort column works', () async {
        final words = await repo.getReview(
          deckSize: 10,
          sortBy: 'score',
          orderBy: 'ASC',
          deckName: 'hsk',
        );
        expect(words.map((w) => w.id).toList(), [2, 1]);
      });

      test('injected sortBy is rejected before reaching SQL', () async {
        await expectLater(
          repo.getReview(
            deckSize: 10,
            sortBy: 'score; DROP TABLE courses --',
            orderBy: 'ASC',
            deckName: 'hsk',
          ),
          throwsA(isA<ArgumentError>()),
        );
        final tables = await testDb.db.rawQuery(
          "SELECT count(1) as count FROM courses",
        );
        expect(tables.first['count'], 3);
      });

      test('injected orderBy is rejected before reaching SQL', () async {
        await expectLater(
          repo.getReview(
            deckSize: 10,
            sortBy: 'score',
            orderBy: 'ASC; DELETE FROM courses --',
            deckName: 'hsk',
          ),
          throwsA(isA<ArgumentError>()),
        );
        final tables = await testDb.db.rawQuery(
          "SELECT count(1) as count FROM courses",
        );
        expect(tables.first['count'], 3);
      });

      test('RANDOM() sort is allowed and returns deck rows', () async {
        final words = await repo.getReview(
          deckSize: 10,
          sortBy: 'RANDOM()',
          orderBy: 'ASC',
          deckName: 'hsk',
        );
        expect(words.map((w) => w.id).toSet(), {1, 2});
      });

      test('negative deckSize fetches the whole deck', () async {
        final words = await repo.getReview(
          deckSize: -1,
          sortBy: 'last_seen',
          orderBy: 'ASC',
          deckName: 'hsk',
        );
        expect(words.map((w) => w.id).toList(), [2, 1]);
      });

      test('sort column of the other query is rejected', () async {
        // 'unit' is a valid manage-review sort column, but it is not in scope
        // for getReview, so it must fail fast instead of reaching SQL.
        await expectLater(
          repo.getReview(
            deckSize: 10,
            sortBy: 'unit',
            orderBy: 'ASC',
            deckName: 'hsk',
          ),
          throwsA(isA<ArgumentError>()),
        );
      });
    });

    group('getManageReview', () {
      test('allow-listed sort column works', () async {
        final stats = await repo.getManageReview(
          deckSize: -1,
          sortBy: 'unit',
          orderBy: 'ASC',
          deckName: 'hsk',
        );
        expect(stats.map((s) => s.id).toList(), [1, 2]);
      });

      test('injected sortBy is rejected before reaching SQL', () async {
        await expectLater(
          repo.getManageReview(
            deckSize: -1,
            sortBy: 'unit; DROP TABLE stats --',
            orderBy: 'ASC',
            deckName: 'hsk',
          ),
          throwsA(isA<ArgumentError>()),
        );
        final tables = await testDb.db.rawQuery(
          "SELECT count(1) as count FROM stats",
        );
        expect(tables.first['count'], 6);
      });
    });

    group('getSrsReview', () {
      test('deckSize limits the number of due words', () async {
        final limited = await repo.getSrsReview(deckSize: 2);
        expect(limited, hasLength(2));

        final unlimited = await repo.getSrsReview(deckSize: -1);
        expect(unlimited, hasLength(3));
      });
    });

    group('getUncategorizedWords', () {
      test('deckSize limits the number of words', () async {
        final limited = await repo.getUncategorizedWords(
          deck: 'hsk',
          deckSize: 1,
        );
        expect(limited, hasLength(1));
      });

      test('negative deckSize fetches all uncategorized words', () async {
        final unlimited = await repo.getUncategorizedWords(
          deck: 'hsk',
          deckSize: -1,
        );
        expect(unlimited.map((w) => w.id).toList(), [1, 2]);
      });

      test('deck name filter is bound, not interpolated', () async {
        // A quote in the deck name stays a literal value and matches nothing.
        final injected = await repo.getUncategorizedWords(
          deck: "' OR 1=1 --",
          deckSize: -1,
        );
        expect(injected, isEmpty);
      });
    });

    group('getReviewRatings', () {
      // Exercises the real ReviewRatingRepositoryImpl against the seeded
      // review_rating table shipped in the asset database.
      test(
        'returns the seeded default ratings ordered by duration start',
        () async {
          final ratings = await repo.getReviewRatings();
          expect(ratings.map((r) => r.name).toList(), [
            'again',
            'hard',
            'good',
            'easy',
            'perfect',
          ]);
          expect(ratings.first.durationStart.inSeconds, 60);
          expect(ratings.last.durationEnd.inSeconds, 2592000);
        },
      );
    });
  });
}

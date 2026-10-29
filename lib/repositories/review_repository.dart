import 'package:hsk_learner/constants/deck_size.dart';
import 'package:hsk_learner/constants/deck_type.dart';
import 'package:hsk_learner/constants/review_word_type.dart';
import 'package:hsk_learner/models/review_progress.dart';
import 'package:hsk_learner/models/review_rating.dart';
import 'package:hsk_learner/models/review_statistics.dart';
import 'package:hsk_learner/models/review_word.dart';
import 'package:hsk_learner/repositories/review_rating_repository.dart';
import 'package:hsk_learner/services/database_service.dart';

abstract class ReviewRepositoryBase {
  Future<List<ReviewWord>> getReviewWords({
    required String deckName,
    required DeckSize deckSize,
    required ReviewWordType type,
  });

  Future<List<ReviewWord>> getSrsReview({required int deckSize});

  Future<List<ReviewWord>> getUncategorizedWords({
    required String deck,
    required int deckSize,
  });

  Future<List<ReviewWord>> getReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deckName,
  });

  Future<List<ReviewProgressData>> getProgress({required String deck});

  Future<void> updateReview({
    required int id,
    required int time,
    required int ratingId,
  });

  Future<void> removeFromDeck({required int id, required String deck});

  Future<List<ReviewStatistics>> getManageReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deckName,
  });

  Future<void> addToReviewDeck({
    required int id,
    required String deck,
    required bool value,
  });

  Future<List<ReviewRatingModel>> getReviewRatings();

  Future<void> setReviewRating({
    required int id,
    required String name,
    required int start,
    required int end,
  });

  Future<void> insertRating({
    required String name,
    required int start,
    required int end,
  });

  Future<void> deleteRating({required int id});
}

class ReviewRepositoryImpl implements ReviewRepositoryBase {
  final DatabaseServiceBase dbService;
  final ReviewRatingRepositoryBase ratingRepo;

  /// `ORDER BY` Columns
  static const Set<String> _reviewSortColumns = {
    'hsk',
    'translations0',
    'right_occurrence',
    'wrong_occurrence',
    'last_seen',
    'score',
    'percent_correct',
    'RANDOM()',
  };

  static const Set<String> _manageReviewSortColumns = {
    'unit',
    'hsk',
    'hanzi',
    'pinyin',
    'translations0',
    'right_occurrence',
    'wrong_occurrence',
    'last_seen',
    'score',
    'percent_correct',
    'RANDOM()',
  };

  ReviewRepositoryImpl(this.dbService, {required this.ratingRepo});

  String _validatedSortBy(String sortBy, Set<String> allowedColumns) {
    if (!allowedColumns.contains(sortBy)) {
      throw ArgumentError.value(
        sortBy,
        'sortBy',
        'Must be one of: ${allowedColumns.join(', ')}',
      );
    }
    return sortBy;
  }

  String _validatedOrderBy(String orderBy) {
    final normalized = orderBy.toUpperCase();
    if (normalized != 'ASC' && normalized != 'DESC') {
      throw ArgumentError.value(
        orderBy,
        'orderBy',
        'Must be ASC or DESC',
      );
    }
    return normalized;
  }

  @override
  Future<List<ReviewWord>> getReviewWords({
    required String deckName,
    required DeckSize deckSize,
    required ReviewWordType type,
  }) {
    switch (type) {
      case ReviewWordType.srs:
        /// The SRS type ignores [deckName], since due words are global.
        return getSrsReview(deckSize: deckSize.cardCount);
      case ReviewWordType.random:
        return getReview(
          deckSize: deckSize.cardCount,
          sortBy: "RANDOM()",
          orderBy: "ASC",
          deckName: deckName,
        );
      case ReviewWordType.difficult:
        return getReview(
          deckSize: deckSize.cardCount,
          sortBy: "score",
          orderBy: "ASC",
          deckName: deckName,
        );
      case ReviewWordType.old:
        return getReview(
          deckSize: deckSize.cardCount,
          sortBy: "last_seen",
          orderBy: "ASC",
          deckName: deckName,
        );
      case ReviewWordType.uncategorized:
        return getUncategorizedWords(
          deck: deckName,
          deckSize: deckSize.cardCount,
        );
    }
  }

  @override
  Future<List<ReviewWord>> getSrsReview({required int deckSize}) async {
    final db = await dbService.database;
    final hasLimit = deckSize >= 0;
    final results = await db.rawQuery("""
        SELECT t1.id, t1.hanzi, t1.pinyin, translations0, subunit,
        a_tl.translation as char_one, b_tl.translation as char_two, c_tl.translation as char_three, d_tl.translation as char_four
            from(
              SELECT
                id, hanzi, pinyin, translations0, subunit, unit,
                SUBSTR(hanzi, 1, 1) a, SUBSTR(hanzi, 2, 1) b,
                SUBSTR(hanzi, 3, 1) c, SUBSTR(hanzi, 4, 1) d
              FROM courses
            ) as t1
        left join unihan a_tl on t1.a = a_tl.hanzi
        left join unihan b_tl on t1.b = b_tl.hanzi  
        left join unihan c_tl on t1.c = c_tl.hanzi
        left join unihan d_tl on t1.d = d_tl.hanzi 
		    join review on review.id = t1.id
		    WHERE show_next < strftime('%s')
        GROUP BY t1.id
        ORDER BY show_next ASC
        ${hasLimit ? 'LIMIT ?' : ''}
      """, <Object?>[if (hasLimit) deckSize]);
    return createReviewWordList(results);
  }

  @override
  Future<List<ReviewWord>> getUncategorizedWords({
    required String deck,
    required int deckSize,
  }) async {
    final db = await dbService.database;
    final hasLimit = deckSize >= 0;
    final results = await db.rawQuery("""
        SELECT t1.id, t1.hanzi, t1.pinyin, translations0, subunit,
        a_tl.translation as char_one, b_tl.translation as char_two, c_tl.translation as char_three, d_tl.translation as char_four
            from(
              SELECT
                id, hanzi, pinyin, translations0, subunit, unit,
                SUBSTR(hanzi, 1, 1) a, SUBSTR(hanzi, 2, 1) b,
                SUBSTR(hanzi, 3, 1) c, SUBSTR(hanzi, 4, 1) d
              FROM courses
            ) as t1
        left join unihan a_tl on t1.a = a_tl.hanzi
        left join unihan b_tl on t1.b = b_tl.hanzi  
        left join unihan c_tl on t1.c = c_tl.hanzi
        left join unihan d_tl on t1.d = d_tl.hanzi 
        join review on review.id = t1.id
        where deck = ? and rating_id is null
        GROUP BY t1.id
        ORDER BY t1.id ASC
        ${hasLimit ? 'LIMIT ?' : ''}
      """, <Object?>[deck, if (hasLimit) deckSize]);
    return createReviewWordList(results);
  }

  @override
  Future<List<ReviewWord>> getReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deckName,
  }) async {
    final db = await dbService.database;
    final safeSortBy = _validatedSortBy(sortBy, _reviewSortColumns);
    final safeOrderBy = _validatedOrderBy(orderBy);
    final hasLimit = deckSize >= 0;
    final args = <Object?>[deckName, if (hasLimit) deckSize];
    final results = await db.rawQuery("""
        SELECT t1.id, t1.score, t1.percent_correct, t1.hanzi,
        t1.translations0, t1.hsk, t1.pinyin,
        a_tl.translation as char_one, b_tl.translation as char_two, 
        c_tl.translation as char_three, d_tl.translation as char_four
        FROM (
          SELECT courses.id, right_occurrence, wrong_occurrence, 
            courses.hanzi, courses.hsk, courses.pinyin, courses.translations0,
            last_seen, (right_occurrence - wrong_occurrence) as score,
            ROUND(right_occurrence * 100.0 / (right_occurrence + wrong_occurrence), 1) AS percent_correct,
            SUBSTR(hanzi, 1, 1) a, SUBSTR(hanzi, 2, 1) b,
            SUBSTR(hanzi, 3, 1) c, SUBSTR(hanzi, 4, 1) d
            FROM(
              SELECT
              wordid,
              SUM(CASE recent_stats.value WHEN 1 THEN 1 ELSE 0 END) right_occurrence,
              SUM(CASE recent_stats.value WHEN 0 THEN 1 ELSE 0 END) wrong_occurrence,
              MAX(recent_stats.date) last_seen
              FROM(
              SELECT *
                ,ROW_NUMBER() OVER (
                PARTITION BY wordid ORDER BY date DESC
              )AS group_size
              FROM stats
              )AS recent_stats
              WHERE group_size <= 5
              GROUP BY wordid
            )
          INNER JOIN courses on courses.id = wordid
        )as t1
    left join unihan a_tl on t1.a = a_tl.hanzi
    left join unihan b_tl on t1.b = b_tl.hanzi  
    left join unihan c_tl on t1.c = c_tl.hanzi
    left join unihan d_tl on t1.d = d_tl.hanzi 
    join review on review.id = t1.id
    where deck = ?
    GROUP BY t1.id
    ORDER BY $safeSortBy $safeOrderBy
    ${hasLimit ? 'LIMIT ?' : ''};
    """, args);
    return createReviewWordList(results);
  }

  @override
  Future<List<ReviewProgressData>> getProgress({required String deck}) async {
    final db = await dbService.database;
    final results = await db.rawQuery("""
        SELECT review_rating.rating_id, count(1) as count, rating_name, 1 as rs 
        from review_rating
        join review on review.rating_id = review_rating.rating_id
        where deck = ?
        group by review_rating.rating_id
        union ALL select review_rating.rating_id, 0 as count, rating_name, 2 from review_rating
        where review_rating.rating_id not in (
	          SELECT review_rating.rating_id from review_rating
                join review on review.rating_id = review_rating.rating_id
                where deck = ?
                group by review_rating.rating_id
        )
        union all select -1, count(1), 'uncategorized', 3
        from review where deck = ? AND rating_id is null
        union all select -2, count(1), 'total', 4
        from review
        where deck = ?
        order by rs
      """, [deck, deck, deck, deck]);
    return createReviewProgressDataList(results);
  }

  @override
  Future<void> updateReview({
    required int id,
    required int time,
    required int ratingId,
  }) async {
    final db = await dbService.database;
    await db.rawUpdate("""
      UPDATE review set show_next = ? , rating_id = ? where id = ? 
    """, [time, ratingId, id]);
  }

  @override
  Future<void> removeFromDeck({
    required int id,
    required String deck,
  }) async {
    final db = await dbService.database;
    await db.rawDelete("""
      delete from review where deck = ? and id = ?
    """, [deck, id]);
  }

  @override
  Future<List<ReviewStatistics>> getManageReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deckName,
  }) async {
    final db = await dbService.database;
    final safeSortBy = _validatedSortBy(sortBy, _manageReviewSortColumns);
    final safeOrderBy = _validatedOrderBy(orderBy);
    String deckFilter;
    List<dynamic> queryArgs;
    if (deckName.toLowerCase() == DeckType.any.toDbString()) {
      deckFilter = '';
      queryArgs = [];
    } else {
      deckFilter = 'AND review.deck = ?';
      queryArgs = [deckName];
    }
    final hasLimit = deckSize > 0;
    if (hasLimit) {
      queryArgs.add(deckSize);
    }
    final results = await db.rawQuery("""
      SELECT courses.id, right_occurrence, wrong_occurrence,
      courses.hanzi, courses.hsk, courses.pinyin, courses.translations0,
      last_seen,
      (right_occurrence - wrong_occurrence) AS score,
      ROUND(right_occurrence * 100.0 / (right_occurrence + wrong_occurrence), 1) AS percent_correct
      FROM (
        SELECT
          wordid,
          SUM(CASE value WHEN 1 THEN 1 ELSE 0 END) AS right_occurrence,
          SUM(CASE value WHEN 0 THEN 1 ELSE 0 END) AS wrong_occurrence,
          MAX(date) AS last_seen
        FROM stats
        WHERE date > 0
        GROUP BY wordid
      )
      INNER JOIN courses ON courses.id = wordid
      INNER JOIN review ON review.id = wordid
      WHERE 1=1 $deckFilter
      GROUP BY courses.id
      ORDER BY $safeSortBy $safeOrderBy
      ${hasLimit ? 'LIMIT ?' : ''};
    """, queryArgs);
    return createReviewStatisticsList(results);
  }

  @override
  Future<void> addToReviewDeck({
    required int id,
    required String deck,
    required bool value,
  }) async {
    final db = await dbService.database;
    await db.transaction((txn) async {
      int timeStamp = DateTime.now().toUtc().millisecondsSinceEpoch ~/ 1000;
      final existingDeck = await txn.rawQuery(
        "SELECT * FROM review WHERE id = ? AND deck = ?",
        [id, deck],
      );

      if (existingDeck.isEmpty) {
        await txn.rawInsert("""
          INSERT INTO review(id, deck, show_next) 
          VALUES(?, ?, ?)
        """, [id, deck, timeStamp]);
      }

      final existingAny = await txn.rawQuery(
        "SELECT * FROM review WHERE id = ? AND deck = 'any'",
        [id],
      );
      if (existingAny.isEmpty) {
        await txn.rawInsert("""
          INSERT INTO review(id, deck, show_next) 
          VALUES(?, 'any', ?)
        """, [id, timeStamp]);
      }
    });
  }

  @override
  Future<List<ReviewRatingModel>> getReviewRatings() async {
    return ratingRepo.getAllReviewRatings();
  }

  @override
  Future<void> setReviewRating({
    required int id,
    required String name,
    required int start,
    required int end,
  }) async {
    await ratingRepo.updateReviewRating(
      ReviewRatingModel(
        id: id,
        name: name,
        durationStart: Duration(seconds: start),
        durationEnd: Duration(seconds: end),
      ),
    );
  }

  @override
  Future<void> insertRating({
    required String name,
    required int start,
    required int end,
  }) async {
    await ratingRepo.addReviewRating(ReviewRatingModel(
      // ignored by addReviewRating; the database assigns the rating_id
      id: 0,
      name: name,
      durationStart: Duration(seconds: start),
      durationEnd: Duration(seconds: end),
    ));
  }

  @override
  Future<void> deleteRating({required int id}) async {
    final db = await dbService.database;
    await db.transaction((txn) async {
      await txn.delete(
        'review_rating',
        where: 'rating_id = ?',
        whereArgs: [id],
      );
      await txn.update(
        'review',
        {'rating_id': null},
        where: 'rating_id = ?',
        whereArgs: [id],
      );
    });
  }
}

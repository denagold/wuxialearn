import 'package:hsk_learner/services/database_service.dart';

abstract class ReviewRepositoryBase {
  Future<List<Map<String, dynamic>>> getSrsReview({required int deckSize});

  Future<List<Map<String, dynamic>>> getUncategorizedWords({
    required String deck,
    required int deckSize,
  });

  Future<List<Map<String, dynamic>>> getReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deckName,
  });

  Future<List<Map<String, dynamic>>> getProgress({required String deck});

  Future<void> updateReview({
    required int id,
    required int time,
    required int ratingId,
  });

  Future<void> removeFromDeck({required int id, required String deck});

  Future<List<Map<String, dynamic>>> getManageReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deck,
  });

  Future<void> addToReviewDeck({
    required int id,
    required String deck,
    required bool value,
  });

  // Ratings
  Future<List<Map<String, dynamic>>> getReviewRatings();

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

  Future<List<Map<String, dynamic>>> test({required String deck});
}

class ReviewRepositoryImpl implements ReviewRepositoryBase {
  final DatabaseServiceBase dbService;

  ReviewRepositoryImpl(this.dbService);

  @override
  Future<List<Map<String, dynamic>>> getSrsReview({required int deckSize}) async {
    final db = await dbService.database;
    String limit = "limit $deckSize";
    if (deckSize < 0) {
      limit = "";
    }
    return db.rawQuery("""
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
		    $limit
      """);
  }

  @override
  Future<List<Map<String, dynamic>>> getUncategorizedWords({
    required String deck,
    required int deckSize,
  }) async {
    final db = await dbService.database;
    String limit = "limit $deckSize";
    if (deckSize < 0) {
      limit = "";
    }
    return db.rawQuery("""
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
        WHERE deck = '$deck' AND rating_id IS NULL
        GROUP BY t1.id
        ORDER BY t1.id ASC
        $limit
      """);
  }

  @override
  Future<List<Map<String, dynamic>>> getReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deckName,
  }) async {
    final db = await dbService.database;
    return db.rawQuery("""
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
    where deck = '$deckName'
    GROUP BY t1.id
    ORDER BY $sortBy $orderBy
    LIMIT $deckSize;
    """);
  }

  @override
  Future<List<Map<String, dynamic>>> getProgress({required String deck}) async {
    final db = await dbService.database;
    return db.rawQuery("""
        SELECT review_rating.rating_id, count(1) as count, rating_name, 1 as rs 
        from review_rating
        join review on review.rating_id = review_rating.rating_id
        where deck = '$deck'
        group by review_rating.rating_id
        union ALL select review_rating.rating_id, 0 as count, rating_name, 2 from review_rating
        where review_rating.rating_id not in (
	              SELECT review_rating.rating_id from review_rating
                join review on review.rating_id = review_rating.rating_id
                where deck = '$deck'
                group by review_rating.rating_id
        )
        union all select -1, count(1), 'uncategorized', 3
        from review where deck = '$deck' AND rating_id is null
        union all select -2, count(1), 'total', 4
        from review
        where deck = '$deck'
        order by rs
      """);
  }

  @override
  Future<void> updateReview({
    required int id,
    required int time,
    required int ratingId,
  }) async {
    final db = await dbService.database;
    await db.rawUpdate("""
      UPDATE review set show_next = $time, rating_id = $ratingId where id = $id 
    """);
  }

  @override
  Future<void> removeFromDeck({
    required int id,
    required String deck,
  }) async {
    final db = await dbService.database;
    await db.rawDelete("""
      delete from review where deck = '$deck' and id = $id
    """);
  }

  @override
  Future<List<Map<String, dynamic>>> getManageReview({
    required int deckSize,
    required String sortBy,
    required String orderBy,
    required String deck,
  }) async {
    final db = await dbService.database;
    return db.rawQuery("""
    SELECT courses.id, right_occurrence, wrong_occurrence, 
    courses.hanzi, courses.hsk, courses.pinyin, courses.translations0,
    last_seen,
    (right_occurrence - wrong_occurrence) as score,
    ROUND(right_occurrence * 100.0 / (right_occurrence + wrong_occurrence), 1) AS percent_correct
    FROM(
      SELECT
        wordid,
        SUM(CASE stats.value WHEN 1 THEN 1 ELSE 0 END) right_occurrence,
        SUM(CASE stats.value WHEN 0 THEN 1 ELSE 0 END) wrong_occurrence,
        MAX(stats.date) last_seen
      FROM stats
      WHERE date > 0
      GROUP BY 
        wordid
    )
    INNER JOIN courses on courses.id = wordid
    join review on review.id = wordid
    $deck
    ORDER BY $sortBy $orderBy
    LIMIT $deckSize;
    """);
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
        "SELECT * FROM review WHERE id = $id AND deck = '$deck'",
      );

      if (existingDeck.isEmpty) {
        await txn.rawInsert("""
          INSERT INTO review(id, deck, show_next) 
          VALUES($id, '$deck', $timeStamp)
        """);
      }

      final existingAny = await txn.rawQuery(
        "SELECT * FROM review WHERE id = $id AND deck = 'any'",
      );
      if (existingAny.isEmpty) {
        await txn.rawInsert("""
          INSERT INTO review(id, deck, show_next) 
          VALUES($id, 'any', $timeStamp)
        """);
      }
    });
  }

  @override
  Future<List<Map<String, dynamic>>> getReviewRatings() async {
    final db = await dbService.database;
    return db.rawQuery("""
        SELECT rating_id, rating_name, rating_duration_start, rating_duration_end
        from review_rating
        order by rating_duration_start asc
      """);
  }

  @override
  Future<void> setReviewRating({
    required int id,
    required String name,
    required int start,
    required int end,
  }) async {
    final db = await dbService.database;
    await db.rawUpdate("""
      update review_rating set rating_name = '$name', 
      rating_duration_start = $start, rating_duration_end = $end
      where rating_id = $id
    """);
  }

  @override
  Future<void> insertRating({
    required String name,
    required int start,
    required int end,
  }) async {
    final db = await dbService.database;
    await db.rawInsert("""
      insert into review_rating (rating_name, rating_duration_start, rating_duration_end)
      values ('$name', $start, $end)
    """);
  }

  @override
  Future<void> deleteRating({required int id}) async {
    final db = await dbService.database;
    await db.rawDelete("""
    delete from review_rating where rating_id = $id
    """);
    await db.rawUpdate("""
    update review set rating_id = null where rating_id = $id
    """);
  }

  @override
  Future<List<Map<String, dynamic>>> test({required String deck}) async {
    final db = await dbService.database;
    return db.rawQuery("""
        SELECT rating_id from review  where rating_id = ''
      """);
  }
}

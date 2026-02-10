import 'package:hsk_learner/models/review_rating.dart';
import 'package:sqflite/sqflite.dart';

class ReviewRatingService {
  final Database _db;

  ReviewRatingService(this._db);

  Future<List<ReviewRatingModel>> getAllRatings() async {
    final List<Map<String, dynamic>> ratingsMap = await _db.query(
        'review_rating',
        orderBy: 'rating_duration_start ASC'
    );
    return List.generate(
        ratingsMap.length, (index) =>
        ReviewRatingModel.fromMap(ratingsMap[index]
    ));
  }

  Future<ReviewRatingModel?> getRating(int id) async {
    var map = await _db.query(
        'review_rating',
        where: "rating_id = ?", whereArgs: [id]);

    if(map.isEmpty) {
      return null;
    }

    return ReviewRatingModel.fromMap(map[0]);
  }

  Future<int> insertRating(ReviewRatingModel rating) async {
    return await _db.insert(
      'review_rating',
      rating.toMap(),
      conflictAlgorithm: ConflictAlgorithm.replace,
    );
  }

  Future<void> updateRating(ReviewRatingModel rating) async {
    await _db.update(
      'review_rating',
      rating.toMap(),
      where: 'rating_id = ?',
      whereArgs: [rating.id],
    );
  }

  Future<void> deleteRating(int id) async {
    await _db.delete(
      'review_rating',
      where: 'rating_id = ?',
      whereArgs: [id],
    );
  }
}
import 'package:hsk_learner/models/review_rating.dart';
import 'package:hsk_learner/services/database_service.dart';

abstract class ReviewRatingRepositoryBase {

  /// Fetch all review ratings.
  Future<List<ReviewRatingModel>> getAllReviewRatings();

  /// Fetch a review rating by its ID.
  Future<ReviewRatingModel> getReviewRatingById(int id);

  /// Add a new review rating.
  /// The `rating_id` is assigned by the database; [rating].id is ignored.
  Future<int> addReviewRating(ReviewRatingModel rating);

  /// Update an existing review rating.
  Future<void> updateReviewRating(ReviewRatingModel rating);

  /// Delete a review rating by its ID.
  Future<void> deleteReviewRating(int id);
}

/// Repository for handling all review ratings operations.
/// Review ratings are holding the possible ratings for a given review.
/// Examples like "Again", "Hard", "Easy", etc.
class ReviewRatingRepositoryImpl implements ReviewRatingRepositoryBase {
  final DatabaseServiceBase _dbService;

  ReviewRatingRepositoryImpl(this._dbService);

  @override
  Future<List<ReviewRatingModel>> getAllReviewRatings() async {
    final db = await _dbService.database;
    final List<Map<String, dynamic>> ratingsMap = await db.query(
        'review_rating',
        orderBy: 'rating_duration_start ASC'
    );
    return List.generate(
        ratingsMap.length, (index) =>
        ReviewRatingModel.fromMap(ratingsMap[index]
    ));
  }

  @override
  Future<ReviewRatingModel> getReviewRatingById(int id) async {
    final db = await _dbService.database;
    var map = await db.query(
        'review_rating',
        where: "rating_id = ?", whereArgs: [id]);

    if(map.isEmpty) {
      throw Exception("No Rating found with ID $id");
    }

    return ReviewRatingModel.fromMap(map[0]);
  }

  @override
  Future<int> addReviewRating(ReviewRatingModel rating) async {
    final db = await _dbService.database;
    // rating_id is omitted so the database assigns it (auto rowid).
    return await db.insert('review_rating', {
      'rating_name': rating.name,
      'rating_duration_start': rating.durationStart.inSeconds,
      'rating_duration_end': rating.durationEnd.inSeconds,
    });
  }

  @override
  Future<void> updateReviewRating(ReviewRatingModel rating) async {
    final db = await _dbService.database;
    await db.update(
      'review_rating',
      rating.toMap(),
      where: 'rating_id = ?',
      whereArgs: [rating.id],
    );
  }

  @override
  Future<void> deleteReviewRating(int id) async {
    final db = await _dbService.database;
    await db.delete(
      'review_rating',
      where: 'rating_id = ?',
      whereArgs: [id],
    );
  }
}

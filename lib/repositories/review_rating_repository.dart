import 'package:hsk_learner/models/review_rating.dart';
import 'package:hsk_learner/services/review_rating_service.dart';

abstract class ReviewRatingRepositoryBase {

  /// Fetch all review ratings.
  Future<List<ReviewRatingModel>> getAllReviewRatings();

  /// Fetch a review rating by its ID.
  Future<ReviewRatingModel> getReviewRatingById(int id);

  /// Add a new review rating.
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
  final ReviewRatingService _ratingService;

  ReviewRatingRepositoryImpl(this._ratingService);

  @override
  Future<List<ReviewRatingModel>> getAllReviewRatings() async {
    return await _ratingService.getAllRatings();
  }

  @override
  Future<ReviewRatingModel> getReviewRatingById(int id) async {
    var value = await _ratingService.getRating(id);

    if (value == null) {
      // TODO: Do something better
      throw Exception("No Rating found with ID $id");
    }

    return value;
  }

  @override
  Future<int> addReviewRating(ReviewRatingModel rating) async {
    return await _ratingService.insertRating(rating);
  }

  @override
  Future<void> updateReviewRating(ReviewRatingModel rating) async {
    await _ratingService.updateRating(rating);
  }

  @override
  Future<void> deleteReviewRating(int id) async {
    await _ratingService.deleteRating(id);
  }
}
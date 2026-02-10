import 'package:hsk_learner/models/review_rating.dart';
import 'package:hsk_learner/repositories/review_rating_repository.dart';

class FakeReviewRatingRepository implements ReviewRatingRepository {
  final List<ReviewRatingModel> _reviewRatings = [ ];
  var _id = 0;

  @override
  Future<int> addReviewRating(ReviewRatingModel rating) {
    _reviewRatings.add(ReviewRatingModel(
        id: _id,
        name: rating.name,
        durationStart: rating.durationStart,
        durationEnd: rating.durationEnd
    ));
    return Future.value(_id++);
  }

  @override
  Future<void> deleteReviewRating(int id) async {
    _reviewRatings.removeAt(id);
  }

  @override
  Future<List<ReviewRatingModel>> getAllReviewRatings() async {
    _reviewRatings.sort((a, b) => a.durationStart.compareTo(b.durationStart));
    return Future.value(_reviewRatings);
  }

  @override
  Future<ReviewRatingModel> getReviewRatingById(int id) async {
    return Future.value(_reviewRatings.firstWhere((r) => r.id == id));
  }

  @override
  Future<void> updateReviewRating(ReviewRatingModel rating) async {
    _reviewRatings.removeAt(rating.id);
    _reviewRatings.add(rating);
  }
}

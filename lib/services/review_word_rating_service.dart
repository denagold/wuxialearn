import 'package:hsk_learner/models/review_rating.dart';
import 'package:hsk_learner/repositories/review_repository.dart';
import 'package:hsk_learner/repositories/stat_repository.dart';
import 'package:hsk_learner/services/srs_service.dart';

abstract class ReviewWordRatingServiceBase {
  Future<void> rateWord({
    required int wordId,
    required int ratingId,
    required ReviewRatingModel rating,
  });
}

class ReviewWordRatingServiceImpl implements ReviewWordRatingServiceBase {
  final StatRepositoryBase statRepo;
  final ReviewRepositoryBase reviewRepo;
  final SRSServiceBase srsService;

  ReviewWordRatingServiceImpl({
    required this.statRepo,
    required this.reviewRepo,
    required this.srsService,
  });

  @override
  Future<void> rateWord({
    required int wordId,
    required int ratingId,
    required ReviewRatingModel rating,
  }) async {
    final int stat = (ratingId == 0 || ratingId == 1) ? 0 : 1;
    await statRepo.insertStat(value: stat, id: wordId);
    final int time = srsService.calculateNextReviewTimestamp(rating);
    await reviewRepo.updateReview(id: wordId, time: time, ratingId: ratingId);
  }
}

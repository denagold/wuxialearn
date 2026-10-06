import 'dart:math';

import 'package:hsk_learner/models/review_rating.dart';

abstract class SRSServiceBase {
  int calculateNextReviewTimestamp(ReviewRatingModel rating);
  Duration getRandomDuration(Duration min, Duration max);
}

class SRSService implements SRSServiceBase {
  final _random = Random();

  @override
  int calculateNextReviewTimestamp(ReviewRatingModel rating) {
    DateTime dateTime;
    if (rating.durationStart == rating.durationEnd) {
      dateTime = DateTime.now().add(rating.durationStart);
    } else {
      dateTime = DateTime.now().add(getRandomDuration(rating.durationStart, rating.durationEnd));
    }
    return dateTime.toUtc().millisecondsSinceEpoch ~/ 1000;
  }

  @override
  Duration getRandomDuration(Duration min, Duration max) {
    int minMs = min.inMilliseconds;
    int maxMs = max.inMilliseconds;
    int range = maxMs - minMs;
    return Duration(milliseconds: minMs + _random.nextInt(range));
  }
}

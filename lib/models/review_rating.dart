import 'package:hsk_learner/extensions/review_rating_extensions.dart';

final class ReviewRatingModel {
  final int id;
  final String name;
  final Duration durationStart;
  final Duration durationEnd;

  ReviewRatingModel({
    required this.id,
    required this.name,
    required this.durationStart,
    required this.durationEnd,
  });

  Map<String, dynamic> toMap() {
    return {
      'rating_id': id,
      'rating_name': name,
      'rating_start': durationStart.inSeconds,
      'rating_end': durationEnd.inSeconds,
    };
  }
  factory ReviewRatingModel.fromMap(Map<String, dynamic> map) => ReviewRatingModel(
        id: map['rating_id'],
        name: map['rating_name'],
        durationStart: Duration(seconds: map["rating_duration_start"]),
        durationEnd: Duration(seconds: map["rating_duration_end"]),
      );

  @override
  String toString() {
    return 'ReviewRatingModel(id: $id, name: $name, start: $durationStart, end: $durationEnd)';
  }

  String startIntervalValue() {
    return ReviewRatingInterval.intervalValue(durationStart);
  }

  String startInterval() {
    return ReviewRatingInterval.intervalUnit(durationStart);
  }

  String endIntervalValue() {
    return ReviewRatingInterval.intervalValue(durationEnd);
  }

  String endInterval() {
    return ReviewRatingInterval.intervalUnit(durationEnd);
  }

  String interval() {
    return ReviewRatingInterval.formatInterval(durationStart, durationEnd);
  }
}

List<ReviewRatingModel> createReviewRatingModel(List<Map<String, dynamic>> data) {
  return List.generate(
    data.length,
    (index) => ReviewRatingModel.fromMap(data[index]),
  );
}
class ReviewProgressData {
  final int ratingId;
  final int count;
  final String ratingName;
  final int sortOrder;

  ReviewProgressData({
    required this.ratingId,
    required this.count,
    required this.ratingName,
    required this.sortOrder,
  });

  factory ReviewProgressData.fromMap(Map<String, dynamic> map) {
    return ReviewProgressData(
      ratingId: map['rating_id'],
      count: map['count'],
      ratingName: map['rating_name'],
      sortOrder: map['rs'],
    );
  }

  @override
  String toString() {
    return 'ReviewProgressData(ratingId: $ratingId, count: $count, ratingName: $ratingName, sortOrder: $sortOrder)';
  }
}

class ReviewProgressSummary {
  final List<ReviewProgressData> progress;
  final int totalCount;
  final int ratedCount;
  final int uncategorizedCount;

  ReviewProgressSummary({
    required this.progress,
    required this.totalCount,
    required this.ratedCount,
    required this.uncategorizedCount,
  });

  factory ReviewProgressSummary.fromProgress(List<ReviewProgressData> progress) {
    int totalCount = 0;
    int ratedCount = 0;
    int uncategorizedCount = 0;

    for (final p in progress) {
      switch (p.ratingId) {
        case -1:
          uncategorizedCount = p.count;
          break;
        case -2:
          totalCount = p.count;
          break;
        default:
          ratedCount += p.count;
          break;
      }
    }

    return ReviewProgressSummary(
      progress: progress,
      totalCount: totalCount,
      ratedCount: ratedCount,
      uncategorizedCount: uncategorizedCount,
    );
  }

  @override
  String toString() {
    return 'ReviewProgressSummary(total: $totalCount, rated: $ratedCount, uncategorized: $uncategorizedCount)';
  }
}

List<ReviewProgressData> createReviewProgressDataList(List<Map<String, dynamic>> data) {
  return List.generate(data.length, (index) => ReviewProgressData.fromMap(data[index]));
}

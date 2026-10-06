class ReviewStatistics {
  final int id;
  final int rightOccurrence;
  final int wrongOccurrence;
  final int score;
  final double percentCorrect;
  final int? lastSeen;
  final String hanzi;
  final String? hsk;
  final String? pinyin;
  final String? translations0;

  ReviewStatistics({
    required this.id,
    required this.rightOccurrence,
    required this.wrongOccurrence,
    required this.score,
    required this.percentCorrect,
    this.lastSeen,
    required this.hanzi,
    this.hsk,
    this.pinyin,
    this.translations0,
  });

  factory ReviewStatistics.fromMap(Map<String, dynamic> map) {
    return ReviewStatistics(
      id: map['id'],
      rightOccurrence: map['right_occurrence'],
      wrongOccurrence: map['wrong_occurrence'],
      score: map['score'],
      percentCorrect: map['percent_correct'] != null
          ? (map['percent_correct'] as num).toDouble()
          : 0.0,
      lastSeen: map['last_seen'],
      hanzi: map['hanzi'],
      hsk: map['hsk'].toString(),
      pinyin: map['pinyin'],
      translations0: map['translations0'],
    );
  }

  @override
  String toString() {
    return 'ReviewStatistics(id: $id, right: $rightOccurrence, wrong: $wrongOccurrence, score: $score, percentCorrect: $percentCorrect, lastSeen: $lastSeen, hanzi: $hanzi)';
  }
}

List<ReviewStatistics> createReviewStatisticsList(List<Map<String, dynamic>> data) {
  return List.generate(data.length, (index) => ReviewStatistics.fromMap(data[index]));
}

class ReviewWord {
  final int id;
  final String hanzi;
  final String pinyin;
  final String translation;
  final List<String> literal;
  final String? subunit;
  final int? score;
  final double? percentCorrect;
  final int? lastSeen;

  ReviewWord({
    required this.id,
    required this.hanzi,
    required this.pinyin,
    required this.translation,
    required this.literal,
    this.subunit,
    this.score,
    this.percentCorrect,
    this.lastSeen,
  });

  factory ReviewWord.fromMap(Map<String, dynamic> map) {
    var literal = <String>[];
    if (map['char_one'] != null) {
      literal.add(map['char_one'].toString());
      if (map['char_two'] != null) {
        literal.add(map['char_two'].toString());
        if (map['char_three'] != null) {
          literal.add(map['char_three'].toString());
          if (map['char_four'] != null) {
            literal.add(map['char_four'].toString());
          }
        }
      }
    }

    return ReviewWord(
      id: map['id'],
      hanzi: map['hanzi'].toString(),
      pinyin: map['pinyin'].toString(),
      translation: map['translations0'].toString(),
      literal: literal,
      subunit: map['subunit'].toString(),
      score: map['score'],
      percentCorrect: map['percent_correct'] != null
          ? (map['percent_correct'] as num).toDouble()
          : null,
      lastSeen: map['last_seen'],
    );
  }

  Map<String, dynamic> toMap() {
    return {
      'id': id,
      'hanzi': hanzi,
      'pinyin': pinyin,
      'translations0': translation,
      'char_one': literal.isNotEmpty ? literal[0] : null,
      'char_two': literal.length > 1 ? literal[1] : null,
      'char_three': literal.length > 2 ? literal[2] : null,
      'char_four': literal.length > 3 ? literal[3] : null,
      'subunit': subunit,
      'score': score,
      'percent_correct': percentCorrect,
      'last_seen': lastSeen,
    };
  }

  @override
  String toString() {
    return 'ReviewWord(id: $id, hanzi: $hanzi, pinyin: $pinyin, translation: $translation, literal: $literal, subunit: $subunit, score: $score, percentCorrect: $percentCorrect, lastSeen: $lastSeen)';
  }
}

List<ReviewWord> createReviewWordList(List<Map<String, dynamic>> data) {
  return List.generate(data.length, (index) => ReviewWord.fromMap(data[index]));
}

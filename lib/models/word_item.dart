class WordItemModel {
  final int id;
  final String hanzi;
  final String pinyin;
  final String translation;
  final List<String> literal;


  WordItemModel({
    required this.id,
    required this.hanzi,
    required this.pinyin,
    required this.translation,
    required this.literal,
  });

  // TODO find a way to better separate chars from the translation
  Map<String, dynamic> toMap() {
    return {
      'word_id': id,
      'hanzi': hanzi,
      'pinyin': pinyin,
      'translations0': translation,
      'char_one': literal[0],
      'char_two': literal.length > 1 ? literal[1] : '',
      'char_three': literal.length > 2 ? literal[2] : '',
      'char_four': literal.length > 3 ? literal[3] : '',
    };
  }

  factory WordItemModel.fromMap(Map<String, dynamic> map) {
    var literal = <String>[];
    if (map['char_one'] != null) {
      literal.add(map['char_one']);
      if (map['char_two'] != null) {
        literal.add(map['char_two']);
        if (map['char_three'] != null) {
          literal.add(map['char_three']);
          if (map['char_four'] != null) {
            literal.add(map['char_four']);
          }
        }
      }
    }

    return WordItemModel(
      id: map['id'],
      hanzi: map['hanzi'],
      pinyin: map["pinyin"],
      translation: map["translations0"],
      literal: literal,
    );
  }

  @override
  String toString() {
    return 'WordItemModel(id: $id; hanzi: $hanzi; $pinyin; $translation; literal: $literal.toString())';
  }
}
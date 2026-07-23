import 'package:hsk_learner/models/word_item.dart';

class WordItemModelWithSubunit extends WordItemModel {
  final int subunit;

  WordItemModelWithSubunit(Map<String, dynamic> map)
      : subunit = map['subunit'] ?? 0,
        super(
          id: map['id'],
          hanzi: map["hanzi"] ?? '',
          pinyin: map["pinyin"] ?? '',
          translation: map["translations0"] ?? '',
          literal: _parseLiteral(map),
        );

  static List<String> _parseLiteral(Map<String, dynamic> map) {
    final literal = <String>[];
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
    return literal;
  }
}

extension WordItemListConverter on List<Map<String, dynamic>> {
  List<WordItemModel> toWordItemModels() {
    return map((word) => WordItemModel.fromMap(word)).toList();
  }

  List<WordItemModelWithSubunit> toWordItemModelsWithSubunit() {
    return map((word) => WordItemModelWithSubunit(word)).toList();
  }
}

class WordItem {
  late final int id;
  late final String hanzi;
  late final String pinyin;
  late final String translation;
  late final List<String> literal;

  WordItem.fromMap(Map<String, dynamic> wordMap) {
    // Parse the first four meaning of a word
    late List<String> literalList = [];
    if (wordMap["char_one"] != null) {
      literalList.add(wordMap["char_one"]);
      if (wordMap["char_two"] != null) {
        literalList.add(wordMap["char_two"]);
        if (wordMap["char_three"] != null) {
          literalList.add(wordMap["char_three"]);
          if (wordMap["char_four"] != null) {
            literalList.add(wordMap["char_four"]);
          }
        }
      }
    }

    id = int.parse(wordMap["id"].toString());
    hanzi = wordMap["hanzi"]  ?? '';
    pinyin = wordMap["pinyin"] ?? '';
    translation = wordMap["translations0"] ?? '';
    literal = literalList;
  }

  WordItem(this.id, this.hanzi, this.pinyin, this.translation, this.literal);
  
}

class WordItemWithSubunit extends WordItem {
  late final int subunit;
  WordItemWithSubunit(Map<String, dynamic> wordMap) : super.fromMap(wordMap) {
    subunit = wordMap["subunit"];
  }
}

List<WordItem> createWordList(List<Map<String, dynamic>> wordList) {
  List<WordItem> wordItemList = [];
  for (final word in wordList) {
    wordItemList.add(WordItem.fromMap(word));
  }
  return wordItemList;
}

List<WordItemWithSubunit> createWordListWithSubunit(
  List<Map<String, dynamic>> wordList,
) {
  List<WordItemWithSubunit> wordItemList = [];
  for (final word in wordList) {
    wordItemList.add(WordItemWithSubunit(word));
  }
  return wordItemList;
}

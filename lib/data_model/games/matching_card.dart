import 'package:flutter/foundation.dart';

enum CardType {
  hanzi,
  english,
}

class MatchingCard {
  final UniqueKey id;
  final String text; // Either hanzi or translation
  final CardType cardType; // Indicates if this is a hanzi card or translation card
  final String pinyin; // Only for hanzi cards
  final UniqueKey pairId; // Pair of this card with another card
  bool isMatched; // Indicates if the card has been matched

  MatchingCard({
    required this.id,
    required this.text,
    required this.cardType,
    required this.pinyin,
    required this.pairId,
    this.isMatched = false,
  });

  void setMatched(bool matched) {
    isMatched = matched;
  }
}
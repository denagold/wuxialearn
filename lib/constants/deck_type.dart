/// Deck types used for organizing words in the review system.
/// Values correspond to entries in the [review] table's [deck] column.
enum DeckType {
  /// All decks — no filtering applied.
  any,

  /// HSK curriculum deck.
  hsk,

  /// Wuxia curriculum deck.
  wuxia;

  /// All available deck types, used by UI components.
  static const List<DeckType> all = [any, hsk, wuxia];

  /// Returns the string value to use in database queries.
  String toDbString() => name;

}

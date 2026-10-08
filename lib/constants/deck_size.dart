/// Deck size presets for a review session.
/// Values correspond to labels stored in the [deckSize] preference.
enum DeckSize {
  small(10),
  medium(20),
  large(35),
  all(-1);

  const DeckSize(this.cardCount);

  /// Number of words to fetch. A negative value means no limit.
  final int cardCount;

  static const List<DeckSize> allSizes = [small, medium, large, all];

  String get label => name[0].toUpperCase() + name.substring(1);

  static DeckSize fromString(String? value) {
    return allSizes.firstWhere(
      (size) => size.label == value,
      orElse: () => small,
    );
  }
}

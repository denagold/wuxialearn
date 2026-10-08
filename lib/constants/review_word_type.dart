/// Word selection strategies for a review session.
/// Values correspond to labels stored in the [reviewWords] preference.
enum ReviewWordType {
  srs,
  random,
  difficult,
  old,
  uncategorized;

  static const List<ReviewWordType> all =
      [srs, random, difficult, old, uncategorized];

  String get label => switch (this) {
        srs => "SRS",
        random => "random words",
        difficult => "difficult words",
        old => "old words",
        uncategorized => "uncategorized",
      };

  static ReviewWordType fromString(String? value) {
    return all.firstWhere(
      (type) => type.label == value,
      orElse: () => srs,
    );
  }
}

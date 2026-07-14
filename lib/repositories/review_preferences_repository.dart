import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/services/preferences_service.dart';

abstract class ReviewPreferencesRepositoryBase {
  String get reviewType;
  set reviewType(String type);

  String get reviewWords;
  set reviewWords(String words);

  String get deckSize;
  set deckSize(String size);

  String get deckName;
  set deckName(String name);
}

class ReviewPreferencesRepositoryImpl implements ReviewPreferencesRepositoryBase {
  final PreferencesServiceBase preferencesService;

  ReviewPreferencesRepositoryImpl(this.preferencesService);

  @override
  String get reviewType =>
      preferencesService.getPreferenceString(key: PreferenceConstants.reviewType) ?? 'characters';

  @override
  set reviewType(String type) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.reviewType,
      value: type,
    );
  }

  @override
  String get reviewWords =>
      preferencesService.getPreferenceString(key: PreferenceConstants.reviewWords) ?? 'SRS';

  @override
  set reviewWords(String words) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.reviewWords,
      value: words,
    );
  }

  @override
  String get deckSize =>
      preferencesService.getPreferenceString(key: PreferenceConstants.deckSize) ?? 'Small';

  @override
  set deckSize(String size) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.deckSize,
      value: size,
    );
  }

  @override
  String get deckName =>
      preferencesService.getPreferenceString(key: PreferenceConstants.deckName) ?? '';

  @override
  set deckName(String name) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.deckName,
      value: name,
    );
  }
}

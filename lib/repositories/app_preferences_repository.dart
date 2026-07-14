import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/services/preferences_service.dart';

abstract class AppPreferencesRepositoryBase {
  bool get showTranslations;
  set showTranslations(bool value);

  bool get showPinyinByDefaultInReview;
  set showPinyinByDefaultInReview(bool value);

  bool get showSentences;
  set showSentences(bool value);

  bool get showLiteralMeaningInUnitLearn;
  set showLiteralMeaningInUnitLearn(bool value);

  String get defaultHomePage;
  set defaultHomePage(String value);

  bool get isCharacterStrokeDataDownloaded;
  set isCharacterStrokeDataDownloaded(bool value);

  String get theme;
  set theme(String value);
}

class AppPreferencesRepositoryImpl implements AppPreferencesRepositoryBase {
  final PreferencesServiceBase preferencesService;

  AppPreferencesRepositoryImpl(this.preferencesService);

  @override
  bool get showTranslations =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.showTranslations) ?? false;

  @override
  set showTranslations(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.showTranslations,
      value: value,
    );
  }

  @override
  bool get showPinyinByDefaultInReview =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.showPinyinByDefaultInReview) ?? false;

  @override
  set showPinyinByDefaultInReview(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.showPinyinByDefaultInReview,
      value: value,
    );
  }

  @override
  bool get showSentences =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.showSentences) ?? false;

  @override
  set showSentences(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.showSentences,
      value: value,
    );
  }

  @override
  bool get showLiteralMeaningInUnitLearn =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.showLiteralMeaningInUnitLearn) ?? false;

  @override
  set showLiteralMeaningInUnitLearn(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.showLiteralMeaningInUnitLearn,
      value: value,
    );
  }

  @override
  String get defaultHomePage =>
      preferencesService.getPreferenceString(key: PreferenceConstants.defaultHomePage) ?? '';

  @override
  set defaultHomePage(String value) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.defaultHomePage,
      value: value,
    );
  }

  @override
  bool get isCharacterStrokeDataDownloaded =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.isCharacterStrokeDataDownloaded) ?? false;

  @override
  set isCharacterStrokeDataDownloaded(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.isCharacterStrokeDataDownloaded,
      value: value,
    );
  }

  @override
  String get theme => preferencesService.getPreferenceString(key: PreferenceConstants.theme) ?? 'system';

  @override
  set theme(String value) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.theme,
      value: value,
    );
  }
}

import 'package:hsk_learner/service/preferences_service.dart';
import 'package:hsk_learner/constants/preference_constants.dart';

class UserPreferencesRepository {
  late final PreferencesServiceBase _preferencesService;

  UserPreferencesRepository(PreferencesServiceBase preferencesService) {
    _preferencesService = preferencesService;
  }


  // Properties for user settings
  String get defaultCourse {
    return _preferencesService.getPreference(key: PreferenceConstants.defaultCourse);
  }

  set defaultCourse(String course) {
    _preferencesService.setPreference(key: PreferenceConstants.defaultCourse, value: course);
  }

  String get defaultHomePage {
    return _preferencesService.getPreference(key: PreferenceConstants.defaultHomePage);
  }

  set defaultHomePage(String homePage) {
    _preferencesService.setPreference(key: PreferenceConstants.defaultHomePage, value: homePage);
  }

  bool get showLiteralMeaningInUnitLearn {
    var showLiteral = _preferencesService.getPreference(key: PreferenceConstants.showLiteralMeaningInUnitLearn);
    return bool.parse(showLiteral);
  }

  set showLiteralMeaningInUnitLearn(bool showLiteral) {
    _preferencesService.setPreference(key: PreferenceConstants.showLiteralMeaningInUnitLearn, value: showLiteral);
  }

  bool get showPinyinByDefaultInReview {
    var showPinyin = _preferencesService.getPreference(key: PreferenceConstants.showPinyinByDefaultInReview);
    return bool.parse(showPinyin);
  }

  set showPinyinByDefaultInReview(bool showPinyin) {
    _preferencesService.setPreference(key: PreferenceConstants.showPinyinByDefaultInReview, value: showPinyin);
  }

  bool get showSentence {
    var showSentence = _preferencesService.getPreference(key: PreferenceConstants.showSentence);
    return bool.parse(showSentence);
  }

  set showSentence(bool showSentence) {
    _preferencesService.setPreference(key: PreferenceConstants.showSentence, value: showSentence);
  }

  bool get showTranslations {
    var showTranslations = _preferencesService.getPreference(key: PreferenceConstants.showTranslations);
    return bool.parse(showTranslations);
  }

  set showTranslations(bool showTranslations) {
    _preferencesService.setPreference(key: PreferenceConstants.showTranslations, value: showTranslations);
  }

}
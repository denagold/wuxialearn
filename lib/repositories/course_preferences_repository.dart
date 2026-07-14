import 'dart:convert';

import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/services/preferences_service.dart';

abstract class CoursePreferencesRepositoryBase {
  List<String> get courses;
  set courses(List<String> value);

  String get defaultCourse;
  set defaultCourse(String value);

  bool get debug;
  set debug(bool value);

  bool get allowSkipUnits;
  set allowSkipUnits(bool value);

  bool get allowAutoCompleteUnit;
  set allowAutoCompleteUnit(bool value);
}

class CoursePreferencesRepositoryImpl implements CoursePreferencesRepositoryBase {
  final PreferencesServiceBase preferencesService;

  CoursePreferencesRepositoryImpl(this.preferencesService);

  @override
  List<String> get courses {
    final data = preferencesService.getPreferenceStringList(
      key: PreferenceConstants.courses,
    );
    return data ?? [];
  }

  @override
  set courses(List<String> value) {
    preferencesService.setPreference(
      key: PreferenceConstants.courses,
      value: value,
    );
  }

  @override
  String get defaultCourse =>
      preferencesService.getPreferenceString(key: PreferenceConstants.defaultCourse) ?? '';

  @override
  set defaultCourse(String value) {
    preferencesService.setPreferenceString(
      key: PreferenceConstants.defaultCourse,
      value: value,
    );
  }

  @override
  bool get debug =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.debug) ?? false;

  @override
  set debug(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.debug,
      value: value,
    );
  }

  @override
  bool get allowSkipUnits =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.allowSkipUnits) ?? false;

  @override
  set allowSkipUnits(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.allowSkipUnits,
      value: value,
    );
  }

  @override
  bool get allowAutoCompleteUnit =>
      preferencesService.getPreferenceBool(key: PreferenceConstants.allowAutoCompleteUnit) ?? false;

  @override
  set allowAutoCompleteUnit(bool value) {
    preferencesService.setPreferenceBool(
      key: PreferenceConstants.allowAutoCompleteUnit,
      value: value,
    );
  }
}

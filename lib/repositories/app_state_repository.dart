import 'package:hsk_learner/services/preferences_service.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/sql/preferences_sql.dart';

abstract class AppStateRepositoryBase {
  String get appVersion;
  set appVersion(String version);

  bool get isFirstRun;
  set isFirstRun(bool isFirstRun);

  bool get checkForNewVersionOnStart;
  set checkForNewVersionOnStart(bool check);

  String get dbVersion;
  set dbVersion(String version);

  String get latestDbVersionConstant;
  set latestDbVersionConstant(String version);
}

class AppStateRepositoryImpl implements AppStateRepositoryBase {
  final PreferencesServiceBase preferencesService;

  AppStateRepositoryImpl(this.preferencesService);

  @override
  String get appVersion => preferencesService.getPreferenceString(key: PreferenceConstants.appVersion) ?? '';

  @override
  set appVersion(String version) {
    preferencesService.setPreferenceString(key: PreferenceConstants.appVersion, value: version);
    PreferencesSql.setPreference(
      name: PreferenceConstants.appVersion,
      value: version,
      type: 'string',
    );
  }

  @override
  bool get isFirstRun => preferencesService.getPreferenceBool(key: PreferenceConstants.isFirstRun) ?? true;

  @override
  set isFirstRun(bool isFirstRun) {
    preferencesService.setPreferenceBool(key: PreferenceConstants.isFirstRun, value: isFirstRun);
    PreferencesSql.setPreference(
      name: PreferenceConstants.isFirstRun,
      value: isFirstRun ? "1" : "0",
      type: "bool",
    );
  }

  @override
  bool get checkForNewVersionOnStart => preferencesService.getPreferenceBool(key: PreferenceConstants.checkForNewVersionOnStart) ?? true;

  @override
  set checkForNewVersionOnStart(bool check) {
    preferencesService.setPreferenceBool(key: PreferenceConstants.checkForNewVersionOnStart, value: check);
    PreferencesSql.setPreference(
      name: PreferenceConstants.checkForNewVersionOnStart,
      value: check ? "1" : "0",
      type: "bool",
    );
  }

  @override
  String get dbVersion => preferencesService.getPreferenceString(key: PreferenceConstants.dbVersion) ?? '';

  @override
  set dbVersion(String version) {
    preferencesService.setPreferenceString(key: PreferenceConstants.dbVersion, value: version);
    PreferencesSql.setPreference(
      name: PreferenceConstants.dbVersion,
      value: version,
      type: 'string',
    );
  }

  @override
  String get latestDbVersionConstant => preferencesService.getPreferenceString(key: PreferenceConstants.latestDbVersionConstant) ?? '';

  @override
  set latestDbVersionConstant(String version) {
    preferencesService.setPreferenceString(key: PreferenceConstants.latestDbVersionConstant, value: version);
    PreferencesSql.setPreference(
      name: PreferenceConstants.latestDbVersionConstant,
      value: version,
      type: 'string',
    );
  }
}

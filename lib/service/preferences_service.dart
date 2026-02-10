import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/sql/preferences_sql.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:logging/logging.dart';

abstract class PreferencesServiceBase {
  dynamic getPreference({required String key});
  Future<void> setPreference({required String key, required dynamic value});

  Future<void> setPreferenceBool({required String key, required bool value});
  Future<void> setPreferenceInt({required String key, required int value});
  Future<void> setPreferenceDouble({required String key, required double value});
  Future<void> setPreferenceString({required String key, required String value});

  Future<bool?> getPreferenceBool({required String key});
  Future<int?> getPreferenceInt({required String key});
  Future<double?> getPreferenceDouble({required String key});
  Future<String?> getPreferenceString({required String key});

  Future<bool> hasPreference(String key);
}

class PreferencesService implements PreferencesServiceBase {
  late SharedPreferencesWithCache _prefStore;
  late final log = Logger('PreferencesService');

  Future<void> constructor() async {
    _prefStore = await SharedPreferencesWithCache.create(
      cacheOptions: const SharedPreferencesWithCacheOptions(
        allowList: <String>{
          PreferenceConstants.allowAutoCompleteUnit,
          PreferenceConstants.allowSkipUnits,
          PreferenceConstants.appVersion,
          PreferenceConstants.checkForNewVersionOnStart,
          PreferenceConstants.courses,
          PreferenceConstants.dbVersion,
          PreferenceConstants.debug,
          PreferenceConstants.defaultCourse,
          PreferenceConstants.defaultHomePage,
          PreferenceConstants.isFirstRun,
          PreferenceConstants.showLiteralMeaningInUnitLearn,
          PreferenceConstants.showPinyinByDefaultInReview,
          PreferenceConstants.showSentence,
          PreferenceConstants.showTranslations,
        },
    ));
  }

  @override
  Future<void> setPreference({required String key, required dynamic value}) async {
    // Use appropriate setter based on value type
    if (value is int) {
      await _prefStore.setInt(key, value);
    } else if (value is String) {
      await _prefStore.setString(key, value);
    } else if (value is bool) {
      await _prefStore.setBool(key, value);
    } else if (value is double) {
      await _prefStore.setDouble(key, value);
    } else if (value is List<String>) {
      await _prefStore.setStringList(key, value);
    } else {
      // For complex objects, serialize to JSON
      await _prefStore.setString(key, value.toString());
    }
  }

  @override
  dynamic getPreference({required String key}) async {
    if (!_prefStore.containsKey(key)) {
      // TODO Try to load from database if not in memory
      return null;
    }
    return _prefStore.get(key);
  }

  // TODO contains check to mitigate ArgumentException when trying to get non-existent preference

  @override
  Future<bool> hasPreference(String key) async {
    return _prefStore.containsKey(key);
  }

  @override
  Future<void> setPreferenceBool({required String key, required bool value}) async {
    if(!_prefStore.containsKey(key)) {
      // TODO Try to load from database default
      return;
    }
    await _prefStore.setBool(key, value);
  }

  @override
  Future<void> setPreferenceDouble({required String key, required double value}) async {
    if(!_prefStore.containsKey(key)) {
      // TODO Try to load from database default
      return;
    }
    await _prefStore.setDouble(key, value);
  }

  @override
  Future<void> setPreferenceInt({required String key, required int value}) async {
    if(!_prefStore.containsKey(key)) {
      // TODO Try to load from database default
      return;
    }
    await _prefStore.setInt(key, value);
  }

  @override
  Future<void> setPreferenceString({required String key, required String value}) async {
    if(!_prefStore.containsKey(key)) {
      // TODO Try to load from database default
      return;
    }
    await _prefStore.setString(key, value);
  }

  @override
  Future<bool?> getPreferenceBool({required String key}) async {
    return _prefStore.getBool(key);
  }

  @override
  Future<double?> getPreferenceDouble({required String key}) async {
    return _prefStore.getDouble(key);
  }

  @override
  Future<int?> getPreferenceInt({required String key}) async {
    return _prefStore.getInt(key);
  }

  @override
  Future<String?> getPreferenceString({required String key}) async {
    return _prefStore.getString(key);
  }


  void loadPreferences() async {
    // Load preferences from database if available
    var listOfPreferences = await PreferencesSql.getPreferences();
    for(var map in listOfPreferences) {
      switch (map['type']) {
        case 'bool':
          await setPreferenceBool(key: map['name'], value: map['value']);
          break;
        case 'double':
          await setPreferenceDouble(key: map['name'], value: map['value']);
          break;
        case 'int':
          await setPreferenceInt(key: map['name'], value: map['value']);
          break;
        case 'string':
          await setPreferenceString(key: map['name'], value: map['value']);
        default:
          throw Exception('Unknown preference type');
      }
    }
  }

  void migratePreferences() async {
    // Migrate preferences to new format if necessary

  }

}


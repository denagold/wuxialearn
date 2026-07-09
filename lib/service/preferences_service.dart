import 'dart:convert';

import 'package:hsk_learner/sql/preferences_sql.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:logging/logging.dart';

abstract class PreferencesServiceBase {
  Future<void> init();

  dynamic getPreference({required String key});
  void setPreference({required String key, required dynamic value});

  void setPreferenceBool({required String key, required bool value});
  void setPreferenceInt({required String key, required int value});
  void setPreferenceDouble({required String key, required double value});
  void setPreferenceString({required String key, required String value});
  void setPreferenceJson({required String key, required String value});

  bool? getPreferenceBool({required String key});
  int? getPreferenceInt({required String key});
  double? getPreferenceDouble({required String key});
  String? getPreferenceString({required String key});
  List<String>? getPreferenceStringList({required String key});

  bool hasPreference(String key);
}

class PreferencesService implements PreferencesServiceBase {
  static final log = Logger('PreferencesService');

  final SharedPreferencesWithCache prefs;

  PreferencesService(this.prefs);

  @override
  Future<void> init() async {
    await loadPreferences();
  }

  @override
  void setPreference({required String key, required dynamic value}) {
    if (value is int) {
      prefs.setInt(key, value);
    } else if (value is String) {
      prefs.setString(key, value);
    } else if (value is bool) {
      prefs.setBool(key, value);
    } else if (value is double) {
      prefs.setDouble(key, value);
    } else if (value is List<String>) {
      prefs.setStringList(key, value);
    } else {
      prefs.setString(key, value.toString());
    }
  }

  @override
  dynamic getPreference({required String key}) {
    if (!prefs.containsKey(key)) return null;
    return prefs.get(key);
  }

  @override
  bool hasPreference(String key) {
    return prefs.containsKey(key);
  }

  @override
  void setPreferenceBool({required String key, required bool value}) {
    prefs.setBool(key, value);
  }

  @override
  void setPreferenceDouble({required String key, required double value}) {
    prefs.setDouble(key, value);
  }

  @override
  void setPreferenceInt({required String key, required int value}) {
    prefs.setInt(key, value);
  }

  @override
  void setPreferenceString({required String key, required String value}) {
    prefs.setString(key, value);
  }

  @override
  void setPreferenceJson({required String key, required String value}) {
    // Lets just think of it as a list with strings - as its only used for that
    var data = json.decode(value);
    var array = data as List<dynamic>;

    if (data.isNotEmpty){
      var parsedList = <String>[];
      for (int i = 0; i < data.length; i++) {
          var castedValue = data[i].toString();
          parsedList.insert(i, castedValue);
      }
      prefs.setStringList(key, parsedList);
    }
  }

  @override
  bool? getPreferenceBool({required String key}) {
    return prefs.getBool(key);
  }

  @override
  double? getPreferenceDouble({required String key}) {
    return prefs.getDouble(key);
  }

  @override
  int? getPreferenceInt({required String key}) {
    return prefs.getInt(key);
  }

  @override
  String? getPreferenceString({required String key}) {
    return prefs.getString(key);
  }

  @override
  List<String>? getPreferenceStringList({required String key}) {
    return prefs.getStringList(key);
  }


  Future<void> loadPreferences() async {
    var listOfPreferences = await PreferencesSql.getPreferences();
    for (var map in listOfPreferences) {
      final name = map['name'] as String;
      final value = map['value'] as String;
      switch (map['type'] as String) {
        case 'bool':
          setPreferenceBool(
              key: name, value: value == '1' || value.toLowerCase() == 'true');
          break;
        case 'double':
          setPreferenceDouble(key: name, value: double.parse(value));
          break;
        case 'int':
          setPreferenceInt(key: name, value: int.parse(value));
          break;
        case 'string':
          setPreferenceString(key: name, value: value);
        case 'json':
          // TODO:  make proper json parsing or rather remove this type as possibility
          setPreferenceJson(key: name, value: value);
        default:
          log.severe('Unknown preference type: ${map['type']} for key: $name');
      }
    }
  }
}

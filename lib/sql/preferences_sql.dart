import 'package:hsk_learner/sql/sql_helper.dart';

class PreferencesSql {
  static Future<void> insertPreference({
    required String name,
    required String value,
    required String type,
  }) async {
    final db = await SQLHelper.db();
    await db.rawInsert("""
        insert into preferences(name, value, type) values(?, ?, ?)
        """, [name, value, type]);
  }

  static Future<void> setPreference({
    required String name,
    required String value,
    required String type,
  }) async {
    final db = await SQLHelper.db();
    await db.rawUpdate(
      "UPDATE preferences SET name = ?, value = ?, type = ? WHERE name = ?",
      [name, value, type, name],
    );
  }

  static Future<List<Map<String, dynamic>>> getPreferences() async {
    final db = await SQLHelper.db();
    return db.rawQuery("SELECT * FROM preferences; ");
  }
}

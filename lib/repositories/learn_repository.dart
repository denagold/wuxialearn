
import 'package:hsk_learner/services/database_service.dart';

abstract class LearnRepositoryBase {
  Future<List<Map<String, dynamic>>> count();
  Future<List<Map<String, dynamic>>> count2({ required String courseName });
  Future<List<Map<String, dynamic>>> getHskLevel(int unit);
  Future<List<Map<String, dynamic>>> getUnit(int unit);
  Future<List<Map<String, dynamic>>> getUnitWithLiteralMeaning(int unit);
  Future<List<Map<String, dynamic>>> getExamples(String word);
  Future<List<Map<String, dynamic>>> getSentences(int unit);
  Future<List<Map<String, dynamic>>> getSentencesForSubunit(int unit, int subunit);
  void completeUnit({required int unit});
  void completeSubUnit({required int unit, required int subUnit});
  Future<List<Map<String, dynamic>>> getSubunitInfo({ required int unit });
  Future<List<Map<String, dynamic>>> getKnownSentences( List<Map<String, dynamic>> words );
  Future<List<Map<String, dynamic>>> getTestOutWords({ required int hsk });
  Future<void> completeHskLevel({ required int hsk });
}

class LearnRepositoryImpl implements LearnRepositoryBase {
  final DatabaseServiceBase dbService;

  LearnRepositoryImpl(this.dbService);

  @override
  Future<void> completeHskLevel({required int hsk}) async {
    final db = await dbService.database;
    final batch = db.batch();
    batch.rawInsert(
      """
      UPDATE unit_info set completed = true where unit_id in
        (select unit_id from units where hsk <= ?)
      """,
      [hsk],
    );
    batch.rawInsert(
      """
      update subunit_info set completed = 1 where subunit_id in (
        SELECT subunit_id from subunits
        join units on units.unit_id = subunits.unit
        where units.hsk <= ?
        order by subunits.unit
      )
      """,
      [hsk],
    );
    batch.rawInsert(
      """
      INSERT INTO stats(date, wordid, value)
        SELECT strftime('%s', 'now'), courses.id, 1 from courses
        join units on units.unit_id = courses.unit
        where units.hsk <= ?
      """,
      [hsk],
    );
    batch.rawInsert(
      """
      INSERT INTO review(id, deck, show_next) select id, course, strftime('%s', 'now') + (1 * 24 * 60 * 60)
      from courses where hsk <= ?
      """,
      [hsk],
    );
    await batch.commit();
  }

  @override
  Future<void> completeSubUnit({required int unit, required int subUnit}) async {
    final db = await dbService.database;
    final String sql = """
      UPDATE subunit_info SET completed = 1 
      WHERE subunit_id = (
        select subunit_id from subunits
        where unit = ? AND subunit = ?
        limit 1
      )
    """;
    await db.rawInsert(sql, [unit, subUnit]);
  }

  @override
  Future<void> completeUnit({required int unit}) async {
    final db = await dbService.database;
    await db.rawInsert(
      'UPDATE unit_info SET completed = 1 WHERE unit_id = ?',
      [unit],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> count() async {
    final db = await dbService.database;
    return db.rawQuery("""
      SELECT
        SUM(CASE WHEN hsk = 1 THEN 1 ELSE 0 END) hsk1,
        SUM(CASE WHEN hsk = 2 THEN 1 ELSE 0 END) hsk2,
        SUM(CASE WHEN hsk = 3 THEN 1 ELSE 0 END) hsk3,
        SUM(CASE WHEN hsk = 4 THEN 1 ELSE 0 END) hsk4,
        SUM(CASE WHEN hsk = 5 THEN 1 ELSE 0 END) hsk5,
        SUM(CASE WHEN hsk = 6 THEN 1 ELSE 0 END) hsk6
      FROM courses  
    """);
  }

  @override
  Future<List<Map<String, dynamic>>> count2({required String courseName}) async {
    final db = await dbService.database;
    var result = db.rawQuery(
      """
      SELECT
        *
      FROM units_with_info as units
      WHERE course = ?
      and visible = 1
      ORDER BY unit_order
      """,
      [courseName],
    );
    return result;
  }

  @override
  Future<List<Map<String, dynamic>>> getExamples(String word) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
      SELECT
        characters, pinyin, meaning 
      FROM sentences  
      WHERE instr(characters, ?) > 0 
      ORDER BY unit ASC
      LIMIT 3;
      """,
      [word],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getHskLevel(int unit) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
      SELECT
        id, hanzi, pinyin, translations0, subunit
      FROM courses
      WHERE unit = ?
      ORDER BY subunit ASC
      """,
      [unit],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getKnownSentences(List<Map<String, dynamic>> words) async {
    final db = await dbService.database;
    String pattern = "^(?:(?: ，|。";
    for (int i = 0; i < words.length; i++) {
      pattern += "| ${words[i]["hanzi"]}";
    }
    pattern += "))*\$";
    return db.rawQuery(
      "SELECT * from sentences where characters REGEXP ?",
      [pattern],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getSentences(int unit) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
      SELECT
        characters, pinyin, meaning, id, subunit
      FROM sentences  
      WHERE unit = ?
      ORDER BY subunit
      """,
      [unit],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getSentencesForSubunit(int unit, int subunit) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
      SELECT
        characters, pinyin, meaning, id, subunit
      FROM sentences  
      WHERE unit = ?
      AND subunit = ?
      ORDER BY subunit
      """,
      [unit, subunit],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getSubunitInfo({required int unit}) async {
    final db = await dbService.database;
    var result = db.rawQuery(
      """
      SELECT unit, subunit, completed
      FROM subunits_with_info as subunits
      WHERE unit = ?
      """,
      [unit],
    );
    return result;
  }

  @override
  Future<List<Map<String, dynamic>>> getTestOutWords({required int hsk}) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
      SELECT
        id, hanzi, pinyin, translations0
      FROM courses
      join units on units.unit_id = courses.unit
      where units.hsk <= ?
      order by random()
      limit 15
      """,
      [hsk],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getUnit(int unit) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
      SELECT
        id, hanzi, pinyin, translations0, subunit, unit
      FROM courses
      WHERE unit = ?
      ORDER BY subunit ASC
      """,
      [unit],
    );
  }

  @override
  Future<List<Map<String, dynamic>>> getUnitWithLiteralMeaning(int unit) async {
    final db = await dbService.database;
    return db.rawQuery(
      """
        SELECT t1.id, t1.hanzi, t1.pinyin, translations0, subunit,
        a_tl.translation as char_one, b_tl.translation as char_two, c_tl.translation as char_three, d_tl.translation as char_four
            from(
              SELECT
                id, hanzi, pinyin, translations0, subunit, unit,
                SUBSTR(hanzi, 1, 1) a, SUBSTR(hanzi, 2, 1) b,
                SUBSTR(hanzi, 3, 1) c, SUBSTR(hanzi, 4, 1) d
              FROM courses
              WHERE unit = ?
            ) as t1
        left join unihan a_tl on t1.a = a_tl.hanzi
        left join unihan b_tl on t1.b = b_tl.hanzi  
        left join unihan c_tl on t1.c = c_tl.hanzi
        left join unihan d_tl on t1.d = d_tl.hanzi 
        GROUP BY t1.id
        ORDER BY subunit ASC
        """,
      [unit],
    );
  }


}

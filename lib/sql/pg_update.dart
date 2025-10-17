import 'package:hsk_learner/sql/sql_helper.dart';
import 'package:postgres/postgres.dart' hide ConnectionInfo;
import 'package:sqflite/sqflite.dart';

import 'connection_db_info.dart';

class PgUpdate {
  static late Connection connection;
  static bool connected = false;
  static Future<Connection> psql() async {
    if (connected == false || !connection.isOpen) {
      // will be added back later
      ConnectionInfo connectionInfo = ConnectionInfo();
      Endpoint endpoint = Endpoint(
          host: connectionInfo.host,
          database: connectionInfo.databaseName,
          username: connectionInfo.username,
          password: connectionInfo.password,
      );
      connection = await Connection.open(endpoint);
      connected = true;
    }
    return connection;
  }

  static Future<bool> updateSqliteFromPg() async {
    final pgdb = await psql();
    List<Map<String, Map<String, dynamic>>> hsk = (await pgdb.execute(
      """
      SELECT * FROM courses ORDER BY id
    """,
    )) as List<Map<String, Map<String, dynamic>>>;
    List<Map<String, dynamic>> hskResult = [];
    for (final row in hsk) {
      hskResult.add(row["courses"]!);
    }
    List<Map<String, Map<String, dynamic>>> sentences = (await pgdb
        .execute("""
      SELECT * FROM sentences ORDER BY id
    """)) as List<Map<String, Map<String, dynamic>>>;
    List<Map<String, dynamic>> sentencesResult = [];
    for (final row in sentences) {
      sentencesResult.add(row["sentences"]!);
    }

    List<Map<String, Map<String, dynamic>>> units = (await pgdb
        .execute("""
      SELECT * FROM units ORDER BY unit_id
    """)) as List<Map<String, Map<String, dynamic>>>;
    List<Map<String, dynamic>> unitsResult = [];
    for (final row in units) {
      unitsResult.add(row["units"]!);
    }

    List<Map<String, Map<String, dynamic>>> subUnits = (await pgdb
        .execute("""
      select unit, subunit, 0 as completed from courses
      where unit is not null and subunit is not null
      group by unit, subunit
      order by unit, subunit
    """)) as List<Map<String, Map<String, dynamic>>>;
    List<Map<String, dynamic>> subUnitsResult = [];
    for (final row in subUnits) {
      subUnitsResult.add(row["courses"]!);
    }

    final db = await SQLHelper.db();
    Batch hskBatch = db.batch();
    db.execute("delete from courses");
    for (final row in hskResult) {
      hskBatch.insert('courses', row);
    }
    hskBatch.commit();

    Batch sentenceBatch = db.batch();
    db.execute("delete from sentences");
    for (final row in sentencesResult) {
      sentenceBatch.insert('sentences', row);
    }
    sentenceBatch.commit();

    Batch unitBatch = db.batch();
    db.execute("delete from units");
    for (final row in unitsResult) {
      unitBatch.insert('units', row);
    }
    unitBatch.commit();

    Batch subUnitBatch = db.batch();
    db.execute("delete from subunits");
    for (final row in subUnitsResult) {
      subUnitBatch.insert('subUnits', row);
    }
    subUnitBatch.commit();

    return true;
  }
}

import 'dart:convert';

import 'package:archive/archive.dart';
import 'package:csv/csv.dart';
import 'package:flutter/foundation.dart';
import 'package:hsk_learner/services/database_service.dart';
import 'package:http/http.dart' as http;

abstract class CharacterRepositoryBase {
  Future<void> replaceCharacterDb();
  Future<void> removeCharacterDb();

  Future<List<Map<String, dynamic>>> getSentenceFromId(String char);
  Future<List<Map<String, dynamic>>> getCharInfo(String char);
}

class CharacterRepositoryImpl implements CharacterRepositoryBase {
  final DatabaseServiceBase dbService;

  CharacterRepositoryImpl(this.dbService);

  @override
  Future<void> replaceCharacterDb() async {
    // TODO download and parsing can be parallelized into background jobs
    const dictionaryUrl = 'https://cdn.jsdelivr.net/gh/wuxialearn/data@main/dictionary.csv';
    final responseDictionary = await downloadData(dictionaryUrl);
    final dictionaryData = await parseDictionaryData(responseDictionary.body);

    const graphicsUrl = 'https://cdn.jsdelivr.net/gh/wuxialearn/data@main/graphics.csv.bz2';
    final responseGraphics = await downloadData(graphicsUrl);
    final graphicsData = await parseStrokeData(responseGraphics.bodyBytes);

    final db = await dbService.database;
    await db.transaction((txn) async {
      final batch = txn.batch();
      batch.execute('DROP TABLE IF EXISTS stroke_info');
      batch.execute('''
      CREATE TABLE stroke_info(
        character TEXT PRIMARY KEY,
        strokes TEXT,
        medians TEXT,
        decomposition TEXT,
        etymology TEXT,
        radical TEXT,
        matches TEXT
      )
      ''');
      for (int i = 1; i < dictionaryData.length; i++) {
        batch.rawInsert('''
        INSERT INTO stroke_info(character, decomposition, etymology, radical, matches)
        VALUES(?, ?, ?, ?, ?)
        ''', dictionaryData[i]);
      }
      for (int i = 1; i < graphicsData.length; i++) {
        batch.rawUpdate(
          '''
        UPDATE stroke_info
        SET strokes = ?, medians = ?
        WHERE character = ?
        ''',
          [graphicsData[i][1], graphicsData[i][2], graphicsData[i][0]],
        );
      }
      await batch.commit();
    });
    // TODO move database scope into the service
    //await db.close();
  }

  Uint8List decompressFile(Uint8List dataBytes) {
    return BZip2Decoder().decodeBytes(dataBytes);
  }

  Future<http.Response> downloadData(String url) async {
    final dictionaryResponse = await http.get(Uri.parse(url));
    if (dictionaryResponse.statusCode != 200) {
      throw Exception('Failed to load dictionary CSV');
    }
    return dictionaryResponse;
  }

  Future<List<List<dynamic>>> parseDictionaryData(String responseBody) async {
    return CsvToListConverter().convert(
      responseBody,
    );
  }

  Future<List<List<dynamic>>> parseStrokeData(Uint8List responseBodyInBytes) async {
    final Uint8List archive = await compute(
      decompressFile,
      responseBodyInBytes,
    );
    final graphicsCsvString = utf8.decode(archive);

    return CsvToListConverter()
        .convert(graphicsCsvString);
  }

  @override
  Future<void> removeCharacterDb() async {
    final db = await dbService.database;
    await db.execute('DROP TABLE IF EXISTS stroke_info');
    // TODO move database scope into the service
    //await db.close();
  }

  @override
  Future<List<Map<String, dynamic>>> getSentenceFromId(
      String char,
      ) async {
    final db = await dbService.database;
    final a = db.rawQuery("""
    SELECT * from sentences where characters like '%$char%'
    order by unit asc
    """);
    return a;
  }

  @override
  Future<List<Map<String, dynamic>>> getCharInfo(String char) async {
    final db = await dbService.database;
    final a = db.rawQuery("""
     SELECT
     id, hanzi, pinyin, translation
     from unihan
     where hanzi = '$char'
     limit 1
    """);
    return a;
  }
}
import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/repositories/character_repository.dart';

import '../helpers/test_db.dart';

void main() {
  group('CharacterRepository', () {
    late TestDb testDb;
    late CharacterRepositoryBase repo;

    setUp(() async {
      testDb = await TestDb.open();
      // Test sentences seeded
      await testDb.db.insert('sentences', {'characters': '今天天气很好', 'unit': 1});
      await testDb.db.insert('sentences', {'characters': '天气很好', 'unit': 2});
      await testDb.db.insert('sentences', {'characters': '明天再见', 'unit': 1});
      await testDb.db.insert('unihan', {
        'hanzi': '天',
        'pinyin': 'tiān',
        'translation': 'sky; day',
      });

      repo = CharacterRepositoryImpl(TestDbService(testDb.db));
    });

    tearDown(() async {
      await testDb.close();
    });

    group('getSentenceFromId', () {
      test('LIKE ? with bound pattern %char% matches rows', () async {
        final rows = await repo.getSentenceFromId('天');
        expect(rows.length, 3);
      });

      test('a quote in the input stays literal, not SQL', () async {
        // If the value were interpolated into the SQL text, this would either
        // throw or match everything. As a bound parameter it matches nothing.
        final rows = await repo.getSentenceFromId("' OR 1=1 --");
        expect(rows, isEmpty);
      });
    });

    group('getCharInfo', () {
      test('exact hanzi lookup works', () async {
        final rows = await repo.getCharInfo('天');
        expect(rows.length, 1);
        expect(rows.first['hanzi'], '天');
      });

      test('injected quote is treated as a literal value', () async {
        final rows = await repo.getCharInfo("x' OR '1'='1");
        expect(rows, isEmpty);
      });
    });
  });
}

import 'package:flutter/material.dart';
import 'package:hsk_learner/extensions/word_item_extensions.dart';
import 'package:hsk_learner/models/review_word.dart';
import 'package:hsk_learner/repositories/learn_repository.dart';
import 'package:hsk_learner/screens/games/unit_game.dart';
import 'package:provider/provider.dart';

class ReviewQuiz extends StatefulWidget {
  final Future<List<ReviewWord>> hskList;
  const ReviewQuiz({super.key, required this.hskList});

  @override
  State<ReviewQuiz> createState() => _ReviewQuizState();
}

/// A review word paired with its example sentences.
class _QuizEntry {
  final ReviewWord word;
  final List<Map<String, dynamic>> sentences;
  const _QuizEntry(this.word, this.sentences);
}

class _ReviewQuizState extends State<ReviewQuiz> {
  late final learnRepo = context.read<LearnRepositoryBase>();

  late Future<List<_QuizEntry>> quizEntries;
  @override
  void initState() {
    quizEntries = getQuizEntries();
    super.initState();
  }

  Future<List<_QuizEntry>> getQuizEntries() async {
    final reviewWords = await widget.hskList;
    final entries = <_QuizEntry>[];
    for (final word in reviewWords) {
      entries.add(_QuizEntry(word, await learnRepo.getExamples(word.hanzi)));
    }
    return entries;
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<List<_QuizEntry>>(
      future: quizEntries,
      builder: (
        BuildContext context,
        AsyncSnapshot<List<_QuizEntry>> snapshot,
      ) {
        if (snapshot.hasData) {
          final entries = snapshot.data!;
          final wordList = entries
              .map((entry) => entry.word)
              .toList()
              .toWordItemModels();
          final sentenceList = entries
              .where((entry) => entry.sentences.isNotEmpty)
              .map((entry) => entry.sentences[0])
              .toList();
          return UnitGame(
            courseName: "",
            wordList: wordList,
            sentenceList: sentenceList,
            unit: 0,
            subunit: 0,
            lastSubunit: false,
            name: "review",
            updateUnits: () {},
          );
        } else {
          return const Center(child: CircularProgressIndicator());
        }
      },
    );
  }
}

import 'package:flutter/material.dart';
import 'package:hsk_learner/models/review_word.dart';
import 'package:hsk_learner/models/word_item.dart';
import 'package:hsk_learner/repositories/learn_repository.dart';
import 'package:hsk_learner/screens/games/unit_game.dart';
import 'package:provider/provider.dart';

class ReviewQuiz extends StatefulWidget {
  final Future<List<ReviewWord>> hskList;
  const ReviewQuiz({super.key, required this.hskList});

  @override
  State<ReviewQuiz> createState() => _ReviewQuizState();
}

class _ReviewQuizState extends State<ReviewQuiz> {
  late final learnRepo = context.read<LearnRepositoryBase>();

  late Future<List<Map<String, dynamic>>> hskMap;
  @override
  void initState() {
    hskMap = getSentenceList();
    super.initState();
  }

  @override
  Widget build(BuildContext context) {
    return FutureBuilder<List<Map<String, dynamic>>>(
      future: hskMap,
      builder: (
        BuildContext context,
        AsyncSnapshot<List<Map<String, dynamic>>> snapshot,
      ) {
        if (snapshot.hasData) {
          List<Map<String, dynamic>>? hskMap = snapshot.data;
          List<WordItemModel> wordList = [];
          List<Map<String, dynamic>> sentenceList = [];
          if (hskMap != null) {
            for (var i = 0; i < hskMap.length; i++) {
              wordList.add(WordItemModel.fromMap(hskMap[i]["hskList"]![0]));
              if (hskMap[i]["sentenceList"] != null) {
                if (hskMap[i]["sentenceList"]!.isNotEmpty) {
                  sentenceList.add(hskMap[i]["sentenceList"]![0]);
                }
              }
            }
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
            return Container();
          }
        } else {
          return const Center(child: CircularProgressIndicator());
        }
      },
    );
  }

  Future<List<Map<String, dynamic>>>
  getSentenceList() async {
    List<ReviewWord> completedHskList = await widget.hskList;
    List<Map<String, dynamic>> result = [];
    for (var i = 0; i < completedHskList.length; i++) {
      final data = await learnRepo.getExamples(completedHskList[i].hanzi);
      Map<String, dynamic> hskMap = {
        "hskList": [completedHskList[i].toMap()],
        "sentenceList": data,
      };
      result.add(hskMap);
    }
    return result;
  }
}

import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:hsk_learner/data_model/word_item.dart';
import 'package:hsk_learner/repositories/course_preferences_repository.dart';
import 'package:hsk_learner/repositories/learn_repository.dart';
import 'package:hsk_learner/screens/learn/unit_learn.dart';
import 'package:hsk_learner/services/audio_service.dart';
import 'package:provider/provider.dart';
import '../../widgets/hsk_listview/hsk_listview.dart';
import '../games/unit_game.dart';

class SubunitView extends StatefulWidget {
  const SubunitView({
    super.key,
    required this.wordList,
    required this.unit,
    required this.subunit,
    required this.lastSubunit,
    required this.name,
    required this.completed,
    required this.updateUnits,
    required this.courseName,
  });
  final List<WordItem> wordList;
  final int unit;
  final int subunit;
  final bool lastSubunit;
  final String name;
  final bool completed;
  final Function updateUnits;
  final String courseName;

  @override
  State<SubunitView> createState() => _SubunitViewState();
}

class _SubunitViewState extends State<SubunitView> {
  late final coursePrefs = context.read<CoursePreferencesRepositoryBase>();
  late final learnRepo = context.read<LearnRepositoryBase>();
  late final _audioService = context.read<AudioServiceBase>();
  late Future<List<Map<String, dynamic>>> sentenceList;
  late final bool debug = coursePrefs.debug;
  late final bool allowSkipUnits = coursePrefs.allowSkipUnits;

  @override
  void initState() {
    sentenceList = learnRepo.getSentencesForSubunit(widget.unit, widget.subunit);
    super.initState();
  }

  @override
  Widget build(BuildContext context) {
    return CupertinoPageScaffold(
      navigationBar: CupertinoNavigationBar(
        middle: Text("Subunit ${widget.unit}"),
      ),
      child: SafeArea(
        child: Column(
          children: [
            const SizedBox(height: 20),
            Expanded(
              child: ListView.builder(
                physics: const ScrollPhysics(),
                padding: EdgeInsets.zero,
                scrollDirection: Axis.vertical,
                itemCount: widget.wordList.length,
                itemBuilder: (context, index) {
                  return HskListviewItem(
                    wordItem: widget.wordList[index],
                    showTranslation: true,
                    showPinyin: true,
                    separator: true,
                    callback: (String s) {
                      _audioService.speak(s);
                    },
                    showPlayButton: true,
                  );
                },
              ),
            ),
            FutureBuilder<List<Map<String, dynamic>>>(
              future: sentenceList,
              builder: (
                BuildContext context,
                AsyncSnapshot<List<Map<String, dynamic>>> snapshot,
              ) {
                if (snapshot.hasData) {
                  List<Map<String, dynamic>> sentenceList = snapshot.data!;
                  return Row(
                    children: [
                      Flexible(
                        fit: FlexFit.tight,
                        child: Padding(
                          padding: const EdgeInsets.all(8.0),
                          child: TextButton(
                            onPressed: () {
                              Navigator.pushReplacement(
                                context,
                                MaterialPageRoute(
                                  builder:
                                      (context) => UnitLearn(
                                        courseName: widget.courseName,
                                        wordList: widget.wordList,
                                        unit: widget.unit,
                                        subunit: widget.subunit,
                                        lastSubunit: widget.lastSubunit,
                                        name: widget.name,
                                        updateUnits: widget.updateUnits,
                                      ),
                                ),
                              ).then((_) {
                                // TODO Unhandled Exception: Null check operator used on a null value
                                Navigator.pop(context);
                              });
                            },
                            child: const Text(
                              "Learn",
                              style: TextStyle(
                                color: Colors.blue,
                                fontSize: 25,
                              ),
                            ),
                          ),
                        ),
                      ),
                      widget.completed || debug || allowSkipUnits
                          ? Flexible(
                            fit: FlexFit.tight,
                            child: Padding(
                              padding: const EdgeInsets.all(8.0),
                              child: TextButton(
                                onPressed: () {
                                  Navigator.push(
                                    context,
                                    MaterialPageRoute(
                                      builder:
                                          (context) => UnitGame(
                                            wordList: widget.wordList,
                                            unit: widget.unit,
                                            sentenceList: sentenceList,
                                            subunit: widget.subunit,
                                            lastSubunit: widget.lastSubunit,
                                            name: "",
                                            updateUnits: widget.updateUnits,
                                            courseName: widget.courseName,
                                          ),
                                    ),
                                  ).then((_) {
                                    widget.updateUnits();
                                    Navigator.pop(context);
                                  });
                                },
                                child: const Text(
                                  "Quiz",
                                  style: TextStyle(
                                    color: Colors.blue,
                                    fontSize: 25,
                                  ),
                                ),
                              ),
                            ),
                          )
                          : const SizedBox(height: 0),
                    ],
                  );
                } else {
                  return const SizedBox(height: 0);
                }
              },
            ),
          ],
        ),
      ),
    );
  }
}

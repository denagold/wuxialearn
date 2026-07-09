import 'package:flutter/material.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/service/preferences_service.dart';
import 'package:provider/provider.dart';
import '../../sql/learn_sql.dart';
import 'course_view.dart';

class CustomCourse extends StatefulWidget {
  final String courseName;
  final void Function(String courseName) changeCourse;
  const CustomCourse({
    super.key,
    required this.courseName,
    required this.changeCourse,
  });

  @override
  State<CustomCourse> createState() => _CustomCourseState();
}

class _CustomCourseState extends State<CustomCourse> {
  late final prefs = context.read<PreferencesServiceBase>();
  late Future<List<Map<String, dynamic>>> unitNumList;
  @override
  void initState() {
    super.initState();
    unitNumList = getUnitNum();
  }

  Future<List<Map<String, dynamic>>> getUnitNum() async {
    final data = await LearnSql.count2(courseName: widget.courseName);
    return data;
  }

  void update() {
    setState(() {
      unitNumList = getUnitNum();
    });
  }

  late bool allowSkipUnits = prefs.getPreference(key: PreferenceConstants.allowSkipUnits);
  List<Widget> gridItems(List<Map<String, dynamic>> hskList) {
    return [
      SliverGrid(
        gridDelegate: const SliverGridDelegateWithMaxCrossAxisExtent(
          maxCrossAxisExtent: 100.0,
          mainAxisSpacing: 30.0,
          crossAxisSpacing: 30.0,
          childAspectRatio: 1,
        ),
        delegate: SliverChildBuilderDelegate((BuildContext context, int index) {
          return GridItem(
            index: index,
            unitList: hskList,
            updateUnits: update,
            courseName: widget.courseName,
            allowSkipUnits: allowSkipUnits,
          );
        }, childCount: hskList.length),
      ),
    ];
  }

  @override
  Widget build(BuildContext context) {
    return CourseView(
      unitList: unitNumList,
      update: update,
      courseName: widget.courseName,
      gridItems: gridItems,
      changeCourse: widget.changeCourse,
    );
  }
}

import 'package:flutter/cupertino.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/screens/courses/hsk_course.dart';
import 'package:hsk_learner/service/preferences_service.dart';
import 'package:provider/provider.dart';

import 'custom_course.dart';

class CourseHome extends StatefulWidget {
  const CourseHome({super.key});

  @override
  State<CourseHome> createState() => _CourseHomeState();
}

class _CourseHomeState extends State<CourseHome> {
  late final prefs = context.read<PreferencesServiceBase>();
  @override
  Widget build(BuildContext context) {
    final course = prefs.getPreference(key: PreferenceConstants.defaultCourse);
    return CupertinoPageScaffold(
      navigationBar: const CupertinoNavigationBar(
        //backgroundColor: Colors.transparent,
        middle: Text("Home"),
      ),
      child: SafeArea(
        child: switch (course) {
          'hsk' => HSKCourseView(changeCourse: changeCourse),
          _ => CustomCourse(courseName: course, changeCourse: changeCourse),
        },
      ),
    );
  }

  void changeCourse(String courseName) {
    setState(() {
      prefs.setPreference(key: PreferenceConstants.defaultCourse, value: courseName);
    });
  }
}

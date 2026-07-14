import 'package:flutter/cupertino.dart';
import 'package:hsk_learner/repositories/course_preferences_repository.dart';
import 'package:hsk_learner/screens/courses/hsk_course.dart';
import 'package:provider/provider.dart';

import 'custom_course.dart';

class CourseHome extends StatefulWidget {
  const CourseHome({super.key});

  @override
  State<CourseHome> createState() => _CourseHomeState();
}

class _CourseHomeState extends State<CourseHome> {
  late final coursePrefs = context.read<CoursePreferencesRepositoryBase>();
  @override
  Widget build(BuildContext context) {
    final course = coursePrefs.defaultCourse;
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
      coursePrefs.defaultCourse = courseName;
    });
  }
}

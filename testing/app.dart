import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/service/theme_service.dart';

Future<void> testApp(
    WidgetTester tester,
    Widget body) async {
  tester.view.devicePixelRatio = 1.0;
  await tester.binding.setSurfaceSize(const Size(800, 1200));
  await tester.pumpWidget(
    CupertinoApp(
      theme: AppTheme.getCupertinoTheme(Brightness.light),
      scrollBehavior: const CupertinoScrollBehavior(),
      home: Scaffold(body: body),
    ),
  );
}

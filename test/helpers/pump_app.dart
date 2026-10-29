import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/services/theme_service.dart';
import 'package:provider/provider.dart';

Future<void> pumpApp(
  WidgetTester tester,
  Widget widget, {
  Brightness brightness = Brightness.light,
}) async {
  tester.view.devicePixelRatio = 1.0;
  await tester.binding.setSurfaceSize(const Size(800, 1200));

  final themeService = ThemeService();
  themeService.setBrightness(brightness);

  await tester.pumpWidget(
    MultiProvider(
      providers: [
        Provider<ThemeServiceBase>(create: (_) => themeService),
      ],
      child: CupertinoApp(
        theme: themeService.getCupertinoTheme(),
        scrollBehavior: const CupertinoScrollBehavior(),
        home: MaterialApp(
          theme: themeService.getMaterialTheme(),
          home: Scaffold(body: widget),
        ),
      ),
    ),
  );

  await tester.pumpAndSettle();
}

import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/service/theme_service.dart';
import 'package:provider/provider.dart';

Future<void> testApp(
    WidgetTester tester,
    Widget body) async {
  tester.view.devicePixelRatio = 1.0;
  await tester.binding.setSurfaceSize(const Size(800, 1200));
  final themeService = ThemeService();
  themeService.setBrightness(Brightness.light);

  await tester.pumpWidget(
    MultiProvider(
      providers: [
        Provider<ThemeServiceBase>(create: (context) => themeService),
        // Provider<AudioServiceBase>(create: (context) => AudioService()),
        // Provider<PreferencesServiceBase>(create: (context) => PreferencesService(),),
        // Provider<UserPreferencesRepository>(create: (context) => UserPreferencesRepository(context.read<PreferencesServiceBase>())),
      ],
      child: CupertinoApp(
        theme: themeService.getCupertinoTheme(),
        scrollBehavior: const CupertinoScrollBehavior(),
        home: Scaffold(body: body),
      )
    ),
  );
}

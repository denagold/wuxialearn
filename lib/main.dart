import 'package:flutter/cupertino.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/repositories/app_state_repository.dart';
import 'package:hsk_learner/repositories/app_preferences_repository.dart';
import 'package:hsk_learner/repositories/character_repository.dart';
import 'package:hsk_learner/repositories/course_preferences_repository.dart';
import 'package:hsk_learner/repositories/review_preferences_repository.dart';
import 'package:hsk_learner/repositories/review_rating_repository.dart';
import 'package:hsk_learner/repositories/word_repository.dart';
import 'package:hsk_learner/services/audio_service.dart';
import 'package:hsk_learner/services/database_service.dart';
import 'package:hsk_learner/services/preferences_service.dart';
import 'package:hsk_learner/services/theme_service.dart';
import 'package:hsk_learner/utils/platform_info.dart';
import 'package:logging/logging.dart';
import 'package:provider/provider.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:hsk_learner/screens/home/load_app.dart';

import 'repositories/learn_repository.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  initSettings();

  final prefs = await SharedPreferencesWithCache.create(
    cacheOptions: SharedPreferencesWithCacheOptions(allowList: null),
  );

  runApp(
    MultiProvider(
      providers: [
        // Services
        Provider<AudioServiceBase>(create: (context) => AudioService()),
        Provider<DatabaseServiceBase>(create: (context) => DatabaseServiceImpl()),
        Provider<ThemeServiceBase>(create: (context) => ThemeService()),

        // Preference related
        Provider<SharedPreferencesWithCache>(create: (_) => prefs),
        Provider<PreferencesServiceBase>(
          create: (ctx) => PreferencesService(ctx.read()),
        ),
        Provider<AppPreferencesRepositoryBase>(
          create: (context) => AppPreferencesRepositoryImpl(context.read()),
        ),
        Provider<AppStateRepositoryBase>(
          create: (context) => AppStateRepositoryImpl(context.read()),
        ),
        Provider<CoursePreferencesRepositoryBase>(
          create: (context) => CoursePreferencesRepositoryImpl(context.read()),
        ),
        Provider<ReviewPreferencesRepositoryBase>(
          create: (context) => ReviewPreferencesRepositoryImpl(context.read()),
        ),

        // Review related
        Provider<CharacterRepositoryBase>(create: (context) => CharacterRepositoryImpl(context.read())),
        Provider<LearnRepositoryBase>(create: (context) => LearnRepositoryImpl(context.read())),
        Provider<ReviewRatingRepositoryBase>(create: (context) => ReviewRatingRepositoryImpl(context.read())),
        Provider<WordRepositoryBase>(create: (context) => WordRepositoryImpl(context.read())),

      ],
      child: const MyApp(fdroid: true),
    ),
  );
}

void initSettings() {
  Logger.root.level = Level.INFO;

  // Write logs to stdout for now
  Logger.root.onRecord.listen((record) {
    if (kDebugMode) {
      print('${record.level.name}: ${record.time}: ${record.message}');
    }
  });

  if (PlatformInfo.isDesktop()) {
    // Initialize FFI
    sqfliteFfiInit();
    databaseFactory = databaseFactoryFfi;
  }
  WidgetsFlutterBinding.ensureInitialized();
  SystemChrome.setSystemUIOverlayStyle(
    const SystemUiOverlayStyle(
      statusBarColor: Colors.transparent,
      systemStatusBarContrastEnforced: false,
      statusBarIconBrightness: Brightness.dark,
    ),
  );
}

class MyApp extends StatefulWidget {
  final bool fdroid;
  const MyApp({super.key, required this.fdroid});

  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  @override
  Widget build(BuildContext context) {
    final ThemeServiceBase themeService = context.read<ThemeServiceBase>();
    final PreferencesServiceBase prefs = context.read<PreferencesServiceBase>();
    //set theme to light if not set
    if (prefs.getPreference(key: PreferenceConstants.theme) == null) {
      prefs.setPreference(key: PreferenceConstants.theme, value: ThemeConstants.light);
    }
    final brightness = switch (prefs.getPreference(key: PreferenceConstants.theme)) {
      ThemeConstants.dark => Brightness.dark,
      ThemeConstants.light => Brightness.light,
      ThemeConstants.system => MediaQuery.platformBrightnessOf(context),
      _ => Brightness.light,
    };
    themeService.setBrightness(brightness);
    return Theme(
      data: themeService.getMaterialTheme(),
      child: CupertinoApp(
        theme: themeService.getCupertinoTheme(),
        scrollBehavior: const CupertinoScrollBehavior(),
        title: 'Wuxia Learn',
        home: LoadApp(fdroid: widget.fdroid),
      ),
    );
  }
}

class MyApp2 extends StatelessWidget {
  const MyApp2({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      scrollBehavior: const CupertinoScrollBehavior(),
      theme: ThemeData.light(useMaterial3: false),
      darkTheme: ThemeData.dark(useMaterial3: false),
      themeMode: ThemeMode.system,
      title: 'Wuxia Learn',
      home: const LoadApp(fdroid: true),
    );
  }
}

import 'package:flutter/cupertino.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/repositories/user_preferences_repository.dart';
import 'package:hsk_learner/screens/settings/preferences.dart';
import 'package:hsk_learner/service/audio_service.dart';
import 'package:hsk_learner/service/preferences_service.dart';
import 'package:hsk_learner/service/theme_service.dart';
import 'package:hsk_learner/utils/platform_info.dart';
import 'package:logging/logging.dart';
import 'package:provider/provider.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:hsk_learner/screens/home/load_app.dart';


Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  initSettings();

  final prefs = await SharedPreferencesWithCache.create(
    cacheOptions: SharedPreferencesWithCacheOptions(allowList: null),
  );

  runApp(
    MultiProvider(
      providers: [
        Provider<SharedPreferencesWithCache>(create: (_) => prefs),
        Provider<PreferencesServiceBase>(
          create: (ctx) => PreferencesService(ctx.read()),
        ),
        Provider<ThemeServiceBase>(create: (context) => ThemeService()),
        Provider<AudioServiceBase>(create: (context) => AudioService()),
        Provider<UserPreferencesRepository>(
          create: (context) => UserPreferencesRepository(context.read()),
        ),
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
  late Future<void> initPrefs;

  @override
  void initState() {
    initPrefs = SharedPrefs.init();
    super.initState();
  }

  @override
  Widget build(BuildContext context) {
    final ThemeServiceBase themeService = context.read<ThemeServiceBase>();
    final PreferencesServiceBase prefs = context.read<PreferencesServiceBase>();
    prefs.setPreference(key: PreferenceConstants.debug, value: true);
    return FutureBuilder(
      future: initPrefs,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.done) {
         // final SharedPreferences prefs = SharedPrefs.prefs;
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
        } else {
          return const SizedBox();
        }
      },
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

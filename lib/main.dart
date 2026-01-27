import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:hsk_learner/screens/settings/preferences.dart';
import 'package:hsk_learner/service/audio_service.dart';
import 'package:hsk_learner/service/theme_service.dart';
import 'package:hsk_learner/utils/platform_info.dart';
import 'package:provider/provider.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:sqflite_common_ffi/sqflite_ffi.dart';
import 'package:hsk_learner/screens/home/load_app.dart';

void main() {
  initSettings();
  runApp(
    MultiProvider(
      providers: [
        Provider<AppTheme>(create: (context) => AppTheme()),
        Provider<AudioService>(create: (context) => AudioService()),
      ],
      child: const MyApp(fdroid: true)
    )
  );
}

void initSettings() {
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
    return FutureBuilder(
      future: initPrefs,
      builder: (context, snapshot) {
        if (snapshot.connectionState == ConnectionState.done) {
          final SharedPreferences prefs = SharedPrefs.prefs;
          //set theme to light if not set
          if (prefs.getString('theme') == null) {
            prefs.setString('theme', 'light');
          }
          final brightness = switch (prefs.getString('theme')) {
            "dark" => Brightness.dark,
            "light" => Brightness.light,
            "system" => MediaQuery.platformBrightnessOf(context),
            _ => Brightness.light,
          };
          return Theme(
            data: AppTheme.getMaterialTheme(brightness),
            child: CupertinoApp(
              theme: AppTheme.getCupertinoTheme(brightness),
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
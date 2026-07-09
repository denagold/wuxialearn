import 'package:flutter/cupertino.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/screens/home/home_page.dart';
import 'package:hsk_learner/service/preferences_service.dart';
import 'package:hsk_learner/sql/load_app_sql.dart';
import 'package:hsk_learner/sql/schema_migration.dart';
import 'package:pubspec_parse/pubspec_parse.dart';
import 'package:provider/provider.dart';
import 'package:version/version.dart';
import '../../sql/preferences_sql.dart';
import '../settings/preferences.dart';
import 'package:http/http.dart' as http;

class LoadApp extends StatefulWidget {
  final bool fdroid;
  const LoadApp({super.key, this.fdroid = false});

  @override
  State<LoadApp> createState() => _LoadAppState();
}

class _LoadAppState extends State<LoadApp> {
  late final _preferencesService = context.read<PreferencesServiceBase>();
  bool isLoading = true;
  @override
  void initState() {
    _preferencesService.init();
    getPreferences();
    super.initState();
  }

  void getPreferences() async {
    //this could cause issues if Preferences should be changed after the
    //schema migration.
    await Preferences.initPreferences();
    final Version appVersion = Version.parse(_preferencesService.getPreference(key: PreferenceConstants.appVersion));
    final Version latestVersion = Version.parse(await _getAppVersion());
    final bool isFirstRun = _preferencesService.getPreference(key: PreferenceConstants.isFirstRun);
    if(appVersion < latestVersion && !isFirstRun){
      _preferencesService.setPreference(key: PreferenceConstants.appVersion, value: latestVersion);
      PreferencesSql.setPreference(name: PreferenceConstants.appVersion, value: latestVersion.toString(), type: "string");
      await SchemaMigration.run();
      showUpdateChangesModal();
    }
    init();
  }

  Future<String> _getAppVersion() async {
    final content = await rootBundle.loadString('pubspec.yaml');
    final pubspec = Pubspec.parse(content);
    final version = pubspec.version?.toString() ?? 'Unknown';
    return version.split('+').first;
  }

  void showUpdateChangesModal() async{
    final Version appVersion = Version.parse(_preferencesService.getPreference(key: PreferenceConstants.appVersion));
    if(appVersion <= Version.parse("1.3.3")){
      final bool isFirstRun = _preferencesService.getPreference(key: PreferenceConstants.isFirstRun);
      if(!isFirstRun) {
        showCupertinoDialog(
        context: context,
        builder: (BuildContext context) => CupertinoAlertDialog(
          title: const Text("What's new"),
          content: const Column(
            children: [
              SizedBox(height: 10),
              Text(" Sentences have been rewritten for all units from HSK 1 - 3. "),
              Text("Some units have been reordered."),
              Text("Some words have moved to different units."),
            ],
          ),
          actions: [
            CupertinoDialogAction(
              child: const Text("Close"),
              onPressed: (){
                Navigator.pop(context);
              },
            )
          ]
        )
      );
      }
    }
  }

  void init() {
    final String currentVersion = _preferencesService.getPreference(key: PreferenceConstants.dbVersion);
    final String latestVersion = _preferencesService.getPreference(key: 
      PreferenceConstants.latestDbVersionConstant,
    );
    print("currentVersion: $currentVersion");
    print("latestVersion: $latestVersion");
    if (currentVersion != latestVersion) {
      //print("backing up...");
      //Backup.startBackupFromTempDir();
    }
    final bool check = _preferencesService.getPreference(key: 
      PreferenceConstants.checkForNewVersionOnStart,
    );
    final bool isFirstRun = _preferencesService.getPreference(key: PreferenceConstants.isFirstRun);
    if (check && !isFirstRun) {
      checkForDbUpdate();
    }
    setState(() {
      isLoading = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    if (isLoading) {
      return const Loading();
    } else {
      final bool isFirstRun = _preferencesService.getPreference(key: PreferenceConstants.isFirstRun);
      if (isFirstRun) {
        if (widget.fdroid) {
          Future.delayed(const Duration(seconds: 0)).then((_) {
            _showActionSheet(context);
          });
        } else {
          setFirstRun();
          setCheckForUpdate(true);
          checkForDbUpdate();
        }
        return const MyHomePage();
      } else {
        return const MyHomePage();
      }
    }
  }

  void _showActionSheet<bool>(BuildContext context) {
    showCupertinoModalPopup<bool>(
      context: context,
      builder:
          (BuildContext context) => CupertinoActionSheet(
            title: const Text('Check for update on app start? (recommended)'),
            actions: [
              CupertinoActionSheetAction(
                isDefaultAction: true,
                onPressed: () {
                  setFirstRun();
                  setCheckForUpdate(true);
                  Navigator.pop(context, true);
                  checkForDbUpdate();
                },
                child: const Text("Yes"),
              ),
              CupertinoActionSheetAction(
                isDefaultAction: true,
                onPressed: () {
                  setFirstRun();
                  setCheckForUpdate(false);
                  Navigator.pop(context, true);
                },
                child: const Text("No"),
              ),
            ],
          ),
    );
  }

  void setFirstRun() {
    PreferencesSql.setPreference(name: PreferenceConstants.isFirstRun, value: "0", type: "bool");
    _preferencesService.setPreference(key: PreferenceConstants.isFirstRun, value: false);
  }

  void setCheckForUpdate(bool setState) {
    PreferencesSql.setPreference(
      name: PreferenceConstants.checkForNewVersionOnStart,
      value: setState ? "1" : "0",
      type: "bool",
    );
    _preferencesService.setPreference(
      key: PreferenceConstants.checkForNewVersionOnStart,
      value: setState,
    );
  }

  void checkForDbUpdate() async {
    print("checking for update");
    final String lastVersion = _preferencesService.getPreference(key: PreferenceConstants.dbVersion);
    const String versionUrl =
        'https://cdn.jsdelivr.net/gh/wuxialearn/data@main/version';
    final req = await http.get(Uri.parse(versionUrl));
    final String version = req.body.trim();
    //disable for this release as we will update sqlite file directly
    if (version != lastVersion && 1 == 0) {
      await LoadAppSql.updateSqliteFromCsv();
      _preferencesService.setPreference(key: PreferenceConstants.dbVersion, value: version);
      PreferencesSql.setPreference(
        name: PreferenceConstants.dbVersion,
        value: version,
        type: 'string',
      );
      //todo: when we implement real state management we should update the courses screen here
      setState(() {});
    }
  }
}

class Loading extends StatelessWidget {
  const Loading({super.key});
  @override
  Widget build(BuildContext context) {
    return const CupertinoPageScaffold(
      child:
          !kIsWeb
              ? SizedBox(height: 10)
              : Visibility(
                visible: false,
                maintainState: true,
                maintainSize: true,
                maintainAnimation: true,
                maintainInteractivity: true,
                maintainSemantics: true,
                child: Text("Load zh 中文"),
              ),
    );
  }
}

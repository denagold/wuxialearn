import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';
import 'package:hsk_learner/constants/preference_constants.dart';
import 'package:hsk_learner/sql/pg_update.dart';
import 'package:hsk_learner/sql/sql_helper.dart';
import 'package:provider/provider.dart';
import '../../service/preferences_service.dart';
import '../../sql/character_stokes_sql.dart';
import '../../utils/platform_info.dart';
import 'backup.dart';
import 'package:pubspec_parse/pubspec_parse.dart';
import 'package:flutter/services.dart';

class Settings extends StatefulWidget {
  const Settings({super.key});

  @override
  State<Settings> createState() => _SettingsState();
}

class _SettingsState extends State<Settings> {
  late final _preferencesService = context.read<PreferencesServiceBase>();

  late bool translation;
  late bool reviewPinyin;
  late bool checkVersionOnStart;
  late bool debug;
  late bool allowSkipUnits;
  late bool showExampleSentences;
  late bool allowAutoComplete;
  late bool showLiteralInUnitLearn;
  late List<String> courses;
  late String defaultCourse;
  late String defaultHomePage;
  late bool isDataDownloaded;
  late String theme;
  List<String> homePages = ["home", "review", "stats"];
  String version = '1.0.13';
  int clicks = 0;
  bool showDebugOptions = false;
  bool isDownloading = false;
  bool isDeleting = false;

  @override
  void initState() {
    super.initState();
    translation = _preferencesService.getPreference(
      key: PreferenceConstants.showTranslations,
    ) ?? false;
    reviewPinyin = _preferencesService.getPreference(
      key: PreferenceConstants.showPinyinByDefaultInReview,
    ) ?? false;
    checkVersionOnStart = _preferencesService.getPreference(
      key: PreferenceConstants.checkForNewVersionOnStart,
    ) ?? false;
    debug = _preferencesService.getPreference(
      key: PreferenceConstants.debug,
    ) ?? false;
    allowSkipUnits = _preferencesService.getPreference(
      key: PreferenceConstants.allowSkipUnits,
    ) ?? false;
    showExampleSentences = _preferencesService.getPreference(
      key: PreferenceConstants.showSentences,
    ) ?? false;
    allowAutoComplete = _preferencesService.getPreference(
      key: PreferenceConstants.allowAutoCompleteUnit,
    ) ?? false;
    showLiteralInUnitLearn = _preferencesService.getPreference(
      key: PreferenceConstants.showLiteralMeaningInUnitLearn,
    ) ?? false;
    courses = _preferencesService.getPreference(
      key: PreferenceConstants.courses,
    ) ?? [];
    defaultCourse = _preferencesService.getPreference(
      key: PreferenceConstants.defaultCourse,
    ) ?? '';
    defaultHomePage = _preferencesService.getPreference(
      key: PreferenceConstants.defaultHomePage,
    ) ?? '';
    isDataDownloaded = _preferencesService.getPreference(
      key: 'character_stroke_data_downloaded',
    ) ?? false;
    theme = _preferencesService.getPreference(key: 'theme') ?? 'system';
  }

  setSettingBool({
    required String name,
    required bool value,
  }) {
    _preferencesService.setPreference(key: name, value: value);
  }

  setSettingString({
    required String name,
    required String value,
  }) {
    _preferencesService.setPreference(key: name, value: value);
  }

  _showDefaultCourseActionSheet<bool>(BuildContext context) {
    showCupertinoModalPopup<bool>(
      context: context,
      builder:
          (BuildContext context) => CupertinoActionSheet(
            title: const Text('Select a default course'),
            actions: List<CupertinoActionSheetAction>.generate(courses.length, (
              index,
            ) {
              return CupertinoActionSheetAction(
                isDefaultAction: true,
                onPressed: () {
                  setSettingString(
                    name: PreferenceConstants.defaultCourse,
                    value: courses[index],
                  );
                  Navigator.pop(context, true);
                  setState(() {
                    defaultCourse = _preferencesService.getPreference(key: PreferenceConstants.defaultCourse)!;
                  });
                },
                child: Text(courses[index]),
              );
            }),
          ),
    );
  }

  _showDefaultHomePageActionSheet<bool>(BuildContext context) {
    showCupertinoModalPopup<bool>(
      context: context,
      builder:
          (BuildContext context) => CupertinoActionSheet(
            title: const Text('Select a default home page'),
            actions: List<CupertinoActionSheetAction>.generate(
              homePages.length,
              (index) {
                return CupertinoActionSheetAction(
                  isDefaultAction: true,
                  onPressed: () {
                    setSettingString(
                      name: PreferenceConstants.defaultHomePage,
                      value: homePages[index],
                    );
                    Navigator.pop(context, true);
                    setState(() {
                      defaultHomePage = homePages[index];
                    });
                  },
                  child: Text(homePages[index]),
                );
              },
            ),
          ),
    );
  }

  _showThemeSelectionDialog(BuildContext context) {
    showCupertinoModalPopup(
      context: context,
      builder:
          (BuildContext context) => CupertinoActionSheet(
            title: const Text('Select Theme'),
            actions: [
              CupertinoActionSheetAction(
                onPressed: () {
                  setSettingString(name: 'theme', value: 'light');
                  setState(() => theme = 'light');
                  Navigator.pop(context);
                },
                child: const Text('Light'),
              ),
              CupertinoActionSheetAction(
                onPressed: () {
                  setSettingString(name: 'theme', value: 'dark');
                  setState(() => theme = 'dark');
                  Navigator.pop(context);
                },
                child: const Text('Dark'),
              ),
              CupertinoActionSheetAction(
                onPressed: () {
                  setSettingString(name: 'theme', value: 'system');
                  setState(() => theme = 'system');
                  Navigator.pop(context);
                },
                child: const Text('System'),
              ),
            ],
            cancelButton: CupertinoActionSheetAction(
              onPressed: () {
                Navigator.pop(context);
              },
              child: const Text('Cancel'),
            ),
          ),
    );
  }

  static const String backupFailedMessage =
      "backup failed  (Folder may be protected. Try using the documents directory)";
  @override
  Widget build(BuildContext context) {
    return CupertinoPageScaffold(
      navigationBar: const CupertinoNavigationBar(middle: Text("Settings")),
      child: SafeArea(
        child: Padding(
          padding: const EdgeInsets.symmetric(horizontal: 20.0),
          child: ListView(
            children: [
              const SizedBox(height: 20),
              Column(
                children: [
                  const Text("Review"),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Show translations in preview"),
                      CupertinoSwitch(
                        value: translation,
                        activeTrackColor: CupertinoColors.activeBlue,
                        onChanged: (bool value) {
                          setSettingBool(
                            name: PreferenceConstants.showTranslations,
                            value: value,
                          );
                          setState(() => translation = value);
                        },
                      ),
                    ],
                  ),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Show pinyin by default"),
                      CupertinoSwitch(
                        value: reviewPinyin,
                        activeTrackColor: CupertinoColors.activeBlue,
                        onChanged: (bool value) {
                          setSettingBool(
                            name: PreferenceConstants.showPinyinByDefaultInReview,
                            value: value,
                          );
                          setState(() => reviewPinyin = value);
                        },
                      ),
                    ],
                  ),
                ],
              ),
              Column(
                children: [
                  const Text("Learn"),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Show example sentences in unit learn"),
                      CupertinoSwitch(
                        value: showExampleSentences,
                        activeTrackColor: CupertinoColors.activeBlue,
                        onChanged: (bool value) {
                          setSettingBool(
                            name: PreferenceConstants.showSentences,
                            value: value,
                          );
                          setState(() => showExampleSentences = value);
                        },
                      ),
                    ],
                  ),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Show literal meaning in unit learn"),
                      CupertinoSwitch(
                        value: showLiteralInUnitLearn,
                        activeTrackColor: CupertinoColors.activeBlue,
                        onChanged: (bool value) {
                          setSettingBool(
                            name: PreferenceConstants.showLiteralMeaningInUnitLearn,
                            value: value,
                          );
                          setState(() => showLiteralInUnitLearn = value);
                        },
                      ),
                    ],
                  ),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Default course"),
                      CupertinoButton(
                        onPressed: () {
                          _showDefaultCourseActionSheet(context);
                        },
                        child: Text(defaultCourse),
                      ),
                    ],
                  ),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Default home page"),
                      CupertinoButton(
                        onPressed: () {
                          _showDefaultHomePageActionSheet(context);
                        },
                        child: Text(defaultHomePage),
                      ),
                    ],
                  ),
                ],
              ),
              Column(
                children: [
                  const Text("Theme"),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Dark mode (requires app restart)"),
                      CupertinoButton(
                        onPressed: () {
                          _showThemeSelectionDialog(context);
                        },
                        child: Text(switch (theme) {
                          "dark" => "Dark",
                          "light" => "Light",
                          _ => "System",
                        }),
                      ),
                    ],
                  ),
                ],
              ),
              Column(
                children: [
                  const Text("Character View"),
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text("Download character stroke data (30MB)"),
                      Row(
                        children: [
                          CupertinoButton(
                            onPressed:
                                isDataDownloaded
                                    ? null
                                    : () async {
                                        setState(() {
                                          isDownloading = true;
                                        });
                                        CharacterStokesSql.createTable()
                                            .then(
                                              (value) {
                                                setSettingBool(
                                                  name:
                                                      'character_stroke_data_downloaded',
                                                  value: true,
                                                );
                                                setState(() {
                                                  isDataDownloaded = true;
                                                });
                                                showCupertinoDialog(
                                                  barrierDismissible: true,
                                                  context: context,
                                                  builder: (context) {
                                                    return const CupertinoAlertDialog(
                                                      content: Text(
                                                        "Download succeeded",
                                                      ),
                                                    );
                                                  },
                                                );
                                              },
                                              onError: (e) {
                                                showCupertinoDialog(
                                                  barrierDismissible: true,
                                                  context: context,
                                                  builder: (context) {
                                                    return const CupertinoAlertDialog(
                                                      content: Text(
                                                        "Download failed",
                                                      ),
                                                    );
                                                  },
                                                );
                                              },
                                            )
                                            .whenComplete(() {
                                              setState(() {
                                                isDownloading = false;
                                              });
                                            });
                                      },
                            child: const Text("Download"),
                          ),
                          if (isDownloading) const CupertinoActivityIndicator(),
                        ],
                      ),
                    ],
                  ),
                ],
              ),
              Visibility(
                visible: !PlatformInfo.isIOs(),
                child: Column(
                  children: [
                    const Text("Backup"),
                    Row(
                      children: [
                        const Text("Backup data"),
                        IconButton(
                          onPressed: () async {
                            Future<bool> updated =
                                Backup.startBackupWithFileSelection();
                            updated.then(
                              (val) => showCupertinoDialog(
                                barrierDismissible: true,
                                context: context,
                                builder: (context) {
                                  late final String text;
                                  val == true
                                      ? text = "backup succeeded"
                                      : text = backupFailedMessage;
                                  return CupertinoAlertDialog(
                                    content: Text(text),
                                  );
                                },
                              ),
                              onError:
                                  (e) => showCupertinoDialog(
                                    barrierDismissible: true,
                                    context: context,
                                    builder: (context) {
                                      return const CupertinoAlertDialog(
                                        content: Text(backupFailedMessage),
                                      );
                                    },
                                  ),
                            );
                          },
                          icon: const Icon(Icons.add),
                        ),
                      ],
                    ),
                    Row(
                      children: [
                        const Text("restore from backup"),
                        IconButton(
                          onPressed: () async {
                            bool updated =
                                await Backup.restoreBackupFromUserFile();
                            setState(() {});
                            showCupertinoDialog(
                              barrierDismissible: true,
                              context: context,
                              builder: (context) {
                                late final String text;
                                updated
                                    ? text = "backup succeeded"
                                    : text = backupFailedMessage;
                                return CupertinoAlertDialog(
                                  content: Text(text),
                                );
                              },
                            );
                          },
                          icon: const Icon(Icons.add),
                        ),
                      ],
                    ),
                  ],
                ),
              ),
              GestureDetector(
                onTap: () {
                  clicks++;
                  if (clicks == 5) {
                    setState(() {
                      showDebugOptions = true;
                    });
                  }
                },
                child: Row(
                  crossAxisAlignment: CrossAxisAlignment.end,
                  mainAxisAlignment: MainAxisAlignment.start,
                  children: [
                    const Text("Version "),
                    FutureBuilder<String>(
                      future: _getVersion(),
                      builder: (context, snapshot) {
                        if (snapshot.connectionState ==
                            ConnectionState.waiting) {
                          return const CupertinoActivityIndicator();
                        } else if (snapshot.hasError) {
                          return const Text("Error");
                        } else {
                          return Text(snapshot.data ?? 'Unknown');
                        }
                      },
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 10),
              Visibility(
                visible: showDebugOptions,
                child: Column(
                  children: [
                    const Text("debug options"),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        const Text("debug mode"),
                        CupertinoSwitch(
                          value: debug,
                          activeTrackColor: CupertinoColors.activeBlue,
                          onChanged: (bool value) {
                            setSettingBool(
                              name: PreferenceConstants.debug,
                              value: value,
                            );
                            setState(() => debug = value);
                          },
                        ),
                      ],
                    ),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        const Text("allow skip units"),
                        CupertinoSwitch(
                          value: allowSkipUnits,
                          activeTrackColor: CupertinoColors.activeBlue,
                          onChanged: (bool value) {
                            setSettingBool(
                              name: PreferenceConstants.allowSkipUnits,
                              value: value,
                            );
                            setState(() => allowSkipUnits = value);
                          },
                        ),
                      ],
                    ),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        const Text("check for new version on start"),
                        CupertinoSwitch(
                          value: checkVersionOnStart,
                          activeTrackColor: CupertinoColors.activeBlue,
                          onChanged: (bool value) {
                            setSettingBool(
                              name: PreferenceConstants.checkForNewVersionOnStart,
                              value: value,
                            );
                            setState(() => checkVersionOnStart = value);
                          },
                        ),
                      ],
                    ),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        const Text("allow auto complete unit"),
                        CupertinoSwitch(
                          value: allowAutoComplete,
                          activeTrackColor: CupertinoColors.activeBlue,
                          onChanged: (bool value) {
                            setSettingBool(
                              name: PreferenceConstants.allowAutoCompleteUnit,
                              value: value,
                            );
                            setState(() => allowAutoComplete = value);
                          },
                        ),
                      ],
                    ),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        const Text("Delete stroke data"),
                        Row(
                          children: [
                            CupertinoButton(
                              onPressed: () async {
                                setState(() {
                                  isDeleting = true;
                                });
                                CharacterStokesSql.dropTable()
                                    .then(
                                      (value) {
                                        setSettingBool(
                                          name:
                                              'character_stroke_data_downloaded',
                                          value: false,
                                        );
                                        setState(() {
                                          isDataDownloaded = false;
                                        });
                                        showCupertinoDialog(
                                          barrierDismissible: true,
                                          context: context,
                                          builder: (context) {
                                            return const CupertinoAlertDialog(
                                              content: Text(
                                                "Deletion succeeded",
                                              ),
                                            );
                                          },
                                        );
                                      },
                                      onError: (e) {
                                        showCupertinoDialog(
                                          barrierDismissible: true,
                                          context: context,
                                          builder: (context) {
                                            return const CupertinoAlertDialog(
                                              content: Text("Deletion failed"),
                                            );
                                          },
                                        );
                                      },
                                    )
                                    .whenComplete(() {
                                      setState(() {
                                        isDeleting = false;
                                      });
                                    });
                              },
                              child: const Text("Delete"),
                            ),
                            if (isDeleting) const CupertinoActivityIndicator(),
                          ],
                        ),
                      ],
                    ),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        const Text("Refresh DB (lose all data)"),
                        TextButton(
                            onPressed: isDeleting ? null : () async {
                              setState(() {
                                isDeleting = true;
                              });
                              await SQLHelper.refreshDB();
                              setState(() {
                                isDeleting = false;
                              });
                            },
                            child: const Text("Refresh"),
                        ),
                      ],
                    ),
                    Row(
                      mainAxisAlignment: MainAxisAlignment.spaceBetween,
                      children: [
                        CupertinoButton(
                          padding: EdgeInsets.zero,
                          onPressed: () async {
                            final map =
                                _preferencesService.getPreference(
                                  key: PreferenceConstants.courses,
                                );
                            print(map);
                            List settings = [];
                            if (map != null) {
                              settings.add([
                                PreferenceConstants.courses,
                                map,
                              ]);
                            }
                            showCupertinoDialog(
                              barrierDismissible: true,
                              context: context,
                              builder: (context) {
                                return Dialog(
                                  child: ListView.builder(
                                    itemCount: settings.length,
                                    itemBuilder: (
                                      BuildContext context,
                                      int index,
                                    ) {
                                      return Center(
                                        child: Text(
                                          "${settings[index][0]}: ${settings[index][1]}",
                                        ),
                                      );
                                    },
                                  ),
                                );
                              },
                            );
                          },
                          child: const Text("Show all settings"),
                        ),
                      ],
                    ),
                    Visibility(
                      visible: false,
                      child: Row(
                        children: [
                          const Text("Get latest data"),
                          IconButton(
                            onPressed: () async {
                              Future<bool> updated =
                                  PgUpdate.updateSqliteFromPg();
                              updated.then(
                                (val) => showCupertinoDialog(
                                  barrierDismissible: true,
                                  context: context,
                                  builder: (context) {
                                    return const CupertinoAlertDialog(
                                      content: Text("updated: true"),
                                    );
                                  },
                                ),
                                onError:
                                    (e) => showCupertinoDialog(
                                      barrierDismissible: true,
                                      context: context,
                                      builder: (context) {
                                        return const CupertinoAlertDialog(
                                          content: Text("updated: false"),
                                        );
                                      },
                                    ),
                              );
                            },
                            icon: const Icon(Icons.add),
                          ),
                        ],
                      ),
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 10),
            ],
          ),
        ),
      ),
    );
  }

  Future<String> _getVersion() async {
    final content = await rootBundle.loadString('pubspec.yaml');
    final pubspec = Pubspec.parse(content);
    final version = pubspec.version?.toString() ?? 'Unknown';
    return version.split('+').first;
  }
}

// only for testing, can be deleted
Map<String, dynamic> currSentence() {
  return {
    "characters": "我爱我的国家，它有很多美丽的河流和公园",
    "pinyin": "wǒ ài wǒ de guójiā, tā yǒu hěnduō měilì de héliú hé gōngyuán",
    "meaning":
        "I love my country, it has many beautiful rivers and parks and more words",
  };
}

void sentenceGameCallBack(
  bool value,
  Map<String, dynamic> currSentence,
  bool buildEnglish,
) {}

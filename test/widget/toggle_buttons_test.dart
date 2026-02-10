import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:hsk_learner/widgets/common/toggle_buttons.dart';

import '../../testing/app.dart';

void main() {
  group('ToggleButtonsWidget', () {
    // late ToggleButtonsWidgetViewModel viewModel;

    // setUp(() {
    //   viewModel = ToggleButtonsWidgetViewModel(
    //     firstRepository: FakeFirstRepository(),
    //   );
    // });

    loadWidget(WidgetTester tester, Widget widget) async {
      //await testApp(tester, ToggleButtonsWidget(viewModel: ToggleButtonsWidgetViewModel));
      await testApp(tester, widget);
    }

    testWidgets('renders correctly with initial state', (WidgetTester tester) async {
      // Arrange
      bool showPinyin = true;
      bool showTranslations = true;

      var widget = ToggleButtonsWidget(
        showPinyin: showPinyin,
        showTranslations: showTranslations,
        onPinyinToggle: (value) {},
        onTranslationToggle: (value) {},
      );

      await loadWidget(tester, widget);

      // Act

      // Assert
      expect(find.text('Hide translation'), findsOneWidget);
      expect(find.text('Hide Pinyin'), findsOneWidget);
    });

    testWidgets('toggles translation visibility', (WidgetTester tester) async {
      // Arrange
      bool showPinyin = true;
      bool showTranslations = true;
      bool translationToggled = false;

      var widget = ToggleButtonsWidget(
        showPinyin: showPinyin,
        onPinyinToggle: (value) {},
        showTranslations: showTranslations,
        onTranslationToggle: (value) {
          translationToggled = !translationToggled;
        },
      );

      await loadWidget(tester, widget);

      // Act
      await tester.tap(find.text('Hide translation'));
      await tester.pumpAndSettle();

      // Assert
      expect(translationToggled, true);
    });

    testWidgets('toggles pinyin visibility', (WidgetTester tester) async {
      // Arrange
      bool showPinyin = true;
      bool showTranslations = true;
      bool pinyinToggled = false;

      var widget = ToggleButtonsWidget(
              showPinyin: showPinyin,
              onPinyinToggle: (value) {
                pinyinToggled = true;
                showPinyin = !showPinyin;
              },
              showTranslations: showTranslations,
              onTranslationToggle: (value) {},
      );

      await loadWidget(tester, widget);

      // Act
      await tester.tap(find.text('Hide Pinyin'));
      await tester.pumpAndSettle();

      // Assert
      expect(pinyinToggled, true);
    });

    testWidgets('shows correct text when toggles are off', (WidgetTester tester) async {
      // Arrange
      bool showPinyin = false;
      bool showTranslations = false;

      await tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: ToggleButtonsWidget(
              showPinyin: showPinyin,
              showTranslations: showTranslations,
              onPinyinToggle: (value) {},
              onTranslationToggle: (value) {},
            ),
          ),
        ),
      );

      // Act

      // Assert
      expect(find.text('Show translation'), findsOneWidget);
      expect(find.text('Show Pinyin'), findsOneWidget);
    });

    testWidgets('renders correctly with only pinyin toggle', (WidgetTester tester) async {
      // Arrange
      bool showPinyin = true;

      var widget = ToggleButtonsWidget(
        showPinyin: showPinyin,
        onPinyinToggle: (value) {},
      );

      await loadWidget(tester, widget);

      // Act

      // Assert
      expect(find.text('Hide Pinyin'), findsOneWidget);
      expect(find.text('Hide translation'), findsNothing);
      expect(find.text('Show translation'), findsNothing);
    });

    testWidgets('renders correctly with only translation toggle', (WidgetTester tester) async {
      // Arrange
      bool showTranslations = true;

      var widget = ToggleButtonsWidget(
        showTranslations: showTranslations,
        onTranslationToggle: (value) {},
      );

      await loadWidget(tester, widget);

      // Act

      // Assert
      expect(find.text('Hide translation'), findsOneWidget);
      expect(find.text('Hide Pinyin'), findsNothing);
      expect(find.text('Show Pinyin'), findsNothing);
    });

    testWidgets('renders correctly with no toggles', (WidgetTester tester) async {
      // Arrange
      var widget = ToggleButtonsWidget();

      await loadWidget(tester, widget);

      // Act

      // Assert
      expect(find.text('Hide translation'), findsNothing);
      expect(find.text('Show translation'), findsNothing);
      expect(find.text('Hide Pinyin'), findsNothing);
      expect(find.text('Show Pinyin'), findsNothing);
    });
  });
}
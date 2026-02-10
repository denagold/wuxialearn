import 'package:flutter/material.dart';

class ToggleButtonsWidget extends StatelessWidget {
  final bool? _showPinyin;
  final bool? _showTranslations;
  final Function(bool)? onPinyinToggle;
  final Function(bool)? onTranslationToggle;

  const ToggleButtonsWidget({
    super.key,
    showPinyin,
    showTranslations,
    this.onPinyinToggle,
    this.onTranslationToggle,
  }) : _showTranslations = showTranslations, _showPinyin = showPinyin;

  @override
  Widget build(BuildContext context) {
    return Row(
      mainAxisAlignment: MainAxisAlignment.end,
      children: [
        if (onTranslationToggle != null && _showTranslations != null)
          TextButton(
            onPressed: () => onTranslationToggle!(!_showTranslations),
            child:  _showTranslations
                ? const Text("Hide translation")
                : const Text("Show translation"),
          ),
        if (onPinyinToggle != null && _showPinyin != null)
          TextButton(
            onPressed: () => onPinyinToggle!(!_showPinyin),
            child: _showPinyin
                ? const Text("Hide Pinyin")
                : const Text("Show Pinyin"),
          ),
      ],
    );
  }
}
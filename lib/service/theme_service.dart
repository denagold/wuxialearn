import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';

class ThemeConstants {
  // Primary colors
  static const Color primaryBlue = Colors.blue;
  static const Color primaryColor = primaryBlue;

  // Text colors
  static const Color textDark = Colors.black;
  static const Color textLight = Colors.white;
  static const Color textSecondary = Color(0xFF999EA3);

  // Background and surface colors
  static const Color backgroundLight = Colors.white;
  static const Color backgroundDark = Color(0xFF121212);

  // Border and divider colors
  static const Color dividerColor = Color(0xFFECECEC);
  static const Color borderColor = dividerColor;

  static const String dark = "dark";
  static const String light = "light";
  static const String system = "system";

  // Action colors
  static const Color actionColor = primaryBlue;

  // Get text color based on brightness
  static Color getTextColor(Brightness brightness) {
    return brightness == Brightness.dark ? textLight : textDark;
  }

  // Get background color based on brightness
  static Color getBackgroundColor(Brightness brightness) {
    return brightness == Brightness.dark ? backgroundDark : backgroundLight;
  }
}

abstract class ThemeServiceBase {
  Brightness getBrightness();
  void setBrightness(Brightness brightness);
  ThemeData getMaterialTheme();
  CupertinoThemeData getCupertinoTheme();
}


class ThemeService extends ThemeServiceBase {

  late Brightness _brightness;

  @override
  Brightness getBrightness() => _brightness;

  @override
  void setBrightness(Brightness brightness) => _brightness = brightness;

  // Material theme data
  @override
  ThemeData getMaterialTheme() {
    return ThemeData(
      brightness: _brightness,
      fontFamily: '.SF UI Text',
      colorScheme: ColorScheme.fromSwatch(
        brightness: _brightness,
        primarySwatch: Colors.blue,
      ),
    );
  }

  // Cupertino theme data
  @override
  CupertinoThemeData getCupertinoTheme() {
    return CupertinoThemeData(
      brightness: _brightness,
      primaryColor: ThemeConstants.primaryColor,
      textTheme: CupertinoTextThemeData(
        textStyle: TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.getTextColor(_brightness),
        ),
        actionTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.getTextColor(_brightness),
        ),
        navActionTextStyle: const TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.actionColor,
        ),
        navLargeTitleTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.getTextColor(_brightness),
        ),
        navTitleTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.getTextColor(_brightness),
        ),
        pickerTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.getTextColor(_brightness),
        ),
        dateTimePickerTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: ThemeConstants.getTextColor(_brightness),
        ),
      ),
    );
  }
}
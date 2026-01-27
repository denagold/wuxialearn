import 'package:flutter/cupertino.dart';
import 'package:flutter/material.dart';

class AppColors {
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

class AppTheme {
  // Material theme data
  static ThemeData getMaterialTheme(Brightness brightness) {
    return ThemeData(
      brightness: brightness,
      fontFamily: '.SF UI Text',
      colorScheme: ColorScheme.fromSwatch(
        brightness: brightness,
        primarySwatch: Colors.blue,
      ),
    );
  }

  // Cupertino theme data
  static CupertinoThemeData getCupertinoTheme(Brightness brightness) {
    return CupertinoThemeData(
      brightness: brightness,
      primaryColor: AppColors.primaryColor,
      textTheme: CupertinoTextThemeData(
        textStyle: TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.getTextColor(brightness),
        ),
        actionTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.getTextColor(brightness),
        ),
        navActionTextStyle: const TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.actionColor,
        ),
        navLargeTitleTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.getTextColor(brightness),
        ),
        navTitleTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.getTextColor(brightness),
        ),
        pickerTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.getTextColor(brightness),
        ),
        dateTimePickerTextStyle: TextStyle(
          fontFamily: 'Roboto',
          color: AppColors.getTextColor(brightness),
        ),
      ),
    );
  }
}
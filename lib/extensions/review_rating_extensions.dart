extension ReviewRatingInterval on Object {
  static String intervalValue(Duration duration) {
    if (duration.compareTo(const Duration(hours: 1)) < 0) {
      return duration.inMinutes.toString();
    } else if (duration.compareTo(const Duration(days: 1)) < 0) {
      return duration.inHours.toString();
    } else {
      return duration.inDays.toString();
    }
  }

  static String intervalUnit(Duration duration) {
    if (duration.compareTo(const Duration(hours: 1)) < 0) {
      return "min";
    } else if (duration.compareTo(const Duration(days: 1)) < 0) {
      return "hrs";
    } else {
      return "days";
    }
  }

  static String formatInterval(Duration start, Duration end) {
    String startUnit = intervalUnit(start);
    String startValue = intervalValue(start);
    String endUnit = intervalUnit(end);
    String endValue = intervalValue(end);

    if (start == end) {
      return "$startValue $startUnit";
    } else if (startUnit == endUnit) {
      return "$startValue - $endValue $startUnit";
    } else {
      return "$startValue $startUnit - $endValue $endUnit";
    }
  }
}

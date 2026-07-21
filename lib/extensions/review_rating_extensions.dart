extension DurationInterval on Duration {
  String intervalValue() {
    if (compareTo(const Duration(hours: 1)) < 0) {
      return inMinutes.toString();
    } else if (compareTo(const Duration(days: 1)) < 0) {
      return inHours.toString();
    } else {
      return inDays.toString();
    }
  }

  String intervalUnit() {
    if (compareTo(const Duration(hours: 1)) < 0) {
      return "min";
    } else if (compareTo(const Duration(days: 1)) < 0) {
      return "hrs";
    } else {
      return "days";
    }
  }
}

String intervalValue(Duration duration) {
  if (duration.compareTo(const Duration(hours: 1)) < 0) {
    return duration.inMinutes.toString();
  } else if (duration.compareTo(const Duration(days: 1)) < 0) {
    return duration.inHours.toString();
  } else {
    return duration.inDays.toString();
  }
}

String intervalUnit(Duration duration) {
  if (duration.compareTo(const Duration(hours: 1)) < 0) {
    return "min";
  } else if (duration.compareTo(const Duration(days: 1)) < 0) {
    return "hrs";
  } else {
    return "days";
  }
}

String formatInterval(Duration start, Duration end) {
  final startUnit = start.intervalUnit();
  final startValue = start.intervalValue();
  final endUnit = end.intervalUnit();
  final endValue = end.intervalValue();

  if (start == end) {
    return "$startValue $startUnit";
  } else if (startUnit == endUnit) {
    return "$startValue - $endValue $startUnit";
  } else {
    return "$startValue $startUnit - $endValue $endUnit";
  }
}

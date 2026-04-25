
import 'package:hsk_learner/service/audio_service.dart';

class FakeAudioService extends AudioServiceBase {
  @override
  Future<void> initTestPlay() {
    return Future.value();
  }

  @override
  Future<void> initialize() {
    return Future.value();
  }

  @override
  Future<void> playCorrectSound() {
    return Future.value();
  }

  @override
  Future<void> playWrongSound() {
    return Future.value();
  }

  @override
  Future<void> speak(String text) {
    return Future.value();
  }

}
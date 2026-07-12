import 'package:flutter_tts/flutter_tts.dart';
import 'package:just_audio/just_audio.dart';
import 'package:logging/logging.dart';


class AudioConstants {
  static const String correct = "assets/correct.wav";
  static const String wrong = "assets/wrong.wav";
}

abstract class AudioServiceBase {
  Future<void> initialize();
  Future<void> initTestPlay();
  Future<void> speak(String text);
  Future<void> playCorrectSound();
  Future<void> playWrongSound();
}

class AudioService implements AudioServiceBase {
  late final log = Logger('AudioService');
  static final AudioService _instance = AudioService._internal();
  factory AudioService() => _instance;
  AudioService._internal();

  final FlutterTts _flutterTts = FlutterTts();
  final AudioPlayer _audioPlayer = AudioPlayer();

  // Initialize audio service
  @override
  Future<void> initialize() async {
    try {
      await _flutterTts.setLanguage("zh-CN");
    }
    catch (e) {
      log.warning("Can't set TTS language");
    }
  }

  @override
  Future<void> initTestPlay() async {
    try {
      // This is because of an issue on android, the first play of the player is always cut off
      await _audioPlayer.setAsset(AudioConstants.correct);
      await _audioPlayer.load();
      final volume = _audioPlayer.volume;
      await _audioPlayer.setVolume(0.0);
      await _audioPlayer.play();
      await _audioPlayer.stop();
      await _audioPlayer.setVolume(volume);
    }
    catch (e) {
      log.warning("Can't initialize the Audio Player");
    }
  }

  // Text-to-speech methods
  @override
  Future<void> speak(String text) async {
    try {
      await _flutterTts.awaitSpeakCompletion(true);
      await _flutterTts.speak(text);
    }
    catch (e)
    {
      log.warning('Error in speak: $e');
    }
  }

  // Sound effect methods
  Future<void> playCorrectSound() async {
    await playSound(audioName: AudioConstants.correct);
    log.info('Correct sound played');
  }

  Future<void> playWrongSound() async {
    await playSound(audioName: AudioConstants.wrong);
    log.info('Wrong sound played');
  }

  Future<void> playSound({required String audioName}) async {
    try {
      await _audioPlayer.stop();
      await _audioPlayer.setAsset(audioName);
      await _audioPlayer.load();
      await _audioPlayer.play();
    } catch (e) {
      log.warning('Error playing correct sound: $e');
    }
  }

  // Cleanup
  void dispose() {
    _flutterTts.stop();
    _audioPlayer.dispose();
  }
}
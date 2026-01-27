import 'package:flutter_tts/flutter_tts.dart';
import 'package:just_audio/just_audio.dart';
import 'package:flutter/foundation.dart';

class AudioService {
  static final AudioService _instance = AudioService._internal();
  factory AudioService() => _instance;
  AudioService._internal();

  final FlutterTts _flutterTts = FlutterTts();
  final AudioPlayer _audioPlayer = AudioPlayer();

  // Initialize audio service
  Future<void> initialize() async {
    await _flutterTts.setLanguage("zh-CN");
  }

  Future<void> initTestPlay() async {
    // This is because of an issue on android, the first play of the player is always cut off
    await _audioPlayer.setAsset('assets/correct.wav');
    await _audioPlayer.load();
    final volume = _audioPlayer.volume;
    await _audioPlayer.setVolume(0.0);
    await _audioPlayer.play();
    await _audioPlayer.stop();
    await _audioPlayer.setVolume(volume);
  }

  // Text-to-speech methods
  Future<void> speak(String text) async {
    try {
      await _flutterTts.awaitSpeakCompletion(true);
      await _flutterTts.speak(text);
    }
    catch (e)
    {
      debugPrint('Error in speak: $e');
    }
  }

  // Sound effect methods
  Future<void> playCorrectSound() async {
    try {
      await _audioPlayer.setAsset('assets/correct.wav');
      await _audioPlayer.load();
      await _audioPlayer.play();
    } catch (e) {
      debugPrint('Error playing correct sound: $e');
    }
  }

  Future<void> playWrongSound() async {
    try {
      await _audioPlayer.setAsset('assets/wrong.wav');
      await _audioPlayer.load();
      await _audioPlayer.play();
    } catch (e) {
      debugPrint('Error playing wrong sound: $e');
    }
  }

  // Cleanup
  void dispose() {
    _flutterTts.stop();
    _audioPlayer.dispose();
  }
}
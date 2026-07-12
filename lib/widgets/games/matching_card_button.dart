import 'package:flutter/material.dart';
import 'package:hsk_learner/data_model/games/matching_card.dart';
import 'package:hsk_learner/services/theme_service.dart';
import 'package:provider/provider.dart';

// TODO Unify constants for all games
class MatchingCardButtonConstants {
  static const borderNotSelectedWidth = 1.0;
  static const borderSelectedWidth = 3.0;
  static const borderRadius = 10.0;
  static const marginSize = 5.0;

  static const fontSize = 14.0;

  static const cardHeight = 80.0;
  static const cardWidthPercentage = 0.4;

  static const selectColor = Color(0xFF0000FF);
  static const wrongColor = Color(0xFFFF0000);
  static const correctColor = Color(0xFF00FF00);
  static const backgroundColor = Color(0xFFB0B0B0);
  static const backgroundBlackColor = Color(0xFFB0B0B0);

  static final selectedBorderColor = Color(0xFF0B4FE3);
  static const backgroundBorderColor = Color(0xFF747272);
}

class MatchingCardButton extends StatelessWidget {
  final MatchingCard card;

  final bool isPinyinVisible;
  final bool isSelectedCard;

  final bool isWrongCard;
  final bool isCompletedCard;

  final double maxWidth;

  final Function(MatchingCard card, bool isPinyinVisible) onTap;

  const MatchingCardButton({
    super.key,
    required this.isPinyinVisible,
    required this.card,
    required this.isSelectedCard,
    required this.isCompletedCard,
    required this.isWrongCard,
    required this.maxWidth,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    late ThemeServiceBase themeService = context.read<ThemeServiceBase>();
    final style = themeService.getCupertinoTheme();


    return AnimatedContainer(
      duration: const Duration(milliseconds: 200),
      curve: Curves.easeIn,
      child: OutlinedButton(
        onPressed: () {
          onTap(card, isPinyinVisible);
        },
        style: OutlinedButton.styleFrom(
          textStyle: style.textTheme.textStyle,
          shape: RoundedRectangleBorder(
            borderRadius:
                BorderRadius.circular(MatchingCardButtonConstants.borderRadius),
          ),
          padding: EdgeInsets.symmetric(
            vertical: MatchingCardButtonConstants.marginSize,
          ),
          backgroundColor: isCompletedCard
              ? MatchingCardButtonConstants.correctColor : isWrongCard
              ? MatchingCardButtonConstants.wrongColor : isSelectedCard
              ? MatchingCardButtonConstants.selectColor : MatchingCardButtonConstants.backgroundColor,
          fixedSize: Size(maxWidth * MatchingCardButtonConstants.cardWidthPercentage, MatchingCardButtonConstants.cardHeight)
        ),
          // width: maxWidth * MatchingCardButtonConstants.cardWidthPercentage,
          // height: MatchingCardButtonConstants.cardHeight,
          // margin: const EdgeInsets.symmetric(
          //   vertical: MatchingCardButtonConstants.marginSize,
          // ),
          // decoration: BoxDecoration(
          //   color:
          //       isCompletedCard
          //           ? MatchingCardButtonConstants.correctColor
          //           : isWrongCard
          //           ? MatchingCardButtonConstants.wrongColor
          //           : isSelectedCard
          //           ? MatchingCardButtonConstants.selectColor
          //           : MatchingCardButtonConstants.backgroundBlackColor,
          //   borderRadius: BorderRadius.circular(
          //     MatchingCardButtonConstants.borderRadius,
          //   ),
          //   border: Border.all(
          //     color:
          //         isSelectedCard
          //             ? MatchingCardButtonConstants.selectedBorderColor
          //             : MatchingCardButtonConstants.backgroundBorderColor,
          //     width:
          //         isSelectedCard
          //             ? MatchingCardButtonConstants.borderSelectedWidth
          //             : MatchingCardButtonConstants.borderNotSelectedWidth,
          //   ),
          // ),
          child: Center(
            child: Column(
              mainAxisAlignment: MainAxisAlignment.center,
              children: [
                if (isPinyinVisible)
                  Text(
                    card.pinyin,
                    style: const TextStyle(
                      fontSize: MatchingCardButtonConstants.fontSize,
                    ),
                  ),
                Text(
                  card.text,
                  style: const TextStyle(
                    fontSize: MatchingCardButtonConstants.fontSize,
                  ),
                ),
              ],
            ),
          ),
        ),
    );
  }
}

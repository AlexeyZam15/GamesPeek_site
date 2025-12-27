# games/management/commands/analyzer/text_preparer.py
"""
Подготовка текста для анализа (полная совместимость со старой версией)
"""

from games.models import Game


class TextPreparer:
    """Подготавливает текст для анализа как в старой версии"""

    def __init__(self, command_instance):
        self.command = command_instance
        self.text_source_mode = self._resolve_text_source_priority()

    def _resolve_text_source_priority(self) -> str:
        """Разрешает приоритет опций источника текста (как в старой версии)"""
        if self.command.use_wiki:
            return 'use_wiki'
        elif self.command.use_rawg:
            return 'use_rawg'
        elif self.command.use_storyline:
            return 'use_storyline'
        elif self.command.prefer_wiki:
            return 'prefer_wiki'
        elif self.command.prefer_storyline:
            return 'prefer_storyline'
        elif self.command.combine_all_texts:
            return 'combine_all_texts'
        elif self.command.combine_texts:
            return 'combine_texts'
        else:
            return 'default'

    def prepare_text(self, game: Game) -> str:
        """Возвращает текст для анализа (как в старой версии)"""
        has_summary = bool(game.summary and game.summary.strip())
        has_storyline = bool(game.storyline and game.storyline.strip())
        has_rawg = bool(game.rawg_description and game.rawg_description.strip())
        has_wiki = bool(game.wiki_description and game.wiki_description.strip())

        if self.text_source_mode == 'use_wiki':
            return game.wiki_description if has_wiki else ""
        elif self.text_source_mode == 'use_rawg':
            return game.rawg_description if has_rawg else ""
        elif self.text_source_mode == 'prefer_wiki':
            if has_wiki:
                return game.wiki_description
            if has_rawg:
                return game.rawg_description
            if has_summary:
                return game.summary
            return game.storyline if has_storyline else ""
        elif self.text_source_mode == 'combine_all_texts':
            texts = []
            if has_summary:
                texts.append(game.summary)
            if has_storyline:
                texts.append(game.storyline)
            if has_rawg:
                texts.append(game.rawg_description)
            if has_wiki:
                texts.append(game.wiki_description)
            return " ".join(texts) if texts else ""
        elif self.text_source_mode == 'use_storyline':
            return game.storyline if has_storyline else (game.summary if has_summary else "")
        elif self.text_source_mode == 'prefer_storyline':
            if has_storyline:
                return game.storyline
            return game.summary if has_summary else ""
        elif self.text_source_mode == 'combine_texts':
            texts = []
            if has_summary:
                texts.append(game.summary)
            if has_storyline:
                texts.append(game.storyline)
            return " ".join(texts) if texts else ""
        else:
            if has_summary:
                return game.summary
            if has_storyline:
                return game.storyline
            if has_rawg:
                return game.rawg_description
            if has_wiki:
                return game.wiki_description
            return ""

    def get_source_description(self) -> str:
        """Возвращает описание источника текста (как в старой версии)"""
        descriptions = {
            'use_wiki': "ТОЛЬКО Wikipedia описание",
            'use_rawg': "ТОЛЬКО описание RAWG",
            'use_storyline': "ТОЛЬКО сторилайн",
            'prefer_wiki': "ПРЕДПОЧТИТЕЛЬНО Wikipedia",
            'prefer_storyline': "ПРЕДПОЧТИТЕЛЬНО сторилайн",
            'combine_all_texts': "ОБЪЕДИНЕННЫЙ ВЕСЬ текст",
            'combine_texts': "ОБЪЕДИНЕННЫЙ текст (IGDB)",
            'default': "ПРЕДПОЧТИТЕЛЬНО описание IGDB"
        }
        return descriptions.get(self.text_source_mode, "Неизвестно")

    def get_text_source_for_game(self, game: Game, text_to_analyze: str) -> str:
        """Определяет источник текста для отладочной информации"""
        if self.text_source_mode == 'combine_all_texts':
            return "объединенный весь текст"
        elif self.text_source_mode == 'combine_texts':
            return "объединенный текст IGDB"
        elif text_to_analyze == game.wiki_description:
            return "Wikipedia описание"
        elif text_to_analyze == game.storyline:
            return "сторилайн"
        elif text_to_analyze == game.summary:
            return "описание IGDB"
        elif text_to_analyze == game.rawg_description:
            return "описание RAWG"
        else:
            return "неизвестный источник"
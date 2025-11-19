import re
from typing import List, Dict, Tuple
from games.models import Keyword, KeywordCategory


class KeywordClassifier:
    """
    Автоматически классифицирует ключевые слова на Gameplay и Setting
    на основе ключевых терминов и паттернов
    """

    # Словари для геймплейных ключевых слов
    GAMEPLAY_TERMS = {
        # Механики и действия
        'action', 'combat', 'fighting', 'shooter', 'fps', 'tps', 'battle', 'warfare',
        'strategy', 'tactical', 'puzzle', 'platformer', 'stealth', 'survival',
        'exploration', 'crafting', 'building', 'farming', 'trading', 'economy',
        'racing', 'driving', 'sports', 'fishing', 'hunting', 'cooking',

        # Системы и механики
        'rpg', 'role-playing', 'leveling', 'skills', 'inventory', 'quest',
        'dialogue', 'choices', 'branching', 'procedural', 'roguelike',
        'permadeath', 'multiplayer', 'co-op', 'pvp', 'competitive', 'casual',

        # Управление и перспектива
        'first-person', 'third-person', 'top-down', 'side-scroller', '2d', '3d',
        'real-time', 'turn-based', 'point-click', 'controller', 'vr',

        # Игровые процессы
        'minigames', 'quick-time', 'tower-defense', 'bullet-hell', 'match-3',
        'endless', 'score-attack', 'speedrun', 'achievements', 'collectathon'
    }

    # Словари для ключевых слов сеттинга
    SETTING_TERMS = {
        # Время и эпохи
        'medieval', 'fantasy', 'sci-fi', 'cyberpunk', 'steampunk', 'dieselpunk',
        'historical', 'prehistoric', 'stone-age', 'victorian', 'renaissance',
        'modern', 'futuristic', 'post-apocalyptic', 'dystopian',

        # Места и локации
        'space', 'alien', 'planet', 'galaxy', 'underwater', 'ocean', 'pirate',
        'western', 'wild-west', 'urban', 'city', 'rural', 'village', 'forest',
        'jungle', 'desert', 'arctic', 'snow', 'mountain', 'cave', 'dungeon',

        # Атмосфера и темы
        'horror', 'survival-horror', 'psychological', 'lovecraftian', 'zombie',
        'noir', 'mystery', 'detective', 'crime', 'gangster', 'war', 'military',
        'espionage', 'spy', 'superhero', 'magic', 'mythology', 'fairy-tale',

        # Визуальный стиль
        'retro', 'pixel', 'low-poly', 'cel-shaded', 'anime', 'cartoon',
        'realistic', 'stylized', 'minimalist', 'dark', 'gothic'
    }

    # Слова-исключения и двусмысленные термины
    AMBIGUOUS_TERMS = {
        'adventure', 'story', 'narrative', 'open-world', 'sandbox', 'simulation'
    }

    def classify_keyword(self, keyword_name: str) -> Tuple[str, float]:
        """
        Классифицирует ключевое слово и возвращает (категория, уверенность)
        """
        keyword_lower = keyword_name.lower()

        # Подсчитываем совпадения для каждой категории
        gameplay_score = self._calculate_score(keyword_lower, self.GAMEPLAY_TERMS)
        setting_score = self._calculate_score(keyword_lower, self.SETTING_TERMS)

        # Определяем категорию на основе скоринга
        if gameplay_score > setting_score:
            confidence = gameplay_score / (gameplay_score + setting_score) if (
                                                                                          gameplay_score + setting_score) > 0 else 0.5
            return 'Gameplay', confidence
        elif setting_score > gameplay_score:
            confidence = setting_score / (gameplay_score + setting_score) if (
                                                                                         gameplay_score + setting_score) > 0 else 0.5
            return 'Setting', confidence
        else:
            # Если скоринги равны или оба нулевые
            return 'Miscellaneous', 0.5

    def _calculate_score(self, keyword: str, term_set: set) -> float:
        """Вычисляет score для категории на основе совпадений"""
        score = 0.0

        # Проверяем полное совпадение
        if keyword in term_set:
            score += 2.0

        # Проверяем частичные совпадения (слова в составе)
        words = re.findall(r'\w+', keyword)
        for word in words:
            if word in term_set:
                score += 1.0
            # Проверяем похожие слова (префиксы/суффиксы)
            for term in term_set:
                if word in term or term in word:
                    score += 0.5

        return score

    def classify_all_keywords(self, min_confidence: float = 0.6) -> Dict:
        """
        Классифицирует все ключевые слова в базе данных
        Возвращает статистику
        """
        keywords = Keyword.objects.all()
        stats = {
            'total': keywords.count(),
            'classified': 0,
            'gameplay': 0,
            'setting': 0,
            'miscellaneous': 0,
            'low_confidence': 0
        }

        # Получаем или создаем категории
        gameplay_category, _ = KeywordCategory.objects.get_or_create(
            name='Gameplay',
            defaults={'description': 'Keywords related to game mechanics and gameplay features'}
        )
        setting_category, _ = KeywordCategory.objects.get_or_create(
            name='Setting',
            defaults={'description': 'Keywords related to game world, environment and location'}
        )
        misc_category, _ = KeywordCategory.objects.get_or_create(
            name='Miscellaneous',
            defaults={'description': 'Unclassified or ambiguous keywords'}
        )

        for keyword in keywords:
            category_name, confidence = self.classify_keyword(keyword.name)

            if confidence >= min_confidence:
                if category_name == 'Gameplay':
                    keyword.category = gameplay_category
                    stats['gameplay'] += 1
                elif category_name == 'Setting':
                    keyword.category = setting_category
                    stats['setting'] += 1
                else:
                    keyword.category = misc_category
                    stats['miscellaneous'] += 1

                keyword.save()
                stats['classified'] += 1
            else:
                stats['low_confidence'] += 1

        return stats
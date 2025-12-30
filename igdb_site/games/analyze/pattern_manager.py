# games/analyzer/pattern_manager.py
import re
import hashlib
import json
from typing import Dict, List
from django.core.cache import cache


class PatternManager:
    """Менеджер для работы с паттернами с кешированием и отслеживанием изменений"""

    # Ключи для кэша
    CACHE_KEY_PATTERNS = 'pattern_manager:all_patterns'
    CACHE_KEY_HASH = 'pattern_manager:patterns_hash'
    CACHE_TIMEOUT = 86400  # 24 часа

    # Компилированные паттерны для производительности
    _compiled_patterns = None
    _patterns_hash = None

    @classmethod
    def get_all_patterns(cls) -> Dict[str, Dict[str, List[re.Pattern]]]:
        """Возвращает ВСЕ скомпилированные паттерны с кэшированием"""
        # Пробуем получить из памяти
        if cls._compiled_patterns is not None:
            return cls._compiled_patterns

        # Пробуем получить из кэша
        cached_patterns = cache.get(cls.CACHE_KEY_PATTERNS)
        cached_hash = cache.get(cls.CACHE_KEY_HASH)

        # Рассчитываем текущий хеш паттернов
        current_hash = cls._calculate_patterns_hash()

        # Если есть кэш и хеш совпадает, используем кэш
        if cached_patterns and cached_hash == current_hash:
            cls._compiled_patterns = cached_patterns
            cls._patterns_hash = current_hash
            return cls._compiled_patterns

        # Иначе компилируем заново и кэшируем
        cls._compiled_patterns = {
            'genres': cls._compile_patterns_dict(cls.GENRE_PATTERNS),
            'themes': cls._compile_patterns_dict(cls.THEME_PATTERNS),
            'perspectives': cls._compile_patterns_dict(cls.PERSPECTIVE_PATTERNS),
            'game_modes': cls._compile_patterns_dict(cls.MODE_PATTERNS),
        }

        # Сохраняем в кэш
        cls._patterns_hash = current_hash
        cache.set(cls.CACHE_KEY_PATTERNS, cls._compiled_patterns, cls.CACHE_TIMEOUT)
        cache.set(cls.CACHE_KEY_HASH, current_hash, cls.CACHE_TIMEOUT)

        return cls._compiled_patterns

    @classmethod
    def _calculate_patterns_hash(cls) -> str:
        """Рассчитывает хеш всех паттернов для отслеживания изменений"""
        # Собираем все паттерны в один словарь
        all_patterns = {
            'genres': cls.GENRE_PATTERNS,
            'themes': cls.THEME_PATTERNS,
            'perspectives': cls.PERSPECTIVE_PATTERNS,
            'game_modes': cls.MODE_PATTERNS,
        }

        # Преобразуем в JSON строку и вычисляем хеш
        patterns_json = json.dumps(all_patterns, sort_keys=True)
        return hashlib.md5(patterns_json.encode('utf-8')).hexdigest()

    @classmethod
    def check_patterns_changed(cls) -> bool:
        """Проверяет, изменились ли паттерны с момента последнего кэширования"""
        # Получаем сохраненный хеш из кэша
        cached_hash = cache.get(cls.CACHE_KEY_HASH)

        # Если нет сохраненного хеша, значит паттерны изменились
        if cached_hash is None:
            return True

        # Рассчитываем текущий хеш
        current_hash = cls._calculate_patterns_hash()

        # Сравниваем хеши
        return cached_hash != current_hash

    @classmethod
    def clear_cache(cls):
        """Очищает кэш паттернов"""
        cls._compiled_patterns = None
        cls._patterns_hash = None
        cache.delete(cls.CACHE_KEY_PATTERNS)
        cache.delete(cls.CACHE_KEY_HASH)

    @staticmethod
    def _compile_patterns_dict(patterns_dict: Dict[str, List[str]]) -> Dict[str, List[re.Pattern]]:
        """Компилирует словарь паттернов"""
        compiled = {}
        for name, patterns in patterns_dict.items():
            compiled_patterns = []
            for pattern_str in patterns:
                try:
                    compiled_patterns.append(re.compile(pattern_str, re.IGNORECASE | re.UNICODE))
                except re.error as e:
                    print(f"⚠️ Ошибка компиляции паттерна '{pattern_str}': {e}")
            compiled[name] = compiled_patterns
        return compiled

    # Паттерны вынесены в константы для легкого редактирования
    GENRE_PATTERNS = {
        'Adventure': [
            r'\badventure(\s+game|\s+title|\s+quest)\b',
            r'\bexploration(\s+based|\s+game|\s+focused)\b',
        ],
        'Arcade': [
            r'\barcade(\s+game|\s+style|\s+classic)\b',
            r'\barcade-style\b',
        ],
        'Card & Board Game': [
            r'\bis\s+a\s+(board|card)\s+game\b',
            r'\bas\s+a\s+(board|card)\s+game\b',
            r'\bthis\s+(board|card)\s+game\b',
            r'\bdigital\s+(board|card)\s+game\b',
            r'\belectronic\s+(board|card)\s+game\b',
            r'\bvideo\s+(board|card)\s+game\b',
            r'\b(board|card)\s+game\s+(simulation|simulator|adaptation)\b',
            r'\b(board|card)\s+based\s+video\s+game\b',
            r'\bvideo\s+game\s+adaptation\s+of\s+a\s+(board|card)\s+game\b',
            r'\bplay\s+as\s+a\s+(board|card)\s+game\b',
            r'\bgameplay\s+resembles\s+a\s+(board|card)\s+game\b',
            r'\bmechanics\s+of\s+a\s+(board|card)\s+game\b',
            r'\bstyled\s+after\s+a\s+(board|card)\s+game\b',
            r'\binspired\s+by\s+(board|card)\s+game\b',
        ],
        'Fighting': [
            r'\bfighting(\s+game|\s+title)\b',
            r'\b(this|the)\s+fighting(\s+game|\s+title)\b',
        ],
        'Hack and slash/Beat \'em up': [
            r'\bhack\s+and\s+slash\b',
            r'\bbeat\s+\'?em\s+up\b',
        ],
        'Indie': [
            r'\bindie(\s+game|\s+title|\s+developer)\b',
            r'\bindependent(\s+developer|\s+studio)\b',
        ],
        'MOBA': [
            r'\bmoba\b',
            r'\bmultiplayer\s+online\s+battle\s+arena\b',
        ],
        'Music': [
            r'\bmusic(\s+game|\s+rhythm)\b',
            r'\brhythm(\s+game|\s+action)\b',
        ],
        'Pinball': [
            r'\bpinball\b',
            r'\bpin\s+ball\b',
        ],
        'Platform': [
            r'\bplatform(\s+game|\s+title)\b',
            r'\bplatformer\b',
        ],
        'Point-and-click': [
            r'\bpoint\s+and\s+click\b',
            r'\bpoint-and-click\b',
        ],
        'Puzzle': [
            r'\bpuzzle(\s+game|\s+title)\b',
            r'\bbrain\s+teaser\b',
        ],
        'Quiz/Trivia': [
            r'\bquiz(\s+game)?\b',
            r'\btrivia(\s+game)?\b',
        ],
        'Racing': [
            r'\bracing(\s+game|\s+simulator|\s+title)\b',
            r'\bdriving(\s+game|\s+simulator|\s+experience)\b',
        ],
        'Real Time Strategy (RTS)': [
            r'\breal\s+time\s+strategy\b',
            r'\brts(\s+game|\s+title)?\b',
        ],
        'Role-playing (RPG)': [
            r'\brole-playing(\s+game|\s+title)\b',
            r'\brpg\b',
        ],
        'Shooter': [
            r'\bshooter(\s+game|\s+title)\b',
            r'\bfps(\s+game|\s+title)\b',
        ],
        'Simulator': [
            r'\bsimulator(\s+game|\s+title)\b',
            r'\bsimulation(\s+game|\s+title)\b',
        ],
        'Sport': [
            r'\bsports(\s+game|\s+title)\b',
            r'\bfootball(\s+game|\s+simulator)\b',
        ],
        'Strategy': [
            r'\bstrategy(\s+game|\s+title|\s+rpg)\b',
            r'\bstrategic(\s+game|\s+thinking)\b',
        ],
        'Tactical': [
            r'\btactical(\s+game|\s+decisions|\s+rpg)\b',
            r'\btactics(\s+game)?\b',
            r'\bturn-based\s+tactics\b',
        ],
        'Turn-based strategy (TBS)': [
            r'\bturn-based\s+strategy\b',
            r'\bturn-based\s+game\b',
            r'\btbs\s+game\b',
            r'\bstrategy\s+turn-based\b',
        ],
        'Visual Novel': [
            r'\bvisual\s+novel\b',
            r'\bvn(\s+game)?\b',
        ]
    }

    THEME_PATTERNS = {
        # Найдите тему 'Crafting & Gathering' и измените паттерн:
        'Crafting & Gathering': [
            r'\bcrafting\s+system\b',
            # ИСПРАВЛЕННЫЙ ПАТТЕРН: добавлен (s|ing)?
            r'\bgather(s|ing)?\s+(resources|materials|supplies)\b',
            # Было: r'\bgather(ing)?\s+(resources|materials|supplies)\b'
            r'\bcollect(ing)?\s+(resources|materials|items)\b',
            r'\bresource\s+gathering\b',
            r'\bmaterial\s+collection\b',
            r'\bitem\s+crafting\b',
            r'\bharvest(ing)?\s+(resources|materials)\b',
            r'\bforag(ing|e)\s+(for|)\b',
            r'\bmining\s+(for|)\s+resources\b',
            r'\bwoodcutting\b',
            r'\bfishing\s+(for|)\s+materials\b',
            r'\bscaveng(ing|e)\s+(for|)\s+supplies\b',
        ],
        'Base Building': [
            r'\bbase\s+building\b',
            r'\bbase\s+construction\b',
            r'\bbuilding\s+bases\b',
            r'\bconstructing\s+bases\b',
            r'\bplayers?\s+can\s+build\b',
            r'\byou\s+can\s+build\b',
            r'\bable\s+to\s+build\b',
            r'\bbuild(ing)?\s+(structures|bases|homes)\b',
            r'\bconstruct(ing)?\s+(buildings|structures)\b',
        ],
        '4X (explore, expand, exploit, and exterminate)': [
            r'\b4x(\s+game|\s|$)',
            r'\bexplore.*expand.*exploit.*exterminate',
        ],
        'Action': [
            r'\baction[-\s]?packed\b',
            r'\bintense\s+action\b',
            r'\bnon[-\s]?stop\s+action\b',
            r'\bfast[-\s]?paced\s+action\b',
            r'\bheart[-\s]?pounding\s+action\b',
            r'\baction[-\s]?oriented\b',
            r'\baction[-\s]?driven\b',
            r'\bhigh[-\s]?octane\s+action\b',
            r'\bexplosive\s+action\b',
            r'\baction[-\s]?heavy\b',
            r'\baction[-\s]?focused\b',
            r'\baction[-\s]?centered\b',
            r'\baction[-\s]?based\b',
            r'\baction[-\s]?filled\b',
            r'\baction[-\s]?laden\b',
            r'\baction[-\s]?intensive\b',
            r'\baction[-\s]?rich\b',
            r'\baction[-\s]?saturated\b',
        ],
        'Business': [
            r'\bbusiness\s+simulation\b',
            r'\bbusiness\s+game\b',
            r'\bcorporation\s+management\b',
            r'\bmanagement\s+simulation\b',
            r'\bentrepreneurship\s+simulation\b',
        ],
        'Comedy': [
            r'\bcomedy\s+game\b',
            r'\bhumor\s+game\b',
            r'\bfunny\s+game\b',
            r'\bcomic\s+game\b',
            r'\blighthearted\s+comedy\b',
        ],
        'Drama': [
            r'\bdrama(\s+game)?\b',
            r'\bdramatic(\s+story)?\b',
        ],
        'Educational': [
            r'\beducational\s+game\b',
            r'\beducational\s+title\b',
            r'\blearning\s+game\b',
            r'\bedutainment\b',
            r'\beducational\s+software\b',
        ],
        'Erotic': [
            r'\berotic(\s+content|\s+themes|\s+elements)\b',
            r'\bsexual(\s+content|\s+themes|\s+situations)\b',
            r'\bmature(\s+content|\s+themes)\b',
            r'\bexplicit(\s+content|\s+scenes)\b',
            r'\badult(\s+content|\s+themes)\b',
            r'\bnsfw\b',
        ],
        'Fantasy': [
            r'\bfantasy(\s+world|\s+setting|\s+game|\s|$)',
            r'\bmagical(\s+world|\s|$)',
        ],
        'Historical': [
            r'\bhistorical\s+(drama|fiction|epic|recreation)\b',
            r'\bbased\s+on\s+(real|actual)\s+historical\s+events\b',
            r'\bperiod-accurate\s+setting\b',
            r'\bhistorical\s+reconstruction\b',
            r'\bgame\s+set\s+in\s+(a\s+)?historical\s+(period|era)\b',
            r'\bexplore\s+historical\s+(periods|events)\b',
            r'\bexperience\s+life\s+in\s+(the\s+)?\d+(th|st|nd|rd)\s+century\b',
            r'\bhistorical\s+simulation\b',
            r'\bauthentic\s+historical\s+setting\b',
            r'\bworld\s+war\s+(i|ii|1|2)\b',
        ],
        'Horror': [
            r'\bhorror(\s+game|\s+title)\b',
            r'\bsurvival\s+horror\b',
        ],
        'Kids': [
            r'\bfor\s+kids\b',
            r'\bchildren\s+as\s+main\s+characters\b',
            r'\bkids\s+as\s+protagonists\b',
            r'\bchild\s+hero\b',
            r'\bchildren\'?s\s+adventure\b',
            r'\bgame\s+for\s+children\b',
            r'\byoung\s+protagonists\b',
            r'\bchildhood\s+adventure\b',
        ],
        'Mystery': [
            r'\bmystery(\s+game)?\b',
            r'\bdetective(\s+story)?\b',
        ],
        'Non-fiction': [
            r'\bnon-fiction\b',
            r'\bnonfiction\b',
            r'\breal(\s+world|\s+life)\b',
        ],
        'Open world': [
            r'\bopen[-\s]?world(\s+game|\s+title|\s+experience|\s+environment|\s+setting)\b',
            r'\bopen[-\s]?world(\s+adventure|\s+exploration|\s+rpg|\s+action)?\b',
            r'\bopen[-\s]?world\s+(sandbox|gameplay|mechanics)\b',
        ],
        'Party': [
            r'\bparty\s+(game|games|title|mode)\b',
            r'\bparty-style\s+(game|gaming)\b',
            r'\bmultiplayer\s+party\b',
            r'\bcasual\s+party\b',
            r'\bfun\s+party\b',
        ],
        'Romance': [
            r'\bromance(\s+game)?\b',
            r'\bromantic(\s+story)?\b',
            r'\bdating\s+sim\b',
            r'\blove(\s+story|\s+game)\b',
        ],
        'Sandbox': [
            r'\bsandbox(\s+game)?\b',
            r'\bsandbox-style\b',
        ],
        'Science fiction': [
            r'\bscience\s+fiction(\s+game|\s|$)',
            r'\bsci-fi(\s+game|\s|$)',
        ],
        'Stealth': [
            r'\bstealth(\s+game)?\b',
            r'\bsneaking(\s+game)?\b',
        ],
        'Survival': [
            # Основные ключевые слова
            r'\bsurvival(\s+(game|title|experience|horror|elements|mechanics|based|focused|oriented))?\b',

            # Глагольные формы
            r'\b(to\s+)?survive(\s+(the|in|against))?\b',
            r'\bsurviving\s+(in|against|the)\b',

            # Конкретные типы survival
            r'\bpost-apocalyptic\s+survival\b',
            r'\bwilderness\s+survival\b',
            r'\bsurvival\s+horror\b',

            # Описательные фразы
            r'\bstruggle\s+to\s+survive\b',
            r'\bfight\s+for\s+survival\b',
            r'\bbattle\s+for\s+survival\b',
            r'\bsurvival\s+of\s+the\s+fittest\b',
        ],
        'Thriller': [
            r'\bpsychological\s+thriller\b',
            r'\btechno[-\s]?thriller\b',
            r'\bpolitical\s+thriller\b',
            r'\bspy\s+thriller\b',
            r'\blegal\s+thriller\b',
            r'\bmedical\s+thriller\b',
            r'\bmilitary\s+thriller\b',
            r'\bcrime\s+thriller\b',
            r'\bconspiracy\s+thriller\b',
            r'\bheart[-\s]?pounding\s+suspense\b',
            r'\bedge[-\s]?of[-\s]?your[-\s]?seat\s+(suspense|thriller)\b',
            r'\btense\s+thriller\b',
            r'\bintense\s+thriller\b',
            r'\bgripping\s+thriller\b',
            r'\brelentless\s+suspense\b',
            r'\bcat[-\s]?and[-\s]?mouse\s+(game|chase)\b',
            r'\bmind\s+games?\b',
            r'\bpsychological\s+mind\s+games?\b',
            r'\btense\s+standoff\b',
            r'\bdeadly\s+game\s+of\s+(cat|wits)\b',
            r'\bin\s+(the\s+)?style\s+of\s+a\s+thriller\b',
            r'\bas\s+a\s+thriller\b',
            r'\bthis\s+thriller\b',
            r'\bthe\s+thriller\s+(elements|aspects)\b',
            r'\b(thriller|suspense)(?:\s+(game|title|novel|film|movie|story|tale|narrative|plot))?\b',
        ],
        'Warfare': [
            r'\bwarfare\s+simulation\b',
            r'\bwar\s+game\b',
            r'\bmilitary\s+conflict\b',
            r'\bcombat\s+simulation\b',
            r'\btactical\s+warfare\b',
        ],
    }

    PERSPECTIVE_PATTERNS = {
        'Auditory': [
            r'\bauditory\b',
            r'\bsound-based\b',
            r'\baudio-focused\b',
        ],
        'Bird view / Isometric': [
            r'\bbird\s*view',
            r'\bisometric(\s+view|\s+game|\s|$)',
            r'\btop-down(\s+view|\s+game|\s|$)'
        ],
        'First person': [
            r'\bfirst-person(\s+view|\s+shooter|\s+game|\s|$)',
            r'\bfpp(\s+game|\s|$)'
        ],
        'Side view': [
            r'\bside\s+view\b',
            r'\bside-view\b',
            r'\bside\s+scroller\b',
        ],
        'Text': [
            r'\btext-based\b',
            r'\btext\s+adventure\b',
        ],
        'Third person': [
            r'\bthird-person(\s+view|\s+game|\s|$)',
            r'\btpp(\s+game|\s|$)'
        ],
        'Virtual Reality': [
            r'\bvirtual\s+reality\s+game\b',
            r'\bvirtual\s+reality\s+experience\b',
            r'\bvr\s+game\b',
            r'\bvr\s+experience\b',
            r'\bimmersive\s+virtual\s+reality\b',
        ],
    }

    MODE_PATTERNS = {
        'Battle Royale': [
            r'\bbattle\s+royale\b',
            r'\broyale(\s+mode)?\b',
        ],
        'Co-operative': [
            r'\bco-operative(\s+game|\s+mode|\s|$)',
            r'\bcooperative(\s+game|\s|$)',
            r'\bco-op(\s+game|\s|$)'
        ],
        'Massively Multiplayer Online (MMO)': [
            r'\bmassively\s+multiplayer(\s+game|\s|$)',
            r'\bmmo(\s+game|\s|$)',
        ],
        'Multiplayer': [
            r'\bmultiplayer(\s+game|\s+mode|\s|$)',
            r'\bmulti-player(\s+game|\s|$)'
        ],
        'Single player': [
            r'\bsingle-player(\s+game|\s+campaign|\s|$)',
            r'\bsingle\s+player(\s+game|\s|$)'
        ],
        'Split screen': [
            r'\bsplit\s+screen\b',
            r'\bsplit-screen\b',
        ],
    }

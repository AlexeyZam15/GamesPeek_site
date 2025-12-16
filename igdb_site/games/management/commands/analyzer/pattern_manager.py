# games/analyzer/pattern_manager.py
import re
from typing import Dict, List


class PatternManager:
    """Менеджер для работы с паттернами с кешированием и оптимизацией"""

    # Компилированные паттерны для производительности
    _compiled_patterns = None

    @classmethod
    def get_compiled_patterns(cls) -> Dict[str, Dict[str, List[re.Pattern]]]:
        """Возвращает скомпилированные паттерны с кешированием"""
        if cls._compiled_patterns is not None:
            return cls._compiled_patterns

        cls._compiled_patterns = {
            'genres': cls._compile_patterns(cls.GENRE_PATTERNS),
            'themes': cls._compile_patterns(cls.THEME_PATTERNS),
            'perspectives': cls._compile_patterns(cls.PERSPECTIVE_PATTERNS),
            'game_modes': cls._compile_patterns(cls.MODE_PATTERNS),
        }
        return cls._compiled_patterns

    @staticmethod
    def _compile_patterns(patterns_dict: Dict[str, List[str]]) -> Dict[str, List[re.Pattern]]:
        """Компилирует все регулярные выражения"""
        compiled = {}
        for name, patterns in patterns_dict.items():
            compiled_patterns = []
            for pattern in patterns:
                try:
                    compiled_patterns.append(re.compile(pattern, re.IGNORECASE))
                except re.error as e:
                    print(f"⚠️ Ошибка компиляции паттерна '{pattern}': {e}")
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
            # Удалить или изменить слишком простой паттерн:
            # r'\bboard\s+game\b',  # ПРОБЛЕМНЫЙ ПАТТЕРН - УДАЛИТЬ

            # Добавить более специфичные и контекстные паттерны:

            # 1. Паттерны, которые указывают на тип игры
            r'\bis\s+a\s+(board|card)\s+game\b',
            r'\bas\s+a\s+(board|card)\s+game\b',
            r'\bthis\s+(board|card)\s+game\b',
            r'\bdigital\s+(board|card)\s+game\b',
            r'\belectronic\s+(board|card)\s+game\b',
            r'\bvideo\s+(board|card)\s+game\b',
            r'\b(board|card)\s+game\s+(simulation|simulator|adaptation)\b',
            r'\b(board|card)\s+based\s+video\s+game\b',
            r'\bvideo\s+game\s+adaptation\s+of\s+a\s+(board|card)\s+game\b',

            # 2. Паттерны, описывающие геймплей
            r'\bplay\s+as\s+a\s+(board|card)\s+game\b',
            r'\bgameplay\s+resembles\s+a\s+(board|card)\s+game\b',
            r'\bmechanics\s+of\s+a\s+(board|card)\s+game\b',
            r'\bstyled\s+after\s+a\s+(board|card)\s+game\b',
            r'\binspired\s+by\s+(board|card)\s+game\b',

            # 3. Исключить упоминания исходного материала
            # Паттерны, которые НЕ должны добавлять этот жанр
            r'\bbased\s+on\s+(the\s+)?(\w+\s+)?(board|card)\s+game\b',  # Исключение!
            r'\badapted\s+from\s+a\s+(board|card)\s+game\b',  # Исключение!
            r'\boriginally\s+a\s+(board|card)\s+game\b',  # Исключение!
            r'\bsource\s+material\s+is\s+a\s+(board|card)\s+game\b',  # Исключение!
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
        '4X (explore, expand, exploit, and exterminate)': [
            r'\b4x(\s+game|\s|$)',
            r'\bexplore.*expand.*exploit.*exterminate',
        ],
        'Action': [
            # Удалите слишком простой паттерн:
            # r'\baction(\s+oriented|\s+packed|\s+game|\s|$)',  # УДАЛИТЬ ЭТУ СТРОКУ

            # Добавьте более специфичные паттерны:
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
            r'\bopen\s+world(\s+game|\s|$)',
            r'\bopen-world(\s+game|\s|$)',
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
            r'\bsurvival\s+horror\b',
            r'\bsurvival\s+game\b',
            r'\bsurvival\s+elements\b',
            r'\bsurvival\s+mechanics\b',
            r'\bpost-apocalyptic\s+survival\b',
            r'\bwilderness\s+survival\b',
        ],
        'Thriller': [

            # Добавить более специфичные и контекстные паттерны для триллера:

            # 1. Паттерны, которые явно указывают на жанр триллера
            r'\bpsychological\s+thriller\b',
            r'\btechno[-\s]?thriller\b',
            r'\bpolitical\s+thriller\b',
            r'\bspy\s+thriller\b',
            r'\blegal\s+thriller\b',
            r'\bmedical\s+thriller\b',
            r'\bmilitary\s+thriller\b',
            r'\bcrime\s+thriller\b',
            r'\bconspiracy\s+thriller\b',

            # 2. Словосочетания, характерные для триллеров
            r'\bheart[-\s]?pounding\s+suspense\b',
            r'\bedge[-\s]?of[-\s]?your[-\s]?seat\s+(suspense|thriller)\b',
            r'\btense\s+thriller\b',
            r'\bintense\s+thriller\b',
            r'\bgripping\s+thriller\b',
            r'\brelentless\s+suspense\b',

            # 3. Контекстные описания, характерные для триллеров
            r'\bcat[-\s]?and[-\s]?mouse\s+(game|chase)\b',
            r'\bmind\s+games?\b',
            r'\bpsychological\s+mind\s+games?\b',
            r'\btense\s+standoff\b',
            r'\bdeadly\s+game\s+of\s+(cat|wits)\b',

            # 4. Только если "thriller" является существительным в контексте
            r'\bin\s+(the\s+)?style\s+of\s+a\s+thriller\b',
            r'\bas\s+a\s+thriller\b',
            r'\bthis\s+thriller\b',
            r'\bthe\s+thriller\s+(elements|aspects)\b',

            # 5. Улучшенная версия простого паттерна (только в определенном контексте)
            r'\b(thriller|suspense)(?:\s+(game|title|novel|film|movie|story|tale|narrative|plot))?\b',
            # Но с проверкой, что это не часть другого слова
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
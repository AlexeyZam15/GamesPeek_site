# games/analyze/pattern_manager.py
import re
from typing import Dict, List, Any, Union


class PatternManager:
    """Менеджер для работы с паттернами - только статические паттерны"""

    @classmethod
    def get_all_patterns(cls) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Возвращает ВСЕ скомпилированные паттерны и стоп-слова

        Возвращает структуру:
        {
            'genres': {
                'Action': {'patterns': [re.Pattern, ...], 'stop_words': []},
                'Precision Combat': {'patterns': [re.Pattern, ...], 'stop_words': [...]},
                ...
            },
            'themes': {...},
            'perspectives': {...},
            'game_modes': {...}
        }
        """
        result = {}

        for category, patterns_dict in [
            ('genres', cls.GENRE_PATTERNS),
            ('themes', cls.THEME_PATTERNS),
            ('perspectives', cls.PERSPECTIVE_PATTERNS),
            ('game_modes', cls.MODE_PATTERNS)
        ]:
            result[category] = {}

            for name, value in patterns_dict.items():
                # Определяем patterns и stop_words
                if isinstance(value, dict) and 'patterns' in value:
                    patterns = value['patterns']
                    stop_words = value.get('stop_words', [])
                else:
                    patterns = value
                    stop_words = []

                # Компилируем паттерны
                compiled_patterns = []
                for pattern_str in patterns:
                    try:
                        if pattern_str.startswith('(?c)'):
                            actual_pattern = pattern_str[4:].lstrip()
                            compiled_patterns.append(re.compile(actual_pattern, re.UNICODE))
                        else:
                            compiled_patterns.append(re.compile(pattern_str, re.IGNORECASE | re.UNICODE))
                    except re.error as e:
                        print(f"⚠️ Ошибка компиляции паттерна '{pattern_str}': {e}")

                result[category][name] = {
                    'patterns': compiled_patterns,
                    'stop_words': stop_words
                }

        return result

    @staticmethod
    def _compile_patterns_dict(patterns_dict: Dict[str, Union[List[str], Dict]]) -> Dict[str, List[re.Pattern]]:
        """
        Компилирует словарь паттернов с поддержкой:
        - простого списка строк: {'name': ['pattern1', 'pattern2']}
        - словаря с ключами 'patterns' и 'exclude': {'name': {'patterns': [...], 'exclude': [...]}}
        """
        compiled = {}
        for name, value in patterns_dict.items():
            compiled_patterns = []

            # Определяем список паттернов
            if isinstance(value, dict) and 'patterns' in value:
                patterns = value['patterns']
                # stop_words не нужны здесь, они будут использоваться в analyze_game_criteria_fast.py
                # Но сохраняем их отдельно? Нет, они не нужны в скомпилированном виде
            else:
                patterns = value

            for pattern_str in patterns:
                try:
                    if pattern_str.startswith('(?c)'):
                        actual_pattern = pattern_str[4:].lstrip()
                        compiled_patterns.append(re.compile(actual_pattern, re.UNICODE))
                    else:
                        compiled_patterns.append(re.compile(pattern_str, re.IGNORECASE | re.UNICODE))
                except re.error as e:
                    print(f"⚠️ Ошибка компиляции паттерна '{pattern_str}': {e}")

            compiled[name] = compiled_patterns

        return compiled

    @staticmethod
    def is_valid_simulator_match(text: str, match_start: int, match_end: int) -> bool:
        """Проверяет, что перед 'X simulator' нет отрицания."""
        negations = {'not', "isn't", 'aren\'t', 'wasn\'t', 'weren\'t', 'no', 'never', 'nor', 'without'}

        before_start = max(0, match_start - 30)
        before_text = text[before_start:match_start].lower()

        for neg in negations:
            if neg in before_text:
                return False

        return True

    GENRE_PATTERNS = {
        'Action': [
            r'\baction\b(?:(?!\.|\!|\?|\n).){0,30}?\bgames?\b',
            r'\baction(?!-)\b(?:(?!\.|\!|\?|\n).){0,30}?\b(?:adventure|thriller|rpg|strategy|puzzle|platformer|shooter|horror|survival|stealth|racing|fighting|simulation|casual|indie|sports|MMO|role-playing?)\b',
            r'\b(?:instant|real-time|fast|rapid|sudden)(?:(?!\.|\!|\?|\n).){0,30}?\baction(?!-)\b',
            # r'\baction\b',
        ],
        # 'Adventure': [
        #     # Core genre markers for 'adventure' (from frequency analysis)
        #     r'\badventure\b(?:(?!\.|\!|\?|\n).){0,30}?\b(?:action|horror|survival|puzzle|platformer|platforming|roguelike|strategy|turn-based|open-world|point-and-click|visual novel)\b',
        #
        #     # # # Reverse order (genre before adventure)
        #     # r'\b(?:action|horror|survival|puzzle|platformer|roguelike|strategy|turn-based|open-world|point-and-click|visual novel)\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     #
        #     # # Role-playing specific (most common hybrid)
        #     # r'\b(?:role-playing|rpg)\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     # r'\badventure\b(?:(?!\.|\!|\?|\n).){0,30}?\b(?:role-playing|rpg)\b',
        #     #
        #     # # Fantasy as genre modifier (often "fantasy adventure" as a genre)
        #     # r'\bfantasy\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     # r'\badventure\b(?:(?!\.|\!|\?|\n).){0,30}?\bfantasy\b',
        #     #
        #     # # Dungeon-crawler subtype
        #     # r'\b(?:dungeon|crawler)\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     # r'\badventure\b(?:(?!\.|\!|\?|\n).){0,30}?\bdungeon\b',
        #     #
        #     # # Quest/adventure game hybrid
        #     # r'\b(?:quest|questing)\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     #
        #     # # Classic adventure game (often denotes genre purity)
        #     # r'\bclassic\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     # r'\badventure\b(?:(?!\.|\!|\?|\n).){0,30}?\bclassic\b',
        #     #
        #     # # Episodic adventure (genre format)
        #     # r'\bepisodic\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        #     #
        #     # # Story-driven as genre qualifier
        #     # r'\bstory-driven\b(?:(?!\.|\!|\?|\n).){0,30}?\badventure\b',
        # ],
        'Arcade': [
            r'\barcade\s+(?:game|mode|style|classic|action|experience|vibe|shooter|racer|fighter|platformer|puzzle)\w*\b',
            r'\barcade\b(?:(?!\.|\!|\?|\n).){0,30}?\b(?:cabinet|machine)\w*\b',
        ],
        'Base Building': [
            r'(?<!-)\b(?:build\w*|rebuild\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:base|fortress\w*|stronghold\w*|settlement\w*|outpost\w*|headquarters\w*|colon\w*|structure\w*|building\w*|facilit\w*|home\w*|room\w*|base|bases)\b',
            # r'(?<!-)\b(?:build\w*|rebuild\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:base|bases)\b',
        ],
        'Card & Board Game': [
            # === card + game/collecting ===
            # r'\bcard\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgame\b',

            # r'\bcard\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcollecting\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgame\b',

            r'(?s)\b(?:card\s+(?:battlers?|battles?|games?|duels?)|tcg|ccg)\b(?!.*?(?:mini-?game|minigame|optional|not\s+a\s+card\s+game|cash\s+card|shark\s+card|gift\s+card|memory\s+card|matching\s+card|solitaire|poker|blackjack|gwent|arcomage|war\s+card\s+game|fishing|racing|horse\s+racing|match-3|puzzle|platformer|stealth|shoot\s+em\s+up|action\s+rpg|turn-based\s+strategy|4x|rts|mmorpg|moba|erotic|hentai|adult|nsfw|visual\s+novel|dating\s+sim|otome|train\s+simulation|business\s+simulation|tycoon|management|survival|idle|clicker|incremental|open\s+world|sandbox|dungeon\s+crawler|hack\s+and\s+slash|beat\s+em\s+up))',

            r'(?s)\bcollectible\s+card\s+game\b(?!.*?(?:mini-?game|minigame|optional|not\s+a\s+card\s+game|erotic|adult|nsfw))',

            r'(?s)\broguelik\w*\s+card\s+(?:battlers?|games?)\b(?!.*?(?:mini-?game|minigame|optional|not\s+a\s+card\s+game|erotic|adult|nsfw))',

            # # === card + battle/combat/duel ===
            # r'\bcard\s+(?:(?!\.|\!|\?|\n).){0,25}?\bbattl\w*\b',
            # r'\bcard\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcombat\b',
            #
            # # === deck + battle/combat (оба порядка) ===
            # r'\bdeck\s+(?:(?!\.|\!|\?|\n).){0,25}?\bbattl\w*\b',
            # r'\bdeck\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcombat\b',
            # r'\bbattl\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bdeck\b',
            # r'\bcombat\s+(?:(?!\.|\!|\?|\n).){0,25}?\bdeck\b',
            #
            # # === deck + building/builder/build ===
            # r'\bdeck\s+(?:(?!\.|\!|\?|\n).){0,25}?\bbuilding\b',
            # r'\bdeck\s+(?:(?!\.|\!|\?|\n).){0,25}?\bbuilder\b',
            # r'\bdeck\s+(?:(?!\.|\!|\?|\n).){0,25}?\bbuild\b',
            #
            # # === collectible/trading + card ===
            # r'\bcollectible\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcard\b',
            # r'\btrading\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcard\b',
            #
            # # === roguelike + card (оба порядка) ===
            # r'\broguelike\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcard\b',
            # r'\bcard\s+(?:(?!\.|\!|\?|\n).){0,25}?\broguelike\b',

        ],
        'Engineering': [
            # 1. Строительство кораблей — сужаем до космических/подводных с инженерными компонентами
            r'(?s)\b(?:build|construct|design|create)\s+(?:(?!\.|\!|\?|\n).){0,100}?\b(?:spaceship|submarine|vessel|spacecraft|starship)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:from\s+scratch|custom|module|component|piece\s+by\s+piece|block|part|blueprint)\b|\b(?:build|construct|design|create)\s+(?:(?!\.|\!|\?|\n).){0,100}?\b(?:ship|vehicle|craft)\s+(?:(?!\.|\!|\?|\n).){0,60}?\b(?:engine|reactor|weapon|cannon|shield|thruster|module|component|conveyor|pipe|wire|circuit)\b(?!.*?(?:esports|e?sports|tycoon|management|simulator|rpg|strategy|mmo))',

            # 3. Конвейеры, трубы, проводка
            r'\b(?:wire|wiring|pipe|conveyor|circuit|cable|tube|fluid|logistics|transport\s+belt)\s+(?:(?!\.|\!|\?|\n).){0,100}?\b(?:system|network|grid|line|chain)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:build|design|create|construct|automate|connect|place)\b',

            # 6. Установка инженерных блоков — исключаем ложные engine (cards engine, game engine, rpg engine)
            r'\b(?:build|place|construct)\s+(?:(?!\.|\!|\?|\n).){0,60}?\b(?:generator|reactor|conveyor|pump|engine(?!.*(?:cards|card|game|rpg|simulation|tabletop|virtual|modding))|thruster|gyroscope|landing\s+gear|oxygen\s+generator|hydrogen\s+tank|nuclear\s+reactor)\b',

            # 8. Модульное/компонентное строительство
            r'\b(?:modular|component|module|voxel)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:building|construction|design|system)\s+(?:(?!\.|\!|\?|\n).){0,60}?\b(?:ship|base|spaceship|submarine|station|settlement)\b',

            # 10. Энергетические сети
            r'\b(?:power\s+grid|energy\s+grid|electrical\s+system|power\s+distribution|electricity)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:build|design|construct|set\s+up|manage|simulation)\b',

            # 11. Строительство с нуля (только космические/подводные)
            r'\b(?:build\s+from\s+scratch|design\s+and\s+build|create\s+your\s+own)\s+(?:(?!\.|\!|\?|\n).){0,100}?\b(?:spaceship|submarine|factory|space\s+station|starship)\b',

            # 14. Инженерные игровые фразы
            r'\b(?:build\s+and\s+maintain|construct\s+and\s+manage|design\s+and\s+build|engineering\s+game|sandbox\s+engineering)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:factory|ship|station|vehicle|spaceship|submarine)\b',

            # 15. Программирование и скриптинг
            r'\b(?:programmable|scripting|lua|visual\s+scripting|logic\s+system)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:block|chip|module|controller|automation)\b',

            # 16. Ключевые инженерные фразы
            r'\b(?:build\s+and\s+maintain\s+factories|physics\s+building\s+game|damage\s+simulations|customizable\s+components)\b',

            # 17. Симуляция физики
            r'\b(?:damage\s+simulation|volumetric\s+physics|buoyancy|sealed\s+compartments|inertia\s+tensor)\s+(?:(?!\.|\!|\?|\n).){0,60}?\b(?:engine|system|game)\b',

            # 18. Логистические системы и конвейеры (Mindustry, Factorio, Satisfactory)
            r'\b(?:conveyor|supply\s+chain|logistics|production\s+block|assembly\s+line|resource\s+distribution|factory\s+block|production\s+chain)\s+(?:(?!\.|\!|\?|\n).){0,100}?\b(?:design|create|manage|optimize|automate|set\s+up)\b',


            # 19. Строительство с нуля (альтернативные фразы) — только космические/станции
            r'\b(?:(?:assemble|build|construct)\s+your\s+own)\s+(?:(?!\.|\!|\?|\n).){0,60}?\b(?:spaceship|submarine|space\s+station)\b(?!.*(?:team|card|deck))',

            # 20. Ремонт и обслуживание корабля (Barotrauma, FTL)
            r'\b(?:maintain|repair|operate)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:submarine|ship|vessel|spaceship)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:systems|life\s+support|reactor|wiring|hull|integrity|oxygen|pressure)\b',

            # 22. Barotrauma — уникальные инженерные термины (on-board wiring, nuclear reactor, barotrauma)
            r'\b(?:on-board\s+wiring|complex\s+(?:on-board\s+)?systems?\s+(?:simulation|management)|(?:maintain|repair|operate)\s+(?:nuclear\s+)?reactor|oxygen\s+generator|(?:hull\s+integrity|water\s+pressure)\s+(?:simulation|system))\b',

            # 25. Без gear trains
            r'\b(?:mechanical\s+(?:energy|power)|(?:cogs|gears?)\s+(?:and\s+)?belts?|mechanical\s+systems?)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:build|design|construct|automate|power)\b|\b(?:build|design|construct|automate|power)\s+(?:(?!\.|\!|\?|\n).){0,80}?\b(?:mechanical\s+(?:energy|power)|(?:cogs|gears?)\s+(?:and\s+)?belts?|mechanical\s+systems?)\b',

            # 26. Конвейеры для транспортировки предметов
            r'\b(?:pipe\s+conveyors?|conveyors?\s+to\s+transport|transport\s+items?\s+(?:through|via))\b',

            # r'\b(?:build\w*|maintain\w*)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:reactors?|wiring|circuits?|submarines?|sonars?|pumps?|machinery)\b',
            # r'\b(?:build\w*|construct\w*)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:machines?|vehicles?|devices?|contraptions?|submarines??)\b',
            # r'\b(?:repair\w*|fix\w*|maintain\w*)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:submarines?|reactors?|engines?|pumpS?|sonars?|wiring|circuits?|machinery|on-board)\b',

            # # wiring + system
            # r'\bwiring\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsystems?\b',
            #
            # # machines + mechanical (на основе: 'War machines, mechanical puzzles')
            # r'\bmechanical\s+(?:(?!\.|\!|\?|\n).){0,25}?\bmachines?\b',
            #
            # # mechanical + systems (на основе: 'mechanical systems')
            # r'\bmechanical\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsystems?\b',
            #
            # # circuit + system
            # r'\bcircuit\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsystems?\b',
            # r'\bsystems?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcircuit\b',
            #
            # # circuit + design
            # r'\bcircuit\s+(?:(?!\.|\!|\?|\n).){0,25}?\bdesign\b',
        ],
        'Grid-Based': [
            r'\b\d+\s*[x×]\s*\d+\s+grid(?:-shaped)?\b',
            r'\b(?:hex(?:agonal)?|isometric|square|tile)\s+grids?\b',
            r'\bhex\s+maps?\b',
            r'\b(?:hex(?:agonal)?|isometric|square)\s+battlefields?\b',
            r'\bgrid[- ]based\b',
            r'\b(?:turn[ -]?based|tactical|strategy|battle|combat|wargame)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:grids?|hex(?:agonal)?)\b'
        ],
        'MOBA': [
            r'\bmoba\b',
            r'\bmultiplayer\s+online\s+battle\s+arena\b',
            # r'\b(?:5v5|3v3|team-based)\s+(?:arena)\b',
        ],
        'Music': [
            r'\b(?:music|rhythm|dance|beat|audio)\s+(?:game|action)\b',
            r'\brhythm[-\s]?(?:based|gameplay|mechanics)\b',
            r'\b(?:press|hit|tap|hold)\s+notes?\s+in\s+time\s+with\s+the\s+music\b',
        ],
        'Open World': [
            r'\bopen[-\s]?world\b',
        ],
        'Pinball': [
            r'\bpinball\b',
        ],
        'Platform': [
            r'\bplatformer\b',

            # 1. Platform + Action (83 и 204 срабатывания)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\baction\b',
            r'\baction\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 2. Platform + Adventure (67 и 77 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\badventure\b',
            r'\badventure\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 3. Platform + Puzzle (57 и 74 срабатывания)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bpuzzl\w*\b',
            r'\bpuzzl\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 4. Platform + Combat (58 и 26 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcombat\b',
            r'\bcombat\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 5. Platform + Fight (23 и 15 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bfight\w*\b',
            r'\bfight\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 6. Platform + RPG (101 и 47 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\brpg\b',
            r'\brpg\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 7. Platform + Metroidvania (9 и 9 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bmetroidvania\b',
            r'\bmetroidvania\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 8. Platform + Scrolling (74 срабатывания)
            r'\bscroll\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 9. Platform + Jump (32 и 28 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bjump\w*\b',
            r'\bjump\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 10. Platform + Precision (15 срабатываний)
            r'\bprecision\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 11. Platform + Elements (40 и 10 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\belement\w*\b',
            r'\belement\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 12. Platform + Features (25 и 36 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bfeatur\w*\b',
            r'\bfeatur\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 13. Platform + Challenges (28 и 39 срабатываний)
            r'\bchalleng\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bchalleng\w*\b',

            # 14. Platform + Level (21 и 13 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\blevel\w*\b',
            r'\blevel\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 15. Platform + World (19 и 32 срабатывания)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bworld\w*\b',
            r'\bworld\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 16. Platform + Mechanics (15 и 6 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bmechanic\w*\b',
            r'\bmechanic\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 17. Platform + Shoot (12 и 8 срабатываний)
            r'\bplatform\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshoot\w*\b',
            r'\bshoot\w*\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 18. Platform + Hack and Slash (5 срабатываний)
            r'\bhack and slash\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',

            # 19. Action-Platformer (50 срабатываний)
            r'\baction[- ]platform\w*\b',

            # 20. Souls-like (2 срабатывания)
            r'\bsouls[- ]?(?:like|lite)\s+(?:(?!\.|\!|\?|\n).){0,25}?\bplatform\w*\b',
        ],
        'Point-and-click': [
            r'\bpoint\s+(?:and\s+|&\s+|&amp;\s+|[n’\']\s?n?\s?\'?\s*|a\s+)?click\w*\b',
            r'\bclick\w*\s+(?:and\s+|&\s+|&amp;\s+|[n’\']\s?n?\s?\'?\s*|a\s+)?point\b',
            r'\bpoint\s*[-–—]\s*click\w*\b',
            r'\bclick\w*\s*[-–—]\s*point\b',
        ],
        'Precision Combat': {
            'stop_words': [],
            'patterns': [
                r'\b(?:dodge|evade|avoid)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:attack|strike|hit|damage)\b',
                r'\b(?:attack|strike|hit|damage)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:dodge|evade|avoid)\b',
                r'\b(?:precis\w*|accura\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:strike|shot|hit|combat)\b',
                r'\b(?:strike|shot|hit|combat)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:precis\w*|accura\w*)\b',
            ]
        },
        'Puzzle': [
            r'\bpuzzle(?:\s+(?:game|title|genre|experience|adventure|platformer|rpg))?\b',
            r'\b(?:logic|brain|mind)\s+(?:puzzle|teaser|challenge)\b',
            r'\b(?:solve|figure\s+out)\s+(?:puzzles?|riddles?|enigmas?|challenges?)\b',
            r'\b(?:environmental|physics|mechanical|electrical)\s+puzzles?\b',
        ],
        'Quiz/Trivia': [
            r'\b(?:quiz|trivia)(?:\s+(?:game|mode|challenge|round))?\b',
            r'\b(?:multiple\s+choice|true/false|fill-in-the-blank)\s+(?:questions?|format)\b',
            r'\b(?:test|challenge)\s+your\s+(?:knowledge|trivia\s+skills)\b',
        ],
        'Racing': [
            r'\bracing(?:\s+(?:game|simulator|title|genre|experience|action))?\b',
            r'\bdriving(?:\s+(?:game|simulator|experience|physics))?\b',
            r'\b(?:arcade|simulation|realistic)\s+(?:racing|driving)\b',
            r'\b(?:lap|track|circuit|course)\s+(?:time|record|design)\b',
            r'\b(?:car|vehicle)\s+(?:customization|tuning|upgrades|modification)\b',
        ],
        'Real Time Strategy (RTS)': [
            r'\breal(?:\s+)?time(?:\s+)?strategy\b',
            r'\brts(?:\s+(?:game|title|genre))?\b',
            r'\b(?:gather|harvest|mine)\s+resources\s+in\s+real-time\b',
            r'\b(?:build\s+up\s+(?:your|a)\s+army|mass\s+production)\s+while\s+managing\s+(?:economy|tech\s+tree)\b',
        ],
        'Real-Time with Pause (RTwP)': [
            r'\breal-time\s+with\s+pause\b',
            r'\brtwp\b',
            r'\b(?:pausable|pauseable)\s+real-time\b',
            r'\b(?:issue\s+commands?|queue\s+actions?)\s+while\s+paused\b',
        ],
        'Roguelike / Roguelite': [
            r'\brogu(?:e|)(?:like|lite)\b',
            r'\brogue-like\b',
            r'\b(?:permadeath|procedural|randomized)\s+(?:dungeons?|levels?|worlds?|generation)\b',
            r'\b(?:run-based|session-based)\s+(?:progression|gameplay)\b',
        ],
        'Role-playing (RPG)': [
            r'\brole-playing(?:\s+(?:game|title|genre))?\b',
            r'\brpg\b',
            r'\b(?:character|class|skill)\s+(?:creation|customization|development|progression|tree)\b',
            r'\b(?:experience|level|stat|attribute)\s+(?:points?|system|mechanics)\b',
            r'\b(?:quest|mission|objective|task)\s+(?:system|design|log|journal)\b',
            r'\bnew\w*\s+(?:(?!\.|\!|\?|\n).){0,30}?\btrait\w*\b',

            # attribute/stat/point/ability/skill/level и их альтернативы
            r'\b(?:charact\w*|skill\w*|strength\w*|level\w*|abilit\w*|hero\w*|stat|stats|magic\w*|class|classes)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:attribut\w*|stat|stats|point\w*|abilit\w*)\b',

            r'\b(?:skill\w*|class|classes|abilit\w*|stat|stats|attribut\w*|trait|traits|equip\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:system\w*|tree\w*|progression\w*|point\w*|slot\w*|inventory\w*|loadout\w*|abilit\w*|skill\w*|unit\w*|class|classes|stat|stats|perk\w*|trait|traits|upgrad\w*|craft\w*)\b',
        ],
        'Sandbox': [
            r'\bsandbox\w*\b',
        ],
        'Shooter': [
            r'\bshooter\b',
            # # Из ТОП-20 фраз: 'first-person shooter' (124 вхождения)
            # r'\bfirst-person\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'third-person' (114 вхождений)
            # r'\bthird-person\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'multiplayer' (178 вхождений)
            # r'\bmultiplayer\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'extraction' (40 вхождений)
            # r'\bextraction\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'hero' (106 вхождений)
            # r'\bhero\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'tactical' (81 вхождение)
            # r'\btactical\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'arena' (73 вхождения)
            # r'\barena\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            #
            # # Из ТОП-20 слов: 'survival' (122 вхождения)
            # r'\bsurvival\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            # r'\bshooter\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsurvival\b',
            #
            # # Из ТОП-20 слов: 'action' (223 вхождения)
            # r'\baction\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
            # r'\bshooter\s+(?:(?!\.|\!|\?|\n).){0,25}?\baction\b',
            #
            # # Shooter + Game (из контекста анализа: 'shooter game' встречается)
            # r'\bshooter\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgame\b',
            # r'\bgame\s+(?:(?!\.|\!|\?|\n).){0,25}?\bshooter\b',
        ],
        'Simulator': [
            # features simulation
            r'\bfeatures?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsimulation\b',
            r'\bsimulation\s+(?:(?!\.|\!|\?|\n).){0,25}?\bfeatures?\b',
            # Прямые жанровые маркеры (X simulator / X simulation)
            r'(?i)\b(?:life|farming|flight|truck|business|construction|police|wolf|animal|driving|combat|space|social|dating|city)\s+simulat(?:ion|or)\b',
        ],
        'Sport': [
            r'\bsports?(?:\s+(?:game|title|genre|simulator|experience))?\b',
            r'\b(?:football|soccer|basketball|baseball|hockey|tennis|golf)\s+(?:game|simulator|simulation)\b',
            r'\b(?:career|franchise|manager)\s+(?:mode|system)\s+in\s+sports?\s+game\b',
            r'\b(?:team|player)\s+(?:management|trading|drafting)\b',
        ],
        'Squad Management': [
            r'\bplayer\s+(?:mov\w*)\s+(?:characters|units)\b',
            r'\b(?:squad\w*|partyteam\w*|group\w*)[-\s]?(?:based|manag\w*)\b',
            r'\b(?:build\w*|manag\w*|control\w*|lead\w*|command\w*|assembl\w*|create|recruit\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:squad\w*|party|team\w*|group\w*|crew\w*)\b',
            r'\b(?:recruit\w*|assembl\w*|hir\w*|commands?|commanding)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:member\w*|companion\w*|follower\w*|ally|allies|unit\w*|squadmate\w*|character\w*|hero\w*)\b',
        ],
        'Stealth': [
            r'\bstealth(?:\s+(?:game|action|experience|mechanics|system|based|focused))?\b',
            r'\bsneaking(?:\s+(?:game|mechanics|section))?\b',
            r'\b(?:avoid|evade|bypass)\s+(?:detection|guards|enemies|security)\b',
            r'\b(?:silent\s+takedown|non-lethal|distraction)\s+(?:mechanics|options|system)\b',
            r'\b(?:visibility|noise|light|shadow)\s+(?:meter|system|mechanics)\b',
        ],
        'Strategy': [
            r'\bstrategy(?:\s+(?:game|title|genre|rpg|experience|simulation))?\b',
            r'\bstrategic(?:\s+(?:game|thinking|planning|decision))?\b',
            r'\b(?:macro|micro)\s+(?:management|strategy|control)\b',
            r'\b(?:resource|economy)\s+(?:management|allocation|optimization)\b',
        ],
        'Survival': [
            r'\bsurvival\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:skill\w*|manag\w*|game\w*|title|experience|genre|sim|simulator|horror|craft\w*|element\w*|mechanic\w*|base|based|focus\w*|oriented)\b',
            r'\b(?:open-world|post-apocalyptic|wilderness|hardcore|single-player|sandbox|zombie|rpg)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:survival)\b',
            r'\b(?:manage|monitor|maintain)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:food|hunger|thirst|oxygen)\b',
            r'\b(?:scaveng\w*|forag\w*|hunt\w*|gather\w*|grow\w*|fish\w*|produc\w*|cultivat\w*|harvest\w*|provid\w*|farm\w*|find\w*|collect\w*|search\w*|need\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:food)\b',
        ],
        'Tactical': [
            r'\btactical(?:\s+(?:game|rpg|shooter|combat|gameplay|decisions|thinking))?\b',
            r'\btactics(?:\s+(?:game|rpg|combat|system))?\b',
            r'\b(?:positioning|flanking|cover|line-of-sight|suppressing\s+fire)\s+(?:mechanics|system|matters)\b',
            r'\b(?:turn-based|real-time)\s+tactics\b',
        ],
        'Turn-based': [
            r'\bturn[-\s]?(?:based|by\s+turn|system|combat|strategy|tactics|gameplay)\b',
            r'\btbs\b',
            r'\b(?:player|enemy|character|unit)\s+turns?\b',
            r'\b(?:alternating|sequential)\s+(?:turns?|actions?)\b',
            r'\b(?:wait|queue)\s+for\s+your\s+turn\b',
        ],
        'Turn-based strategy (TBS)': [
            r'\bturn-based\s+strategy\b',
            r'\btbs\s+(?:game|strategy)\b',
            r'\bstrategy\s+turn-based\b',
        ],
        'Visual Novel': [
            r'\bvisual\s+novel\b',
            r'\bvn(?:\s+(?:game|experience))?\b',
            r'\b(?:branching|diverging)\s+(?:story|narrative|paths?|routes?)\b',
            r'\b(?:dialogue|conversation)\s+(?:choices?|options?|system)\b',
            r'\b(?:read|experience)\s+(?:a\s+)?(?:story|narrative)\s+with\s+(?:multiple\s+)?endings\b',
        ],
        'Built-in Editors': [
            r'\b(?:level\w*|map\w*|mission\w*|scenario\w*|campaign\w*|world\w*|terrain\w*|character\w*|item\w*|weapon\w*|spell\w*|script\w*|built-in\w*|in-game\w*|design\w*|build\w*|mak\w*|shar\w*|steam)\s+(?:(?!\.|\!|\?|\n).){0,25}?\beditor\w*\b',
        ],
    }

    THEME_PATTERNS = {
        'Modding': [
            r'\b(?:mods?|modding)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:support|tools?|system|community|workshop|content)\b',

            r'\bsteam\s+(?:(?!\.|\!|\?|\n).){0,25}?\bworkshop\b',
            r'\bworkshop\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsupport\b',

            r'\b(?:creat\w*|mak\w*|build\w*|design\w*|add\w*|install\w*|use\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:mods?|modding)\b',
        ],
        'Procedural Generation': [
            # procedurally + world/worlds
            r'\bprocedurall?y\s+(?:(?!\.|\!|\?|\n).){0,25}?\bworld(?:s)?\b',
            r'\bworld(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bprocedurall?y\b',

            # procedurally + dungeon(s)
            r'\bprocedurall?y\s+(?:(?!\.|\!|\?|\n).){0,25}?\bdungeon(?:s)?\b',
            r'\bdungeon(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bprocedurall?y\b',

            # procedurally + level(s)
            r'\bprocedurall?y\s+(?:(?!\.|\!|\?|\n).){0,25}?\blevel(?:s)?\b',
            r'\blevel(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bprocedurall?y\b',

            # procedurally + map(s)
            r'\bprocedurall?y\s+(?:(?!\.|\!|\?|\n).){0,25}?\bmap(?:s)?\b',
            r'\bmap(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bprocedurall?y\b',

            # # randomly + generated (основная коллокация)
            # r'\brandomly\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgenerat(?:ed|ing|es)?\b',
            # r'\bgenerat(?:ed|ing|es)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\brandomly\b',

            # # random + generation
            # r'\brandom(?:ly)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgenerat(?:ion|ed|ing|es)?\b',
            # r'\bgenerat(?:ion|ed|ing|es)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\brandom(?:ly)?\b',

            # procedural + generation
            r'\bprocedural\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgenerat(?:ion|ed|ing|es)?\b',

            # dungeon + generation
            r'\bdungeon(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:generation)\b',
            r'\b(?:generation)\s+(?:(?!\.|\!|\?|\n).){0,25}?\bdungeon(?:s)?\b',

            # world + generation
            r'\bworld(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bgenerat(?:ion|ed|ing|es)?\b',
            r'\bgenerat(?:ion|ed|ing|es)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bworld(?:s)?\b',

            # level + generation + procedural (упрощенная версия двух ключевых слов)
            r'\blevel(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:procedural|random|generat(?:ed|ion)?)\b',
            r'\b(?:procedural|random|generat(?:ed|ion)?)\s+(?:(?!\.|\!|\?|\n).){0,25}?\blevel(?:s)?\b',

            # # enemy + generation
            # r'\bgenerat(?:ed|ion|ing)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\benemi(?:es|e?s)\b',
            #
            # # loot + generation
            # r'\bgenerat(?:ed|ion|ing)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bloot\b',

            # randomly + level(s)
            r'\brandomly\s+(?:(?!\.|\!|\?|\n).){0,25}?\blevel(?:s)?\b',
            r'\blevel(?:s)?\s+(?:(?!\.|\!|\?|\n).){0,25}?\brandomly\b',

            # dynamically + generation/levels
            r'\bdynamicall?y\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:generat(?:e|ed|ion|ing)|levels?)\b',
        ],
        '4X (explore, expand, exploit, and exterminate)': [
            r'\b4x(?:\s+(?:game|strategy))?\b',
            r'\b(?:explore|expand|exploit|exterminate)\b.*\b(?:explore|expand|exploit|exterminate)\b',
        ],
        'Business': [
            r'\b(?:business|corporate|corporation|company|entrepreneur|management)\s+(?:sim|simulator|simulation|game)\b',
            r'\b(?:economy|financial|stock|market)\s+(?:simulation|management|system)\b',
            r'\b(?:profit|revenue|income|expense|budget)\s+(?:management|optimization)\b',
        ],
        'Comedy': [
            r'\b(?:comedy|humor|funny|comic|lighthearted|witty|satire|parody)(?:\s+(?:game|element|tone|story))?\b',
        ],
        'Crafting & Gathering': [
            r'\bcraft(?:ing)?\s+(?:system|mechanic|station|table|recipes|ingredients)\b',
            r'\b(?:craft|create|forge|smith|manufacture|fabricate|assemble|build)\s+(?:(?!\.|\!|\?|\n).){0,40}?\b(?:items?|equipment|gear|weapons?|armor|tools|consumables?)\b',
            r'\b(?:gather|collect|harvest|mine|chop|fish|forage|scavenge|excavate)\s+(?:(?!\.|\!|\?|\n).){0,40}?\b(?:resources?|materials?|supplies?|ingredients?|components?|ore|wood|herbs)\b',
            r'\b(?:upgrade|enhance|improve|modify|refine|process|repair)\s+(?:(?!\.|\!|\?|\n).){0,40}?\b(?:gear|equipment|weapons?|armor|tools|items?)\b',
        ],
        'Cyberpunk': [
            r'\bcyber(?:[-\s]?)?punk\b',
            r'\b(?:cybernetics|cyberware|implants|augmentations|biotech)\b',
        ],
        'Dark Fantasy': [
            r'\b(?:dark|gothic|grimdark|grim\s+dark|bleak)\s+fantasy\b',
            r'\b(?:corrupted|decaying|dying)\s+(?:world|realm|kingdom)\b',
            r'\b(?:hopeless|despairing|tragic)\s+(?:atmosphere|tone|setting)\b',
        ],
        'Drama': [
            r'\bdrama(?:\s+(?:game|story|narrative|experience))?\b',
            r'\bdramatic(?:\s+(?:story|plot|narrative|twist))?\b',
            r'\b(?:emotional|heartfelt|poignant)\s+(?:story|journey|experience)\b',
        ],
        'Educational': [
            r'\b(?:educational|learning|edutainment|academic|didactic)\s+(?:game|title|software|experience)\b',
            r'\b(?:teach|learn|understand|master)\s+(?:(?!\.|\!|\?|\n).){0,40}?\b(?:concepts|skills|subjects|history|math|science|language)\b',
        ],
        'Erotic': [
            r'\b(?:erotic|sexual|mature|explicit|adult|nsfw|xxx)\s+(?:content|themes|elements|scenes|situations|game)\b',
        ],
        'Fantasy': [
            r'\bfantasy(?:\s+(?:world|setting|realm|game|rpg|adventure))?\b',
            r'\b(?:magical|mythical|legendary)\s+(?:world|setting|realm|creatures|beings)\b',
            r'\b(?:swords|sorcery|magic|dragons|elves|dwarves|orcs)\s+(?:and\s+)?(?:fantasy|setting|world)\b',
        ],
        'Fire Emblem': [
            r'\bfire\s+emblem\b',
            r'\b(?:support\s+conversations|weapon\s+triangle|permadeath)\s+(?:system|mechanic)\b',
        ],
        'Gothic': [
            r'\bgothic(?:\s+(?:horror|fantasy|atmosphere|setting|aesthetic))?\b',
            r'\b(?:dark\s+cathedrals|ancient\s+curses|forbidden\s+knowledge)\b',
        ],
        'Historical': [
            r'\bhistorical\s+(?:setting|period|era|drama|fiction|epic|simulation|reconstruction)\b',
            r'\b(?:set|based)\s+in\s+(?:the\s+)?(?:ancient|medieval|renaissance|victorian|world\s+war)\s+(?:era|period)\b',
            r'\b(?:authentic|accurate|period-correct)\s+(?:depiction|representation|setting)\b',
        ],
        'Horror': [
            r'\bhorror(?:\s+(?:game|title|experience|atmosphere|survival|psychological))?\b',
            r'\b(?:survival|psychological|lovecraftian|cosmic|body)\s+horror\b',
            r'\b(?:terrifying|frightening|creepy|eerie|disturbing)\s+(?:atmosphere|experience|encounters)\b',
        ],
        'Indie': [
            r'\bindie(?:\s+(?:game|title|developer|studio|gem|experience))?\b',
            r'\bindependent(?:\s+(?:developer|studio|production))?\b',
        ],
        'Mecha': [
            r'\b(?:mecha|meka|machina|giant\s+robots?|mobile\s+suit)\b',
            r'\b(?:pilot|control|customize)\s+(?:your|a|the)\s+mech(?:a)?\b',
        ],
        'Medieval': [
            r'\b(?:medieval|middle\s+ages|middle-age|feudal)\s+(?:setting|era|fantasy|world)\b',
            r'\b(?:castles|knights|kings|queens|peasants|serfs|feudalism)\b',
        ],
        'Non-fiction': [
            r'\b(?:non[-\s]?fiction|nonfiction|documentary|realistic|contemporary|present[-\s]?day|modern[-\s]?day)\s+(?:setting|story|experience)\b',
            r'\bbased\s+on\s+(?:real|actual|true)\s+(?:events|story|people)\b',
        ],
        'Party': [
            r'\b(?:party|group|social|casual|local\s+co-op|same\s+screen|couch\s+co-op|hot\s+seat|multiplayer\s+local|split\s+screen)\s+(?:game|games|mode|experience|title|mini-game|night)\b',
            r'\b(?:perfect\s+for\s+parties|great\s+with\s+friends|for\s+(\d+|several|multiple)\s+players|play\s+with\s+friends|versus\s+mode|competitive\s+multiplayer)\b',
            r'\b(?:board\s+game|trivia|quiz|minigames|mini-games|party\s+game|social\s+deduction|card\s+game\s+(?:for|with))\b',
        ],
        'Post-apocalyptic': [
            r'\bpost[-\s]?(?:apocalyptic|apocalypse|nuclear|atomic|doomsday|collapse)\b',
        ],
        'Romance': [
            r'\b(?:romance|romantic|love|dating)(?:\s+(?:game|sim|story|narrative|subplot|option))?\b',
            r'\b(?:relationship|affection|heart|dating)\s+(?:system|mechanics|meter|points)\b',
        ],
        'Science fiction': [
            r'(?i)\b(?:sci[-]?fi|science\s+fiction)\b',
            r'\b(?:science\s+fiction|sci-fi|sf)\s+(?:game|setting|world|adventure|rpg|shooter|strategy)\b',
            r'\b(?:futuristic|interstellar|alien+technology|space\s+travel|spaceship)\b',
        ],
        'Thriller': [
            r'\b(?:thriller|suspense)(?:\s+(?:game|story|narrative|experience|plot))?\b',
            r'\b(?:psychological|conspiracy|political|military|crime|spy|legal|medical|techno)\s+thriller\b',
            r'\b(?:twist|reveal|uncover|conspiracy)\s+(?:ending|plot|narrative|story)\b',
        ],
        'Time Travel': [
            r'\btime\s+(?:travel|loop|paradox|manipulation|jump)\b',
            r'\b(?:past|present|future|alternate\s+timeline)\s+(?:setting|visit|mission)\b',
        ],
        'Warfare': [
            r'\b(?:war|invasion|crusade|offensive|insurgency)\s+(?:against|between|of|for)\b',
            r'\b(?:global|interstellar|intergalactic|civil|world|nuclear)\s+(?:war|conflict)\b',
            r'\b(?:alien|demonic|extraterrestrial|undead)\s+(?:invasion|horde|menace|infestation)\b',
            r'\b(?:warfare|military|armed)\s+(?:conflict|operations|action)\b',
            r'\b(?:declare|wage|fight)\s+war\b',
            r'\b(?:guerrilla|asymmetric|conventional|biological|chemical)\s+(?:warfare|war)\b',
            r'\b(?:fight|battle)\w*\s+(?:(?!\.|\!|\?|\n).){0,30}?\benem\w*\s+(?:(?!\.|\!|\?|\n).){0,30}?\bwar\b'
        ],
        'Western': [
            r'\bwestern(?:\s+(?:setting|theme|game|style|adventure|rpg|shooter|action|open[-\s]?world))?\b',
            r'\b(?:wild|old|american)\s+west\b',
            r'\b(?:cowboy|outlaw|sheriff|gunslinger|frontier|saloon)\b',
        ],
        'Zombie': [
            r'\bzombie(?:s|apocalypse|outbreak|horde|survival)?\b',
            r'\b(?:undead|walking\s+dead|infected|rotting)\s+(?:hordes?|enemies|creatures)\b',
        ],
    }

    PERSPECTIVE_PATTERNS = {
        'Auditory': [
            r'\b(?:auditory|sound|audio|hearing)[-\s]?(?:based|focused|driven|centric|only)\b',
            r'\b(?:no\s+visuals|blind\s+gameplay|echolocation)\b',
        ],
        'Bird view / Isometric': [
            r'\b(?:bird[-\s]?(?:eye|s\s+eye|view)|top[-\s]?down|overhead|aerial)\s+(?:view|perspective|camera|angle)\b',
            r'\bisometric(?:\s+(?:view|perspective|camera|angle|3d))?\b',
        ],
        'First person': [
            r'\b(?:first[-\s]?person|1st\s+person|fp)(?:\s+(?:perspective|camera|view|shooter|combat|gameplay))?\b',
            r'\b(?:through\s+the\s+eyes\s+of|from\s+the\s+perspective\s+of)\s+(?:the\s+)?(?:character|protagonist|player)\b',
        ],
        'Side view': [
            r'\b(?:side\s+(?:view|scroller|scrolling|perspective)|2d\s+platformer|camera\s+from\s+the\s+side)\b',
        ],
        'Text': [
            r'\b(?:text[-\s]?based|interactive\s+fiction|parser|command\s+line|terminal)\s+(?:game|adventure|experience)\b',
        ],
        'Third person': [
            r'\b(?:third[-\s]?person|3rd\s+person|tp|tpp|over[-\s]?the[-\s]?shoulder)(?:\s+(?:perspective|camera|view|shooter|combat|gameplay))?\b',
        ],
        'Virtual Reality': [
            r'\b(?:virtual\s+reality|vr|head-mounted\s+display|hmd)(?:\s+(?:game|experience|mode|support|exclusive|headset|enabled|compatible))?\b',
            r'\b(?:360\s+degree|room-scale|standing|seated)\s+(?:vr|experience)\b',
        ],
    }

    MODE_PATTERNS = {
        'Battle Royale': [
            r'\bbattle\s+royale\b',
            r'\b(?:last\s+man\s+standing|last\s+team\s+standing|100\s+players)\b',
        ],
        'Co-operative': [
            r'\bco[-\s]?(?:op|operative|operative)\s+(?:gameplay|mode|multiplayer|campaign|experience)\b',
            r'\b(?:team\s+up|team-based|play\s+together|join\s+forces)\s+(?:with\s+friends?|co-op)\b',
        ],
        'Massively Multiplayer Online (MMO)': [
            r'\b(?:massively\s+multiplayer|mmo|mmorpg|mmo\s+rpg)(?:\s+(?:game|experience|world))?\b',
            r'\b(?:thousands\s+of\s+players|persistent\s+world|server\s+shards)\b',
        ],
        'Multiplayer': [
            r'\bmulti(?:[-\s]?)?player(?:\s+(?:game|mode|experience|match|competitive))?\b',
            r'\b(?:online|lan|local)\s+multiplayer\b',
        ],
        'Player vs Player (PvP)': [
            r'\b(?:pvp|pve|player\s+vs\s+player|player\s+versus\s+player|player\s+vs\s+environment)\b',
            r'\b(?:compete|fight|battle|duel)\s+against\s+(?:other\s+players|real\s+opponents)\b',
        ],
        'Single player': [
            r'\bsingle[-\s]?(?:player|play|experience|game|campaign|story)\b',
            r'\bsolo\s+(?:experience|play|gameplay|mode)\b',
        ],
        'Split screen': [
            r'\bsplit[-\s]?(?:screen|couch|local\s+co-op|couch\s+co-op)\b',
        ],
    }
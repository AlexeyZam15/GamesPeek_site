# games/analyze/pattern_manager.py
import re
from typing import Dict, List


class PatternManager:
    """Менеджер для работы с паттернами - только статические паттерны"""

    @classmethod
    def get_all_patterns(cls) -> Dict[str, Dict[str, List[re.Pattern]]]:
        """Возвращает ВСЕ скомпилированные паттерны БЕЗ кэширования"""
        return {
            'genres': cls._compile_patterns_dict(cls.GENRE_PATTERNS),
            'themes': cls._compile_patterns_dict(cls.THEME_PATTERNS),
            'perspectives': cls._compile_patterns_dict(cls.PERSPECTIVE_PATTERNS),
            'game_modes': cls._compile_patterns_dict(cls.MODE_PATTERNS),
        }

    @staticmethod
    def _compile_patterns_dict(patterns_dict: Dict[str, List[str]]) -> Dict[str, List[re.Pattern]]:
        """Компилирует словарь паттернов с поддержкой регистра через префикс (?c)"""
        compiled = {}
        for name, patterns in patterns_dict.items():
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
            r'\baction\b',
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
            r'\b(?:is|as|this|digital|electronic|video)\s+(board|card)\s+game\b',
            r'\b(?:play|gameplay|mechanics|styled|inspired)\s+(?:like|resembles|of)\s+(?:a\s+)?(board|card)\s+game\b',
            r'\b(?:deck|hand|draw|discard|shuffle)\s+(?:building|management|mechanics)\b',
            r'\bturn-based\s+(?:board|card)\s+(?:game|combat|strategy)\b',
        ],
        'Engineering': [
            # r'\b(?:build\w*|maintain\w*)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:reactors?|wiring|circuits?|submarines?|sonars?|pumps?|machinery)\b',
            # r'\b(?:build\w*|construct\w*)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:machines?|vehicles?|devices?|contraptions?|submarines??)\b',
            # r'\b(?:repair\w*|fix\w*|maintain\w*)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:submarines?|reactors?|engines?|pumpS?|sonars?|wiring|circuits?|machinery|on-board)\b',

            # wiring + system
            r'\bwiring\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsystems?\b',

            # machines + mechanical (на основе: 'War machines, mechanical puzzles')
            r'\bmechanical\s+(?:(?!\.|\!|\?|\n).){0,25}?\bmachines?\b',

            # mechanical + systems (на основе: 'mechanical systems')
            r'\bmechanical\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsystems?\b',

            # circuit + system
            r'\bcircuit\s+(?:(?!\.|\!|\?|\n).){0,25}?\bsystems?\b',
            r'\bsystems?\s+(?:(?!\.|\!|\?|\n).){0,25}?\bcircuit\b',

            # circuit + design
            r'\bcircuit\s+(?:(?!\.|\!|\?|\n).){0,25}?\bdesign\b',
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
            r'\b(?:5v5|3v3|team-based)\s+(?:arena)\b',
            r'\b(?:lane|jungle|tower|creep|minion|turret)\s+(?:pushing|defense|control)\b',
        ],
        'Music': [
            r'\b(?:music|rhythm|dance|beat|audio)\s+(?:game|action|experience)\b',
            r'\brhythm[-\s]?(?:based|gameplay|mechanics)\b',
            r'\b(?:timing|sync|match\s+the\s+beat)\s+(?:based|mechanic|gameplay)\b',
            r'\b(?:press|hit|tap|hold)\s+notes?\s+in\s+time\s+with\s+the\s+music\b',
        ],
        'Open World': [
            r'\bopen[-\s]?world(?:\s+(?:game|title|experience|environment|setting|adventure|exploration|rpg|action|sandbox|gameplay|mechanics|design|map))?\b',
            r'\b(?:seamless|vast|expansive|living|dynamic)\s+open\s+world\b',
            r'\b(?:explore|roam|traverse)\s+(?:freely|at\s+your\s+own\s+pace)\s+(?:the\s+)?(?:world|map|environment)\b',
            r'\b(?:non-linear|branching)\s+(?:story|quests|narrative|progression)\b',
        ],
        'Pinball': [
            r'\bpinball\b',
            r'\bpin\s+ball\b',
            r'\b(?:flipper|bumper|plunger)\s+(?:physics|mechanics|action)\b',
        ],
        'Platform': [
            r'\bplatform(?:\s+(?:game|title|genre|experience|puzzle|action))?\b',
            r'\bplatformer\b',
            r'\b(?:2d|3d)\s+platformer\b',
            r'\b(?:jump|double\s+jump|wall\s+jump|dash|glide|climb)\s+(?:based|mechanics|gameplay)\b',
            r'\bprecision\s+(?:jumping|platforming)\b',
        ],
        'Point-and-click': [
            r'\bpoint(?:\s+)?and(?:\s+)?click\b',
            r'\bp&c\b',
            r'\b(?:adventure|puzzle)\s+game\s+with\s+point-and-click\s+(?:controls|interface|mechanics)\b',
            r'\b(?:interact|examine|combine)\s+(?:with|using)\s+(?:the\s+)?(?:cursor|mouse)\b',
        ],
        'Precision Combat': [
            r'\b(?:aiming|targeting)\s+(?:mechanics|system|based)\s+requires\s+(?:precision|skill|accuracy)\b',
            r'\b(?:manual|direct|cursor-based|skill-based)\s+(?:aiming|targeting|combat)\b',
            r'\b(?:no\s+auto-aim|no\s+aim\s+assist|requires\s+precise\s+aim)\b',
            r'\b(?:twin-stick|wasd|cursor)\s+(?:controls|movement|combat)\b',
            r'\b(?:projectile|bullet)\s+(?:physics|travel\s+time|drop|dodging)\b',
        ],
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
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

    GENRE_PATTERNS = {
        'Action': [
            r'\baction\b(?:(?!\.|\!|\?|\n).){0,30}?\bgames?\b',
            r'\baction(?!-)\b(?:(?!\.|\!|\?|\n).){0,30}?\b(?:adventure|thriller|rpg|strategy|puzzle|platformer|shooter|horror|survival|stealth|racing|fighting|simulation|casual|indie|sports|MMO|role-playing?)\b',
            r'\b(?:instant|real-time|fast|rapid|sudden)(?:(?!\.|\!|\?|\n).){0,30}?\baction(?!-)\b',
            # r'\baction\b',
        ],
        'Adventure': [
            r'\b(?:action-?adventure|adventure)\s+(?:game|title|experience|story|narrative)\b',
            r'\b(?:exploration|discovery|journey)[-\s]?(?:based|focused|driven|heavy)\s+(?:game|experience|adventure)\b',
            r'\b(?:point-?-and-?-click|interactive\s+story|narrative-driven)\s+adventure\b',
            r'\b(?:uncover\s+secrets|solve\s+mysteries|discover\s+hidden\s+treasures)\s+in\s+(?:a|an)\s+adventure\b',
            r'\b(?:embark|go)\s+on\s+(?:an?\s+)?(?:epic|grand|great)\s+adventure\b',
            r'\b(?:story-rich|character-driven|choice-based)\s+adventure\b',
        ],
        'Arcade': [
            r'\barcade\s+(?:game|style|classic|action|experience|vibe|shooter|racer|fighter)\b',
            r'\barcade-style\s+(?:gameplay|action|shooter|racer)\b',
            r'\b(?:retro|classic)\s+arcade\s+(?:experience|feel|gameplay)\b',
            r'\bhigh\s+score\s+(?:chasing|hunting|system|mechanics|leaderboard)\b',
            r'\b(?:coin-op|coin-operated)\s+(?:game|machine|cabinet)\b',
        ],
        'Base Building': [
            r'(?<!-)\b(?:build\w*|rebuild\w*)\s+(?:(?!\.|\!|\?|\n).){0,25}?\b(?:base|fortress\w*|stronghold\w*|settlement\w*|outpost\w*|headquarters\w*|colon\w*|structure\w*|building\w*|facilit\w*|home\w*)\b',
        ],
        'Card & Board Game': [
            r'\b(?:is|as|this|digital|electronic|video)\s+(board|card)\s+game\b',
            r'\b(?:play|gameplay|mechanics|styled|inspired)\s+(?:like|resembles|of)\s+(?:a\s+)?(board|card)\s+game\b',
            r'\b(?:deck|hand|draw|discard|shuffle)\s+(?:building|management|mechanics)\b',
            r'\bturn-based\s+(?:board|card)\s+(?:game|combat|strategy)\b',
        ],
        'Engineering': [
            r'\b(?:build|operate|maintain|manage)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:reactor|engine|wiring|circuits?|hull|submarine|vessel|sonar|pumps?|machinery)\b',
            r'\b(?:design|build|construct)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:machines?|vehicles?|devices?|contraptions?|submarines?|vessels?)\b',
            r'\b(?:nuclear\s+reactor|wiring\s+system|power\s+grid|engine\s+control)\s+(?:operation|management|mechanics)\b',
            r'\b(?:operate|repair|fix|maintain)\s+(?:(?!\.|\!|\?|\n).){0,30}?\b(?:submarine|reactor|engine|pump|sonar|hull|wiring|circuit|machinery|on-board\s+systems?)\b',
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
        ],
        'Sandbox': [
            r'\bsandbox(?:\s+(?:game|experience|environment|world|mode|gameplay))?\b',
            r'\bsandbox-style\s+(?:gameplay|progression|design)\b',
            r'\b(?:emergent|player-driven)\s+(?:gameplay|systems|narrative)\b',
            r'\b(?:create|build|shape|manipulate)\s+(?:the\s+)?(?:world|environment|everything)\s+as\s+you\s+see\s+fit\b',
        ],
        'Shooter': [
            r'\bshooter(?:\s+(?:game|title|genre|experience))?\b',
            r'\b(?:fps|tps)\s+(?:game|shooter)\b',
            r'\b(?:first|third)-person\s+shooter\b',
            r'\b(?:cover|blind-fire|lean)\s+(?:system|mechanics)\b',
            r'\b(?:weapon|gun|arsenal)\s+(?:customization|loadout|variety)\b',
        ],
        'Simulator': [
            r'\b(?:simulator|simulation|sim)(?:\s+(?:game|experience|genre))?\b',
            r'\brealistic\s+(?:simulation|simulator|experience)\b',
            r'\b(?:life|job|career|profession|vehicle)\s+(?:simulator|simulation)\b',
            r'\b(?:sandbox|physics)\s+(?:simulation|based)\b',
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
            r'\bsurvival\s+(?:game|title|experience|genre|mode|sim|simulator|horror|crafting|elements|mechanics|based|focused|oriented)\b',
            r'\b(?:open-world|post-apocalyptic|wilderness|hardcore)\s+survival\b',
            r'\b(?:manage|monitor|maintain)\s+(?:(?!\.|\!|\?|\n).){0,40}?\b(?:hunger|thirst|stamina|health|temperature|energy|oxygen|sanity)\b',
            r'\b(?:scavenge|forage|hunt|fish|gather)\s+for\s+(?:food|water|supplies|resources|materials)\b',
            r'\b(?:craft|build|construct)\s+(?:shelter|tools|weapons|clothing|fire)\s+to\s+survive\b',
            r'\b(?:struggle|fight|battle)\s+for\s+survival\s+against\s+(?:the\s+)?(?:elements|nature|wildlife|enemies|zombies|creatures|environment)\b',
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
    }

    THEME_PATTERNS = {
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
            r'\b(?:high\s+tech|neon[-\s]?soaked|dystopian\s+future|corporate\s+domination)\b',
            r'\b(?:cybernetics|cyberware|implants|augmentations|biotech)\b',
            r'\b(?:hacking|netrunning|virtual\s+reality|matrix)\s+(?:mechanics|system)\b',
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
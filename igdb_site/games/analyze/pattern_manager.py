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
        'Turn-based': [
            r'\bturn[-\s]?based\b',
            r'\btbs\b',
            r'\bturn[-\s]?by[-\s]?turn\b',
            r'\bturns?\s+(system|mechanic|combat)\b',
            r'\bplayer\s+turns?\b',
            r'\benemy\s+turns?\b',
            r'\balternating\s+turns?\b',
            r'\bcharacter\s+turns?\b',
            r'\bwait\s+for\s+your\s+turn\b',
            r'\btake\s+turns\b',
            r'\btaking\s+turns\b',
            r'\bstrategy\s+turn[-\s]?based\b',
            r'\btactical\s+turn[-\s]?based\b',
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
        'Open World': [
            r'\bopen[-\s]?world(\s+game|\s+title|\s+experience|\s+environment|\s+setting)\b',
            r'\bopen[-\s]?world(\s+adventure|\s+exploration|\s+rpg|\s+action)?\b',
            r'\bopen[-\s]?world\s+(sandbox|gameplay|mechanics)\b',
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
        'Sandbox': [
            r'\bsandbox(\s+game)?\b',
            r'\bsandbox-style\b',
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
        'Survival': [
            r'\bsurvival(\s+(game|title|experience|horror|elements|mechanics|based|focused|oriented))?\b',
            r'\b(to\s+)?survive(\s+(the|in|against))?\b',
            r'\bsurviving\s+(in|against|the)\b',
            r'\bpost-apocalyptic\s+survival\b',
            r'\bwilderness\s+survival\b',
            r'\bsurvival\s+horror\b',
            r'\bstruggle\s+to\s+survive\b',
            r'\bfight\s+for\s+survival\b',
            r'\bbattle\s+for\s+survival\b',
            r'\bsurvival\s+of\s+the\s+fittest\b',
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
        'Fire Emblem': [
            r'\bfire\s+emblem\b',
        ],
        '4X (explore, expand, exploit, and exterminate)': [
            r'\b4x(\s+game|\s|$)',
            r'\bexplore.*expand.*exploit.*exterminate',
        ],
        'Base Building': [
            r'\bbase\s+building\b',
            r'\bbase\s+construction\b',
            r'\bbuilding\s+bases\b',
            r'\bconstructing\s+bases\b',
            r'\bplayers?\s+can\s+build\b',
            r'\byou\s+can\s+build\b',
            r'\bable\s+to\s+build\b',
            r'\bbuild(?:ing)?\s+(?:structures?|bases?|homes?|buildings?)\b',
            r'\bconstruct(?:ing)?\s+(?:buildings?|structures?)\b',
            r'\brebuild(?:ing)?\s+(?:bases?|structures?|buildings?|cities?|towns?|settlements?|castle|fortress)\b',
            r'\brebuild(?:ing)?\s+(?:your|their|the)\s+(?:castle|fortress|base|structure|building|city|town|settlement)\b',
            r'\bbuilds?\s+a\s+castle\b',
            r'\bconstructs?\s+a\s+castle\b',
            r'\bcastle\s+building\b',
            r'\bcastle\s+construction\b',
            r'\bbuild(?:ing)?\s+castles?\b',
            r'\bconstruct(?:ing)?\s+castles?\b',
            r'\beraise\s+a\s+castle\b',
            r'\bestablish\s+a\s+castle\b',
            r'\bfortress\s+building\b',
            r'\bfortress\s+construction\b',
            r'\bbuild(?:ing)?\s+fortresses?\b',
            r'\bconstruct(?:ing)?\s+fortresses?\b',
            r'\bstronghold\s+building\b',
            r'\bstronghold\s+construction\b',
            r'\bbuild(?:ing)?\s+strongholds?\b',
            r'\bconstruct(?:ing)?\s+strongholds?\b',
            r'\bbuild(?:ing)?\s+(?:your|their|a)\s+own\s+castle\b',
            r'\bconstruct(?:ing)?\s+(?:your|their|a)\s+own\s+castle\b',
            r'\bcreate(?:ing)?\s+a\s+castle\b',
            r'\beraise(?:ing)?\s+(?:your|their|a)\s+castle\b',
            r'\bdesign(?:ing)?\s+(?:your|their|a)\s+castle\b',
            r'\bdevelop(?:ing)?\s+a\s+castle\b',
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
        'Crafting & Gathering': [
            r'\bcrafting\s+system\b',
            r'\bcraft(?:ing)?\s+(?:items?|equipment|gear|weapons?|armor|tools)\b',
            r'\bgather(s|ing)?\s+(?:resources?|materials?|supplies?|ingredients?|components?)\b',
            r'\bcollect(?:ing)?\s+(?:resources?|materials?|items?|components?)\b',
            r'\bresource\s+gathering\b',
            r'\bmaterial\s+collection\b',
            r'\bgathering\s+materials?\b',
            r'\bcollecting\s+resources?\b',
            r'\bitem\s+crafting\b',
            r'\bweapon\s+crafting\b',
            r'\barmor\s+crafting\b',
            r'\bgear\s+crafting\b',
            r'\btool\s+crafting\b',
            r'\bmakes?\s+(?:their|your)?\s+equipment\b',
            r'\bupgrades?\s+(?:their|your)?\s+equipment\b',
            r'\bupgrad(?:ing|e)\s+(?:gear|armor|weapons?|tools|items?|equipment)\b',
            r'\benhanc(?:ing|e)\s+(?:gear|armor|weapons?|equipment)\b',
            r'\bimprove(?:ing)?\s+(?:gear|equipment|items?)\b',
            r'\bmodif(?:ying|y)\s+(?:gear|equipment|weapons?)\b',
            r'\bcustomiz(?:ing|e)\s+(?:gear|equipment|weapons?)\b',
            r'\bforge(?:ing)?\s+(?:weapons?|armor|tools|equipment)\b',
            r'\bsmith(?:ing)?\s+(?:weapons?|armor)\b',
            r'\bcreate(?:ing)?\s+(?:gear|equipment|items?)\b',
            r'\bmanufactur(?:ing|e)\s+(?:gear|equipment|items?)\b',
            r'\bfabricat(?:ing|e)\s+(?:gear|equipment)\b',
            r'\bbuild(?:ing)?\s+(?:equipment|gear|items?)\b',
            r'\bassemble(?:ing)?\s+(?:equipment|gear|items?)\b',
            r'\bharvest(?:ing)?\s+(?:resources?|materials?|crops?)\b',
            r'\bforag(?:ing|e)\s+(?:for|)\s+(?:resources?|materials?|ingredients?)\b',
            r'\bmining\s+(?:for|)\s+resources?\b',
            r'\bwoodcutting\b',
            r'\bfishing\s+(?:for|)\s+(?:materials?|resources?)\b',
            r'\bscaveng(?:ing|e)\s+(?:for|)\s+supplies?\b',
            r'\bexcavat(?:ing|e)\s+(?:for|)\s+materials?\b',
            r'\bprospect(?:ing)?\s+(?:for|)\s+resources?\b',
            r'\bcraft(?:ing)?\s+and\s+gather(?:ing)?\b',
            r'\bgather(?:ing)?\s+and\s+craft(?:ing)?\b',
            r'\bcraft(?:ing)?\s+and\s+upgrad(?:ing|e)\b',
            r'\bcollect(?:ing)?\s+and\s+craft(?:ing)?\b',
            r'\bcrafting\s+mechanic\b',
            r'\bgathering\s+mechanic\b',
            r'\bresource\s+management\b',
            r'\bmaterial\s+processing\b',
            r'\bcrafting\s+station\b',
            r'\bworkbench\b',
            r'\bcrafting\s+table\b',
            r'\bforge\s+station\b',
            r'\bcrafting\s+recipes?\b',
            r'\bcrafting\s+ingredients?\b',
            r'\brefin(?:ing|e)\s+(?:materials?|resources?)\b',
            r'\bprocess(?:ing)?\s+(?:materials?|resources?)\b',
            r'\bdisassemble(?:ing)?\s+items?\b',
            r'\bbreak\s+down\s+items?\b',
            r'\brepair(?:ing)?\s+(?:gear|equipment|items?)\b',
            r'\bmaint(?:ain|enance)\s+(?:gear|equipment)\b',
            r'\bunlock\s+access\s+to\s+more\s+advanced\s+(?:materials|equipment)\b',
            r'\bunlock(?:ing)?\s+(?:new|advanced|better)\s+(?:materials?|resources?|equipment)\b',
            r'\baccess\s+(?:new|advanced)\s+(?:materials?|equipment)\b',
            r'\bprogress\s+to\s+(?:better|advanced)\s+(?:materials?|equipment)\b',
            r'\badvance(?:ing|e)\s+to\s+(?:better|superior)\s+(?:materials?|equipment)\b',
            r'\bunlock(?:ing)?\s+(?:higher|tier|level)\s+(?:materials?|equipment)\b',
            r'\bgain\s+access\s+to\s+(?:advanced|superior|rare)\s+(?:materials?|equipment)\b',
            r'\bobtain(?:ing)?\s+(?:advanced|better)\s+(?:materials?|equipment)\b',
            r'\bacquir(?:ing|e)\s+(?:advanced|superior)\s+(?:materials?|equipment)\b',
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
        'Gothic': [
            r'\bgothic\b',
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
        'Medieval': [
            r'\bmedieval\b',
            r'\bmiddle\s+ages\b',
            r'\bmiddle-age\b',
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
        'Party': [
            r'\bparty\s+(game|games|title|mode)\b',
            r'\bparty-style\s+(game|gaming)\b',
            r'\bmultiplayer\s+party\b',
            r'\bcasual\s+party\b',
            r'\bfun\s+party\b',
        ],
        'Precision Combat': [
            r'\baim\s+skill[\s-]?shots?\b',
            r'\bdodge\s+projectiles?\b',
            r'\bprecise\s+WASD\s+controls\b',
            r'\bcursor[\s-]?based\s+aiming\b',
            r'\bno\s+click\s+to\s+move\b',
            r'\bmanual\s+aiming\b',
            r'\bdirect\s+control\s+combat\b',
            r'\bprecision\s+movement\b',
            r'\bskill[\s-]?based\s+aiming\b',
            r'\bprojectile\s+dodging\b',
            r'\breal[\s-]?time\s+aiming\b',
            r'\btwin[\s-]?stick\s+controls?\b',
            r'\bmanual\s+targeting\b',
        ],
        'Real-time Combat': [
            r'\breal.time.combat\b',
            r'\brealtime.combat\b',
            r'\breal.time.combat\b',
        ],
        'Romance': [
            r'\bromance(\s+game)?\b',
            r'\bromantic(\s+story)?\b',
            r'\bdating\s+sim\b',
            r'\blove(\s+story|\s+game)\b',
        ],
        'Science fiction': [
            r'\bscience\s+fiction(\s+game|\s|$)',
            r'\bsci-fi(\s+game|\s|$)',
        ],
        'Stealth': [
            r'\bstealth(\s+game)?\b',
            r'\bsneaking(\s+game)?\b',
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
            r'\ba\s+world\s+of\s+conflict\b',
        ],
    }

    PERSPECTIVE_PATTERNS = {
        'Auditory': [
            r'\bauditory\b',
            r'\bsound-based\b',
            r'\baudio-focused\b',
        ],
        'Bird view / Isometric': [
            r'\bbird[\s-]*view\b',
            r'\bisometric(\s+view|\s+game|\s|$)',
            r'\btop-down(\s+view|\s+game|\s+perspective|\s+camera|\s|$)'
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
        # ДОБАВЛЯЕМ НОВУЮ КАТЕГОРИЮ
        'Player vs Player (PvP)': [
            r'\bpvp\b',
            r'\bpvp\s+mode\b',
            r'\bplayer\s+vs\s+player\b',
            r'\bplayer\s+versus\s+player\b',
            r'\bagainst\s+other\s+players\b',
            r'\bcompete\s+against\s+other\s+players\b',
            r'\bfight\s+against\s+other\s+players\b',
            r'\bbattle\s+against\s+other\s+players\b',
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
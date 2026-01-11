"""
Хранилище ключевых слов для удаления.
Формат: KEYWORDS_BY_CATEGORY = { "category_name": "ключевые слова через запятую" }
"""

KEYWORDS_BY_CATEGORY = {
    "system_keywords": """
45 defender, aftermarket release, always online, bink video, bundled with peripherals, cross buy, ds microphone use, dsi camera support, dsiware, dual-monitor arcade games, e-reader, excluded on-disc content, first on discord, gog preservation program, humble bundle, humble original, konami code, never re-released, never released outside of bundles, unreleased, unreleased games with an aftermarket release, upcoming, vchiban, virgin killer sweater
""",

    "keywords_001": """
    achievement hitching, auto-saving, camera comfort, camera control, camera shift, closed captions, color separation, context sensitive, contextual controller rumble, convert, custom ui, custom volume controls, cutscene menu, cutscene pause, input method - button, input method - touch, optional touch control, playable without timed input, save anytime, skip button, skippable gameplay, static defense, subtitled silence, subtitles, aggressive door-opening, behind the waterfall, claimable, consolation achievements, damage, difficulty achievement, drop-in drop-out, elevator ambush, enforced playing order, full-screen attack, kill feed, kill streak, kill streak reward, non-five achievements, placeholder text, punctuation mark above head, skip, skipping rope, zero point achievements, cold open, completed but unreleased game, delayed release, fig funded, game jam, global game jam, gmtk game jam, gmtk game jam 2019, gmtk game jam 2025, igf awards, igmc, indie royale, indiedb, indiegogo, indievania, kickstarter unsuccessful, level by doing, ludum dare, ludum dare 24, ludum dare 28, ludum dare 33, ludum dare 38, ludum dare 41, ludum dare 44, ludum dare 45, ludum dare 46, ludum dare 49, ludum dare 55, ludum dare 56, mid-development ip split, pax 2008, pax 2009, pax australia 2013, pax east 2010, pax east 2011, pax east 2012, pax east 2013, pax east 2014, pax east 2015, pax east 2016, pax east 2017, pax east 2020, pax east indie showcase, pax prime 2010, pax prime 2011, pax prime 2012, pax prime 2013, pax prime 2014, pax prime 2015, pax south 2015, pax south 2017, pax south 2020, pax west 2016, pax west 2017, pre-order exclusive, pre-release public testing, project by former employee, prototype found, prototype only, released between christmas and new year's day, resistedjam, retailer exclusive, reviewed by avgn, rpg maker horror game jam, satellaview demo/trial release, satellaview slotted cartridge data, satellaview soundlink support, satellaview view-limited games, sequel by a different developer, sequel is in a different genre, spike video game awards 2003, spike video game awards 2004, spike video game awards 2006, spike video game awards 2007, spike video game awards 2008, spike video game awards 2009, spike video game awards 2010, spike video game awards 2011, sponsored by a tv network, spooktober jam, steam greenlight, steam pre-purchase rewards, steam timeline, steam turn notifications, student project, summer consumer electronics show 1990, summer of arcade 2008, summer of arcade 2010, summer of arcade 2011, summer of arcade 2012, summer of arcade 2013, to be continued, tokyo game show 2008, tokyo game show 2009, tokyo game show 2010, tokyo game show 2011, tokyo game show 2012, tokyo game show 2013, tokyo game show 2014, tokyo game show 2015, tokyo game show 2016, tokyo game show 2019, tokyo game show 2020, unresolved stories, viral achievement, winter consumer electronics show 1992, yuri game jam, yuri game jam 2024
    """,
    "keywords_002": """
    acronym titles, androgyny, apfel project, aqool al tabia, askolurumsana, box art - half of face displayed, box art - pixel art, box art - real photos, cheeky, club penguin, distaff counterpart, excluded on-disc content, failed kickstarter, flavor text, friend codes, humble original, icons, in-game announcer, letterboxed, light sensor, loyal, memorial, minor characters, mirror mode, nintendo power, no sexual content, original soundtrack release, partially voiced, physical copy protection, played for charity, playstation home tie-in, playstation underground, previously on - apple arcade, previously on - gametap, previously on - horizon catalog, previously on - horizon plus, previously on - luna plus, previously on - netflix, previously on - origin access, previously on - pc gamepass, previously on - prime gaming, previously on - stadia pro, previously on - utomik, previously on - wild unlimited, previously on - xbox gamepass, profanity in the title, running gag, snes enhancement chip - dsp-2, snes enhancement chip - s-dd1, snes enhancement chip - s-rtc, snes enhancement chip - sa1, snes enhancement chip - spc7110, snes mouse support, sports cars, steam remote play, stereo sound, super deformed cg's, super scaler, super-ness, tmtam studio, tmtamstudio, villain turned good, voices in the wii remote, xbox cloud saves, xbox controller support for pc, xbox live arcade game integration, xbox live aware, xbox live indie games, xbox live local multiplayer, xbox live vision, xbox one backwards compatibility, xbox one x enhanced, xbox play anywhere, yume nikki
    """,
    "keywords_003": """
    4k ultra hd, 60fps, cga - composite mode, color cartridges, ega graphics, hdr available, lowres, pre-rendered backgrounds, pre-rendered graphics, ray tracing
    """,
    "keywords_004": """
    
    """,
}


def parse_keywords_string(keywords_string):
    """Парсит строку с ключевыми словами через запятую в список"""
    keywords = []
    for kw in keywords_string.split(','):
        kw = kw.strip()
        if kw:  # Не добавляем пустые строки
            keywords.append(kw)
    return keywords


def get_all_keywords():
    """Возвращает все ключевые слова из всех категорий"""
    all_keywords = []
    for category, keywords_string in KEYWORDS_BY_CATEGORY.items():
        keywords = parse_keywords_string(keywords_string)
        all_keywords.extend(keywords)
    return list(set(all_keywords))  # Убираем дубликаты


def get_keywords_by_category(category_name):
    """Возвращает ключевые слова для конкретной категории"""
    if category_name in KEYWORDS_BY_CATEGORY:
        return parse_keywords_string(KEYWORDS_BY_CATEGORY[category_name])
    return []


def get_categories():
    """Возвращает список всех категорий"""
    return list(KEYWORDS_BY_CATEGORY.keys())
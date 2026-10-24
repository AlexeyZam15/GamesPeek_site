<div align="center">

# 🎮 GamesPeek – Intelligent Game Discovery Engine

**Find similar games across a database of 80,000+ titles**

[![English](https://img.shields.io/badge/Language-English-red)](README.md)
[![Русский](https://img.shields.io/badge/Язык-Русский-blue)](README.ru.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Desktop App](https://img.shields.io/badge/Download-Windows%20.exe-blue)](https://github.com/AlexeyZam15/GamesPeek_site/releases/latest)
[![Discord](https://img.shields.io/badge/Discord-Join-7289da)](https://discord.gg/YcxwAuSv)

</div>

---

## 📦 Download

**Windows users** can grab a portable archive with the EXE — no Python or PostgreSQL installation required.

➡️ **[Download the latest version](https://github.com/AlexeyZam15/GamesPeek_site/releases/latest)**

1. Open the **Releases** section on GitHub
2. Download `gamespeek-*.zip`
3. Extract to any folder (e.g. `C:\GamesPeek`)
4. Run **`gamespeek.exe`**

---

## 💬 Community

Join our Discord for support, feedback, and updates:

➡️ **[Join Discord](https://discord.gg/YcxwAuSv)**

---

## 💝 Support the Project

If GamesPeek is useful to you, consider supporting development:

➡️ **[Support the author](https://dalink.to/wolfgunt)**

Thank you for your support! ❤️

---

## 🖥️ Desktop App (Windows)

Portable GamesPeek build — **no installation**, everything inside the archive.

### What's inside the archive

- `gamespeek.exe` — small launcher (opens a GUI window with logs)
- `venv/` — bundled Python environment with all dependencies
- `PostgreSQL/` — embedded PostgreSQL 18 binaries
- `database.dump` — full database dump (~80,000+ games, schema + data)
- `staticfiles/` — collected static assets
- `run.py`, `igdb_site/`, `games/` — Django project
- `.env` — configuration (DB credentials, API keys)

### First launch

On the first run, GamesPeek will:

1. Initialize a local PostgreSQL instance in `%APPDATA%\IGDB_Site_PostgreSQL`
2. Restore the bundled `database.dump` (~80,000+ games)
3. Start the local server at **http://127.0.0.1:8000** and open it in your browser

The first launch takes **1–2 minutes** (database restore). Subsequent launches are fast.

### Notes

- No Redis required — the desktop build uses Django's `LocMemCache`
- To reset the local database, close the app and delete `%APPDATA%\IGDB_Site_PostgreSQL`
- The archive is self-contained; nothing is installed system-wide

---

## 🎯 Core Concept

GamesPeek solves a simple problem: **you know what games you like, but you don't know what to play next.**

Instead of endless scrolling through stores or reading reviews:

1. **Find a game** you love (via search, browsing with filters)
2. **Click "Find Similar"** on its card or detail page
3. **Get ranked recommendations** — from most similar to least similar

### Example Recommendations

| Source Game | Similar Games Found |
|-------------|---------------------|
| The Witcher 3 | Kingdom Come Deliverance, Skyrim, Dragon Age Inquisition |
| Dark Souls | Elden Ring, Sekiro, Bloodborne, Nioh |
| Stardew Valley | Animal Crossing, My Time at Portia, Coral Island |
| Hollow Knight | Ori, Dead Cells, Blasphemous |

### Two Ways to Discover Games

| Workflow | What happens |
|----------|---------------|
| **One-click similarity** | Pick any game, click "Find Similar", get ranked recommendations |
| **Manual criteria** | Build your own search with genres, keywords, themes, platforms, release year, game modes, and engines |

---

## 🔍 Features

- **80,000+ game database** covering PC, PlayStation, Xbox, Nintendo Switch, and mobile
- **Full coverage of RPG and Strategy genres** — every RPG and Strategy game is already in the database
- **Intelligent similarity algorithm** analyzing genres, keywords, themes, and more
- **Advanced search filters** by genre, platform, theme, keyword, developer, game mode, engine, and release year
- **Side-by-side game comparison** to compare ratings, features, and specs
- **Detailed game information** including descriptions, screenshots, and ratings
- **Mobile-responsive design** for gaming on any device
- **Portable Windows desktop app** — run locally with no installation

---

## 🛠️ Technology Stack

- **Backend:** Django 5.x, PostgreSQL
- **Frontend:** Bootstrap 5, Vanilla JavaScript
- **Caching:** Django LocMemCache (desktop build, no Redis required)
- **Database:** 80,000+ games from IGDB API
- **Desktop:** PyInstaller launcher, embedded PostgreSQL 18

---

## ⚙️ Requirements

- **Windows 10/11** (64-bit)
- **Disk space:** ~4–5 GB (archive + restored database)
- **RAM:** 2 GB minimum (4 GB recommended)
- **Browser:** any modern one (Chrome, Edge, Firefox, Yandex Browser)

---

## 🐛 Debug Mode

If something goes wrong, run `gamespeek.exe` and check the log in the launcher window. Logs are also saved in `logs/`
next to the `.exe`:

- `logs/gamespeek_*.log` — general launcher log
- `logs/postgresql_*.log` — PostgreSQL log

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License. Free to use, modify, and distribute with attribution.

---

## 🙏 Attribution

- **Django** — web framework
- **PostgreSQL** — database
- **IGDB API** — game data source
- **PyInstaller** — EXE build
- **Bootstrap 5** — frontend
- **Django LocMemCache** — cache in the desktop build

*GamesPeek is not affiliated with or endorsed by IGDB or Twitch.*

---

## 📬 Contact

- **Email:** gamespeek@mail.ru
- **Discord:** [Join our server](https://discord.gg/YcxwAuSv)
- **GitHub:** [AlexeyZam15/GamesPeek_site](https://github.com/AlexeyZam15/GamesPeek_site)

---
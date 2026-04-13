# 🎮 Gamespeek – Find Your Next Favorite Game

**Gamespeek** is a desktop application that runs a local game discovery website on your computer. Find similar games
using smart filters — no internet required after download.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🚀 Quick Start

1. **Download** `gamespeek.exe`
2. **Double‑click** to run it
3. **Open your browser** and go to: `http://127.0.0.1:8000`
4. **Start discovering** games!

> ⚠️ **First launch may take a while** — the database is being loaded and optimized. Please be patient. Subsequent
launches will be much faster.

> ⚠️ Keep the terminal window open while using Gamespeek. Close it to stop the local server.

## 🔍 How It Works

1. Pick a game you like (or create custom search criteria)
2. Adjust similarity filters — choose what "similar" means to you
3. Apply search filters to narrow down candidates
4. Browse results ranked by match percentage

## ⚡ One‑Click Discovery

See a game you like? Click **"Find Similar"** — Gamespeek instantly shows you the 500 most similar games using that
game's own attributes.

## 🎯 Two Filter Types

| Filter Type | What It Does |
|-------------|---------------|
| **Similarity Filters** | Affect the match percentage (e.g., "genres matter more than keywords") |
| **Search Filters** | Strictly include or exclude games (e.g., "only RPGs from 2015–2025") |

## 🗃️ Game Database

The current version includes approximately **45,000 games** from the **RPG** and **Strategy** genres across multiple
platforms — not just PC. More genres and platforms will be added in future updates.

## 💻 System Requirements

- Windows 10 or 11
- Any modern web browser (Chrome, Firefox, Edge)
- No installation required — just run the `.exe`

## 📦 What's Included

The executable contains everything:

- Django web server
- Game database (~45,000 RPGs and Strategy games)
- Similarity scoring engine
- All dependencies

## ❓ FAQ

**Q: How do I stop the server?**  
A: Close the terminal window that opened when you ran `gamespeek.exe`.

**Q: Can I bookmark the page?**  
A: Yes — `http://127.0.0.1:8000` works every time you run the app.

**Q: The page doesn't load?**  
A: Make sure the terminal window is still open, then refresh your browser.

## 💖 Support the Project

If you enjoy using Gamespeek and want to support its development:

👉 **[Support the author](https://dalink.to/wolfgunt)**

### 🚀 What Your Support Will Bring

With sufficient support, future updates will include:

- **Online hosting** — access Gamespeek from anywhere without downloading
- **Regular database updates** — new games added continuously
- **More genres and platforms** — expand beyond RPG and Strategy
- **Improved similarity algorithm** — even better recommendations
- **More filters and features** — based on your feedback

### 📢 Have Ideas?

Found a bug? Want a new feature? **Let me know what needs improvement!**

Reach out via Boosty or GitHub Issues with your suggestions. Every opinion matters.

## 🙏 Attribution

This software uses the **IGDB API** (Internet Game Database) to provide game data.

- IGDB API is used under free non-commercial terms
- Visit [IGDB.com](https://www.igdb.com) for more information

*Gamespeek is not affiliated with or endorsed by IGDB or Twitch.*

---

*Stop guessing. Start discovering.*
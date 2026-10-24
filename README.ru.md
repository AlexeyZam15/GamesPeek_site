<div align="center">

# 🎮 GamesPeek – Intelligent Game Discovery Engine

**Поиск похожих игр по базе из 80 000+ тайтлов**

[![Русский](https://img.shields.io/badge/Язык-Русский-blue)](README.ru.md)
[![English](https://img.shields.io/badge/Language-English-red)](README.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Desktop App](https://img.shields.io/badge/Скачать-Windows%20.exe-blue)](https://github.com/AlexeyZam15/GamesPeek_site/releases/latest)
[![Discord](https://img.shields.io/badge/Discord-Присоединиться-7289da)](https://discord.gg/YcxwAuSv)

</div>

---

## 📦 Скачать готовую программу

**Для пользователей Windows** доступен портативный архив с EXE-файлом — установка Python и PostgreSQL не требуется.

➡️ **[Скачать последнюю версию](https://github.com/AlexeyZam15/GamesPeek_site/releases/latest)**

1. Перейдите в раздел **Releases** на GitHub
2. Скачайте архив `gamespeek-*.zip`
3. Распакуйте в любую папку (например, `C:\GamesPeek`)
4. Запустите **`gamespeek.exe`**

---

## 💬 Присоединяйтесь к сообществу

Обсуждайте проект, задавайте вопросы и делитесь опытом в нашем Discord:

➡️ **[Присоединиться к Discord](https://discord.gg/YcxwAuSv)**

---

## 💝 Поддержать проект

Если программа вам полезна, вы можете поддержать развитие проекта:

➡️ **[Поддержать автора](https://dalink.to/wolfgunt)**

Спасибо за вашу поддержку! ❤️

---

## 🖥️ Desktop-приложение (Windows)

Портативная сборка GamesPeek — **без установки**, всё внутри архива.

### Что внутри архива

- `gamespeek.exe` — маленький загрузчик (открывает окно с логами)
- `venv/` — встроенное Python-окружение со всеми зависимостями
- `PostgreSQL/` — встроенный PostgreSQL 18
- `database.dump` — полный дамп базы (~80 000+ игр, схема + данные)
- `staticfiles/` — собранная статика
- `run.py`, `igdb_site/`, `games/` — Django-проект
- `.env` — конфигурация (доступы к БД, API-ключи)

### Первый запуск

При первом запуске GamesPeek:

1. Инициализирует локальный PostgreSQL в `%APPDATA%\IGDB_Site_PostgreSQL`
2. Восстанавливает `database.dump` (~80 000+ игр)
3. Запускает локальный сервер на **http://127.0.0.1:8000** и открывает его в браузере

Первый запуск занимает **1–2 минуты** (восстановление базы). Последующие — быстрые.

### Примечания

- Redis не нужен — используется Django `LocMemCache`
- Чтобы сбросить локальную БД, закройте приложение и удалите `%APPDATA%\IGDB_Site_PostgreSQL`
- Архив самодостаточный, ничего не устанавливается в систему

---

## 🎯 Как это работает

GamesPeek решает простую проблему: **вы знаете, какие игры вам нравятся, но не знаете, во что играть дальше.**

Вместо бесконечного листания магазинов и чтения обзоров:

1. **Найдите игру**, которая вам нравится (поиск, фильтры)
2. **Нажмите «Find Similar»** на карточке или странице игры
3. **Получите ранжированные рекомендации** — от самых похожих к менее похожим

### Примеры рекомендаций

| Исходная игра | Похожие игры |
|---------------|--------------|
| The Witcher 3 | Kingdom Come Deliverance, Skyrim, Dragon Age Inquisition |
| Dark Souls | Elden Ring, Sekiro, Bloodborne, Nioh |
| Stardew Valley | Animal Crossing, My Time at Portia, Coral Island |
| Hollow Knight | Ori, Dead Cells, Blasphemous |

### Два способа поиска

| Способ | Что происходит |
|--------|----------------|
| **Похожесть в один клик** | Выбираете игру, жмёте «Find Similar» — получаете ранжированные рекомендации |
| **Ручные критерии** | Строите свой поиск по жанрам, ключевым словам, темам, платформам, году выхода, режимам и движкам |

---

## 🔍 Возможности

- **База 80 000+ игр** — PC, PlayStation, Xbox, Nintendo Switch, мобильные
- **Полное покрытие жанров RPG и стратегий** — все игры этих жанров уже в базе
- **Умный алгоритм похожести** — анализирует жанры, ключевые слова, темы и другое
- **Продвинутые фильтры** — по жанру, платформе, теме, ключевому слову, разработчику, режиму, движку и году выхода
- **Сравнение игр бок о бок** — рейтинги, особенности, характеристики
- **Подробная информация об играх** — описания, скриншоты, рейтинги
- **Адаптивный дизайн** — удобно с любого устройства
- **Портативное Windows-приложение** — запуск локально без установки

---

## 🛠️ Технологии

- **Backend:** Django 5.x, PostgreSQL
- **Frontend:** Bootstrap 5, Vanilla JavaScript
- **Кэш:** Django LocMemCache (desktop-сборка, Redis не требуется)
- **База:** 80 000+ игр из IGDB API
- **Desktop:** PyInstaller, встроенный PostgreSQL 18

---

## ⚙️ Требования

- **Windows 10/11** (64-bit)
- **Свободное место:** ~4–5 ГБ (архив + распакованная база)
- **Оперативная память:** 2 ГБ минимум (4 ГБ рекомендуется)
- **Браузер** — любой современный (Chrome, Edge, Firefox, Яндекс.Браузер)

---

## 🐛 Режим отладки

Если что-то пошло не так, запустите `gamespeek.exe` и посмотрите лог в окне лаунчера. Логи также сохраняются в `logs/`
рядом с `.exe`:

- `logs/gamespeek_*.log` — общий лог лаунчера
- `logs/postgresql_*.log` — лог PostgreSQL

---

## 🤝 Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для новой функции (`git checkout -b feature/amazing-feature`)
3. Закоммитьте изменения (`git commit -m 'Add amazing feature'`)
4. Запушьте ветку (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

---

## 📄 Лицензия

Проект распространяется под лицензией MIT. Это означает свободное использование, модификацию и распространение с
указанием авторства.

---

## 🙏 Благодарности

- **Django** — веб-фреймворк
- **PostgreSQL** — база данных
- **IGDB API** — источник данных об играх
- **PyInstaller** — сборка EXE
- **Bootstrap 5** — фронтенд
- **Django LocMemCache** — кэш в desktop-сборке

*GamesPeek не аффилирован с IGDB или Twitch и не одобрен ими.*

---

## 📬 Контакты

- **Email:** gamespeek@mail.ru
- **Discord:** [Присоединиться к серверу](https://discord.gg/YcxwAuSv)
- **GitHub:** [AlexeyZam15/GamesPeek_site](https://github.com/AlexeyZam15/GamesPeek_site)

---
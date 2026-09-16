# 📁 Solo Topic Bot

A **private, personal media-relay vault bot** for Telegram, organized with
**Topics in private chats** (Bot API 9.3 / 9.4). One admin, one bot (or several),
zero public surface.

> ⚠️ This folder is a **frozen, tested build** (code v9). New experiments belong
> in separate folders (e.g. `Group Topic Bot`) — never modify this one.

---

## ✨ Features

- **Relay to DM topics** — send/forward media into a topic → bot returns a clean
  copy (no forward tag) in the same topic and deletes your original.
- **One caption per album** — first item carries `original caption + ────────── + 📁 #topic_name`.
- **All Messages picker** — drop media in *All Messages* → inline buttons let you
  choose the destination topic (no accidents).
- **Duplicate vault** — SHA-256 of `file_unique_id` in SQLite; duplicates are
  counted and blocked (toggleable).
- **Legacy migration** — an old `media.db` is drained read+delete-only:
  forwarded old media is re-filed into topics, history (`created_at`, `bot_name`)
  is carried over, old row deleted.
- **Smart grouping** — small albums merged into albums of 10; big albums (6+)
  pass through as-is; per-topic queues.
- **Multi-bot in one process** — list several tokens; each bot gets its own DM
  vault & topics; duplicate vault is shared across bots.
- **Safety** — admin-only (silent ignore for others), flood-limit backoff
  everywhere, single-instance lock file, graceful shutdown, WAL mode,
  no token leaks in logs.

## 📦 Requirements

- Python 3.8+
- `pip install python-telegram-bot aiosqlite`

## 🔧 Setup

1. **@BotFather** → your bot → *Bot Settings* → **Threads Settings** →
   `Threaded Mode` **ON** (recommended: also *disallow users to create new threads*).
2. Copy `example_config.py` → `config.py`, fill in your **token(s)** and **ADMIN_ID**.
3. Place your old `media.db` next to `bot.py` (optional — for migration).
4. `python bot.py`
5. Open the bot's DM → `/start` → tap the bot name at top → enable **Topics**.
6. `/use maria` (creates topic) → forward media → enjoy.

## ⚙️ Commands

| Category | Commands |
|---|---|
| Relay & Topics | `/relay` `/bind <t>` `/unbind` `/use <t>` `/topics` `/tdel <t>` `/trename <old> <new>` |
| Migration | `/migrate on\|off\|stats` |
| Behavior | `/gp` `/autodelete` `/addcaption <txt>` `/removecaption` `/db` |
| Vault | `/dbstats` `/dbclear` `/dbfind` `/dbdel` |
| Info | `/help` (button menu) `/settings` |

**Notes**
- `/bind` = permanent per-chat routing (for source groups). `/use` = temporary
  manual switch. In *All Messages* the picker buttons decide.
- `/tdel` archives a topic safely — the Telegram topic and its media stay.
- `/dbfind` / `/dbdel`: reply to media = instant; alone (or replied to text) =
  arm mode, then forward within 60 s.

## 🗂 Repository layout

```
Solo Topic Bot/
├── bot.py             # frozen tested build (v9)
├── example_config.py  # safe config template
├── README.md
└── .gitignore         # ignores config.py, *.db*, logs, lock, __pycache__
```

**Never commit:** `config.py` (tokens!), any `*.db*` files, `*.log`,
`vault_bot.lock`.

## 🛡 Privacy / safety model

- Admin-only; strangers silently ignored → nothing to report, nothing leaked.
- All data (hashes, topics, bindings, logs) stays **on your device**.
- Only official Bot API methods; polite rate handling → no ban risk from mechanics.
- Run in **one place only** (lock file enforces it).

## 🔮 Roadmap (separate folders)

- `Group Topic Bot/` — shared-with-friends variant with group-topic features
  (planned; will not touch this folder).

---

*Use responsibly. Content responsibility is always yours.*

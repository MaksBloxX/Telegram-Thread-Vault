import asyncio
import config
import time
import hashlib
import logging
import os
import signal
import sys
import aiosqlite
from telegram import Update, InputMediaPhoto, InputMediaVideo, InputMediaDocument, \
    InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, \
    filters, ContextTypes
from telegram.error import RetryAfter, TelegramError, BadRequest, Forbidden

# --- WINDOWS CONSOLE FIX ---
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("vault_bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# --- GLOBAL STATE ---
CODE_VERSION = "v10 (2026-09-16)"
LOCK_FILE = "vault_bot.lock"

def acquire_lock():
    """Refuse to start if another instance is already running (prevents the
    two-process update-stealing chaos)."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE) as f:
                old_pid = int(f.read().strip())
            os.kill(old_pid, 0)  # raises ProcessLookupError if dead
            log.error(f"Another bot instance (pid {old_pid}) is already running! "
                      f"Stop it first: pkill -f bot.py")
            sys.exit(1)
        except (ValueError, ProcessLookupError, OSError):
            pass  # stale lock — previous process died
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))

def release_lock():
    try:
        with open(LOCK_FILE) as f:
            if int(f.read().strip()) == os.getpid():
                os.remove(LOCK_FILE)
    except (OSError, ValueError):
        pass

bot_states = {}
_db_conn: aiosqlite.Connection = None   # NEW db (media_new.db)
_old_conn: aiosqlite.Connection = None  # OLD db (media.db) — drain-only, never written
MIGRATE = {"on": True}
TOKEN_LABELS = {}
TOPIC_CACHE = {}     # (bot_id, name) -> thread_id
REV_CACHE = {}       # (bot_id, thread_id) -> name
BIND_CACHE = {}      # (bot_id, chat_id) -> topic name
CACHE_LOADED = False
_topic_lock = None

# Valid Telegram topic icon colors (rotated per topic)
ICON_COLORS = [7322096, 9367192, 16766590, 13338331, 15840869]

# --- GP SMART GROUPING THRESHOLD ---
GP_ALBUM_SKIP_MIN = 6

# --- ARMED INSPECT MODE (shared by ALL bots) ---
INSPECT = {
    "mode": None,
    "task": None,
    "responder": None,
    "grace_until": 0.0,
    "albums": {},
}
INSPECT_TIMEOUT = 60.0
INSPECT_GRACE   = 5.0

def get_lock():
    global _topic_lock
    if _topic_lock is None:
        _topic_lock = asyncio.Lock()
    return _topic_lock

class QueueState:
    def __init__(self):
        self.media, self.message_ids = [], []
        self.timer_task = None
        self.chat_id = None          # SOURCE chat (status msgs + delete originals)
        self.dest_chat = None        # relay destination (ADMIN dm or source)
        self.thread = None           # destination topic thread id (or None=General)
        self.topic_name = None       # for #tag captions
        self.processing_msg_ids = []

def get_state(bot_id):
    if bot_id not in bot_states:
        bot_states[bot_id] = {
            "settings": config.DEFAULT_SETTINGS.copy(),
            "queues": {},               # (source_chat, thread) -> QueueState
            "off_mode_albums": {},
            "gp_albums": {},
            "album_lock": asyncio.Lock(),
            "db_enabled": config.DEFAULT_SETTINGS.get("db_check", True),
            "last_warn": 0.0,
            "manual_topic": None,       # /use <topic>
            "dm_retry_at": 0.0,         # cooldown after DM/topic creation failure
            "chooser": None,            # All Messages topic picker pending media
        }
    return bot_states[bot_id]

# --- SECURITY FILTER: only ADMIN_ID ---
class AdminFilter(filters.MessageFilter):
    def filter(self, message):
        return message.from_user and message.from_user.id == config.ADMIN_ID

admin_filter = AdminFilter()

# --- BOT LABELS ---
def bot_label(bot) -> str:
    return TOKEN_LABELS.get(bot.token, "Unknown")

def is_announcer(bot) -> bool:
    return bool(config.BOT_TOKENS) and bot.token == config.BOT_TOKENS[0]

# ================= DATABASE (NEW) =================
async def init_db():
    global _db_conn
    _db_conn = await aiosqlite.connect(config.NEW_DB_NAME)
    await _db_conn.execute("PRAGMA journal_mode=WAL")
    await _db_conn.execute("PRAGMA synchronous=NORMAL")
    await _db_conn.execute("PRAGMA busy_timeout=5000")
    await _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS media_vault (
            file_hash TEXT PRIMARY KEY,
            bot_name TEXT,
            created_at REAL,
            duplicate_count INTEGER NOT NULL DEFAULT 0,
            last_duplicate_at REAL
        )
    """)
    await _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            bot_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            thread_id INTEGER NOT NULL,
            icon_color INTEGER,
            created_at REAL,
            PRIMARY KEY (bot_id, name)
        )
    """)
    await _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS bindings (
            bot_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            topic_name TEXT NOT NULL,
            PRIMARY KEY (bot_id, chat_id)
        )
    """)
    await _db_conn.commit()
    log.info("New database initialized successfully.")

async def init_old_db():
    """Open legacy media.db READ+DELETE only. Missing file => migration off."""
    global _old_conn
    if not getattr(config, "OLD_DB_NAME", ""):
        MIGRATE["on"] = False
        return
    try:
        conn = await aiosqlite.connect(config.OLD_DB_NAME)
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='media_vault'")
        if not await cur.fetchone():
            await conn.close()
            log.warning("Old DB has no media_vault table — migration disabled.")
            MIGRATE["on"] = False
            return
        _old_conn = conn
        cur = await _old_conn.execute("SELECT COUNT(*) FROM media_vault")
        log.info(f"Old DB opened (drain-only). Rows remaining: {(await cur.fetchone())[0]}")
    except Exception as e:
        log.warning(f"Old DB not opened ({e}) — migration disabled.")
        _old_conn = None
    MIGRATE["on"] = bool(_old_conn) and config.MIGRATE_DEFAULT

async def close_db():
    global _db_conn, _old_conn
    if _db_conn:
        await _db_conn.close()
    if _old_conn:
        await _old_conn.close()
    log.info("Databases closed cleanly.")

async def db_fetchone(sql, params=()):
    cursor = await _db_conn.execute(sql, params)
    return await cursor.fetchone()

async def db_fetchall(sql, params=()):
    cursor = await _db_conn.execute(sql, params)
    return await cursor.fetchall()

async def old_fetch(f_hash):
    if not _old_conn:
        return None
    cursor = await _old_conn.execute(
        "SELECT bot_name, created_at FROM media_vault WHERE file_hash = ?", (f_hash,))
    return await cursor.fetchone()

async def old_delete(f_hash):
    if not _old_conn:
        return
    await _old_conn.execute("DELETE FROM media_vault WHERE file_hash = ?", (f_hash,))
    await _old_conn.commit()

def fmt_time(ts):
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts)) if ts else "—"

def short_hash(h):
    return f"{h[:12]}…" if h else "—"

DIVIDER = "─" * 10

def tag_line(name):
    """Clickable tag, e.g. '📁 #maria'."""
    if not name:
        return None
    t = name.strip().replace(" ", "_")
    t = "".join(c for c in t if c.isalnum() or c == "_")
    return f"📁 #{t}" if t else None

def compose_str(head, tag):
    """Style B: caption + divider + tag."""
    if head and tag:
        return f"{head}\n{DIVIDER}\n{tag}"
    return head or tag

def cap_parts(base, base_ents, custom, tag):
    """Returns (caption, entities, parse_mode)."""
    if custom:
        return compose_str(custom, tag), None, "HTML"
    head = base or ""
    return compose_str(head, tag), (base_ents if head else None), None

def captioned_obj(msg, custom, tag):
    cap, ent, pm = cap_parts(msg.caption, msg.caption_entities, custom, tag)
    return get_media_obj(msg, cap, ent, pm)

def captioned_input(mobj, custom, tag):
    cap, ent, pm = cap_parts(mobj.caption, mobj.caption_entities, custom, tag)
    kw = {"media": mobj.media, "caption": cap}
    if pm:
        kw["parse_mode"] = pm
    elif ent:
        kw["caption_entities"] = ent
    return type(mobj)(**kw)

def album_first_caption(items):
    """Rescue the group's caption: first non-empty caption among items."""
    for it in items:
        if it.caption:
            return it.caption, it.caption_entities
    if items:
        return items[0].caption, items[0].caption_entities
    return None, None

def first_obj(msg, custom, tag, base_cap, base_ents):
    """First item of a returned album: one caption = title + divider + tag."""
    cap, ent, pm = cap_parts(base_cap, base_ents, custom, tag)
    return get_media_obj(msg, cap, ent, pm)

def first_input(mobj, custom, tag, base_cap, base_ents):
    cap, ent, pm = cap_parts(base_cap, base_ents, custom, tag)
    kw = {"media": mobj.media, "caption": cap}
    if pm:
        kw["parse_mode"] = pm
    elif ent:
        kw["caption_entities"] = ent
    return type(mobj)(**kw)

def hash_id(unique_id: str) -> str:
    return hashlib.sha256(unique_id.encode()).hexdigest()

def get_unique_id(msg):
    if msg.photo: return msg.photo[-1].file_unique_id
    if msg.video: return msg.video.file_unique_id
    if msg.document: return msg.document.file_unique_id
    if msg.animation: return msg.animation.file_unique_id
    return None

# ================= TOPICS & BINDINGS =================
async def ensure_cache():
    global CACHE_LOADED
    if CACHE_LOADED:
        return
    for b, n, t in await db_fetchall("SELECT bot_id, name, thread_id FROM topics"):
        TOPIC_CACHE[(b, n)] = t
        REV_CACHE[(b, t)] = n
    for b, c, n in await db_fetchall("SELECT bot_id, chat_id, topic_name FROM bindings"):
        BIND_CACHE[(b, c)] = n
    CACHE_LOADED = True

def binding_for(bot_id, chat_id):
    return BIND_CACHE.get((bot_id, chat_id))

async def topic_thread(bot, state, name):
    """thread_id for a topic name; creates the topic in the admin DM if needed."""
    if not name:
        return None
    await ensure_cache()
    key = (bot.id, name)
    if key in TOPIC_CACHE:
        return TOPIC_CACHE[key]
    now = time.time()
    if now < state["dm_retry_at"]:
        return None  # DM not ready yet — fall back to General
    async with get_lock():
        if key in TOPIC_CACHE:
            return TOPIC_CACHE[key]
        try:
            color = ICON_COLORS[len(TOPIC_CACHE) % len(ICON_COLORS)]
            ft = await bot.create_forum_topic(
                chat_id=config.ADMIN_ID, name=name, icon_color=color)
            thread = ft.message_thread_id
            await _db_conn.execute(
                "INSERT OR REPLACE INTO topics (bot_id, name, thread_id, icon_color, created_at) "
                "VALUES (?,?,?,?,?)", (bot.id, name, thread, color, time.time()))
            await _db_conn.commit()
            TOPIC_CACHE[key] = thread
            REV_CACHE[(bot.id, thread)] = name
            log.info(f"Topic created: {name} -> thread {thread}")
            return thread
        except (Forbidden, BadRequest, RetryAfter) as e:
            log.warning(f"Cannot create topic '{name}' ({e}) — enable Threaded Mode in "
                        f"BotFather + the Topics toggle inside the bot DM.")
            state["dm_retry_at"] = time.time() + 300
            return None

async def dm_warn_once(bot, state, chat_id):
    if state.get("dm_warned"):
        return
    state["dm_warned"] = True
    try:
        m = await bot.send_message(
            chat_id, "⚠️ DM vault not reachable — /start this bot, enable Threaded Mode "
                     "in BotFather (Threads Settings) and the Topics toggle in this DM.")
        asyncio.create_task(delete_msg(m, 10))
    except TelegramError:
        pass

# ================= ALL-MESSAGES TOPIC PICKER =================
async def show_chooser(context, state):
    """After batch delay, ask (buttons) which topic the All-Messages media goes to."""
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        ch = state.get("chooser")
        if not ch or not ch["m"] or ch["msg"]:
            return
        names = sorted({n for (b, n) in TOPIC_CACHE if b == context.bot.id})
        ch["map"] = [(TOPIC_CACHE[(context.bot.id, n)], n) for n in names]
        kb = [[InlineKeyboardButton(n, callback_data=f"pick:{i}")]
              for i, n in enumerate(names)]
        kb.append([InlineKeyboardButton("🏠 Stay here", callback_data="pick:-1")])
        m = await context.bot.send_message(
            config.ADMIN_ID, f"📥 {len(ch['m'])} media — choose topic:",
            reply_markup=InlineKeyboardMarkup(kb))
        ch["msg"] = m.message_id
        ch["task"] = asyncio.create_task(chooser_timeout(context, state))
    except asyncio.CancelledError:
        pass
    except TelegramError as e:
        log.error(f"chooser show failed: {e}")

async def chooser_timeout(context, state):
    try:
        await asyncio.sleep(60.0)
        ch = state.get("chooser")
        if ch and ch["m"]:
            if ch["msg"]:
                try:
                    await context.bot.delete_message(config.ADMIN_ID, ch["msg"])
                except TelegramError:
                    pass
            state["chooser"] = None
            m = await context.bot.send_message(
                config.ADMIN_ID, "⌛ No topic chosen — left as is.")
            asyncio.create_task(delete_msg(m, 5))
    except asyncio.CancelledError:
        pass

async def pick_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or q.from_user.id != config.ADMIN_ID:
        return  # silent for strangers
    try:
        await q.answer()
    except TelegramError:
        pass
    state = get_state(context.bot.id)
    ch = state.get("chooser")
    if not ch or not ch["m"]:
        state["chooser"] = None
        try:
            await q.message.delete()
        except TelegramError:
            pass
        return
    try:
        idx = int(q.data.split(":")[1])
    except (ValueError, IndexError):
        return
    thread, name = ch["map"][idx] if idx >= 0 else (None, None)
    msgs, ids = ch["m"][:], ch["ids"][:]
    if ch["msg"]:
        try:
            await context.bot.delete_message(config.ADMIN_ID, ch["msg"])
        except TelegramError:
            pass
    state["chooser"] = None

    line = tag_line(name)
    custom = state["settings"]["custom_caption"]
    base_cap, base_ents = album_first_caption(msgs)
    final = []
    for i, m in enumerate(msgs):
        obj = first_obj(m, custom, line, base_cap, base_ents) if i == 0 \
            else get_media_obj(m, None)
        if obj:
            final.append(obj)
    v = [m for m in final if not isinstance(m, InputMediaDocument)]
    d = [m for m in final if isinstance(m, InputMediaDocument)]
    for chunk in [v[i:i+10] for i in range(0, len(v), 10)] + \
                 [d[i:i+10] for i in range(0, len(d), 10)]:
        if chunk:
            await safe_send_media_group(context.bot, config.ADMIN_ID, chunk, thread or None)
            await asyncio.sleep(config.QUEUE_COOLDOWN)
    if state["settings"]["autodelete"]:
        await safe_delete_messages(context.bot, config.ADMIN_ID, ids)

# ================= SAFE HELPERS =================
async def delete_msg(msg, delay=0):
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        await msg.delete()
    except RetryAfter as e:
        log.warning(f"Rate limit on delete — waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            await msg.delete()
        except TelegramError as e2:
            log.debug(f"Delete retry failed (likely already deleted): {e2}")
    except TelegramError as e:
        log.debug(f"Delete failed (likely already deleted): {e}")

def get_media_obj(msg, cap=None, cap_entities=None, parse_mode=None):
    kwargs = {"caption": cap}
    if cap_entities and not parse_mode:
        kwargs["caption_entities"] = cap_entities
    elif parse_mode:
        kwargs["parse_mode"] = parse_mode
    if msg.photo: return InputMediaPhoto(msg.photo[-1].file_id, **kwargs)
    if msg.video: return InputMediaVideo(msg.video.file_id, **kwargs)
    if msg.document: return InputMediaDocument(msg.document.file_id, **kwargs)
    return None

async def safe_send_media_group(bot, chat_id, media, thread=None):
    kwargs = {"chat_id": chat_id, "media": media}
    if thread:
        kwargs["message_thread_id"] = thread
    try:
        await bot.send_media_group(**kwargs)
        return True
    except RetryAfter as e:
        log.warning(f"FloodWait on send_media_group — waiting {e.retry_after}s")
        await asyncio.sleep(e.retry_after + 1.5)
        try:
            await bot.send_media_group(**kwargs)
            return True
        except TelegramError as e2:
            log.error(f"send_media_group failed after retry: {e2}")
            return False
    except TelegramError as e:
        log.error(f"send_media_group failed: {e}")
        return False

async def safe_delete_messages(bot, chat_id, ids):
    for i in range(0, len(ids), config.MAX_DELETE_CHUNK):
        chunk = ids[i:i + config.MAX_DELETE_CHUNK]
        try:
            await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
        except RetryAfter as e:
            log.warning(f"FloodWait on delete_messages — waiting {e.retry_after}s")
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                await bot.delete_messages(chat_id=chat_id, message_ids=chunk)
            except TelegramError as e2:
                log.debug(f"Bulk delete retry failed: {e2}")
        except TelegramError as e:
            log.debug(f"Bulk delete failed: {e}")

# ================= ADMIN COMMANDS =================
HELP_OVERVIEW = (
    " *VAULT HELP*\n\n"
    "📡 Relay & Topics\n"
    "🔄 Migration\n"
    "⚙️ Behavior\n"
    "🗄 Vault Tools\n\n"
    "Tap a category 👇"
)

HELP_CATS = {
    "relay": (
        "📡 *RELAY & TOPICS*\n"
        "/relay – ON: media → DM topics • OFF: old mode\n"
        "/bind <t> – this chat → topic (saved)\n"
        "/unbind – remove this chat's bind\n"
        "/use <t> – temporary topic (alone = clear)\n"
        "/topics – list topics & binds\n"
        "/tdel <t> – archive topic, media stays\n"
        "/trename <old> <new> – rename topic (registry + title)\n"
        "💡 All Messages: buttons pick the topic"
    ),
    "migrate": (
        "🔄 *MIGRATION*\n"
        "/migrate stats – new vs old count\n"
        "/migrate on|off – pause / resume drain\n"
        "Old = 0 → set OLD_DB_NAME=\"\" in config"
    ),
    "behavior": (
        "⚙️ *BEHAVIOR*\n"
        "/gp – grouping on/off\n"
        "/autodelete – delete original after relay\n"
        "/addcaption <txt> – custom caption\n"
        "/removecaption – back to originals\n"
        "/db – duplicate blocking on/off"
    ),
    "vault": (
        "🗄 *VAULT TOOLS*\n"
        "/dbstats – full report\n"
        "/dbclear – reset counters only\n"
        "/dbfind – lookup (reply or forward)\n"
        "/dbdel – remove hash (reply or forward)\n"
        "ℹ️ /settings – status + version"
    ),
}

def help_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📡 Relay & Topics", callback_data="help:relay"),
         InlineKeyboardButton("🔄 Migration", callback_data="help:migrate")],
        [InlineKeyboardButton("⚙️ Behavior", callback_data="help:behavior"),
         InlineKeyboardButton("🗄 Vault", callback_data="help:vault")],
    ])

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    m = await update.message.reply_text("🚀 **Vault Active**.", parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 5))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    m = await update.message.reply_text(HELP_OVERVIEW, parse_mode="Markdown",
                                        reply_markup=help_kb())
    asyncio.create_task(delete_msg(m, 60))

async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or q.from_user.id != config.ADMIN_ID:
        return
    try:
        await q.answer()
    except TelegramError:
        pass
    key = q.data.split(":", 1)[1]
    if key == "main":
        text, kb = HELP_OVERVIEW, help_kb()
    else:
        text = HELP_CATS.get(key, HELP_OVERVIEW)
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Back", callback_data="help:main")]])
    try:
        await q.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
    except TelegramError:
        pass

async def relay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["relay"] = not state["settings"]["relay"]
    status = "ON" if state["settings"]["relay"] else "OFF (in-place mode)"
    m = await update.message.reply_text(f"Relay to DM topics: **{status}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def bind_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    name = update.message.text.replace("/bind", "", 1).strip()
    if not name:
        cur = binding_for(context.bot.id, update.message.chat_id)
        m = await update.message.reply_text(
            f"🔗 Bound here: `{cur}`" if cur else "🔗 This chat is not bound. Use /bind <topic>")
        asyncio.create_task(delete_msg(m, 8))
        return
    thread = await topic_thread(context.bot, state, name)
    if thread is None:
        m = await update.message.reply_text(
            "⚠️ **Chat is not a forum yet.** Fix (2 steps):\n"
            "1. @BotFather → your bot → Bot Settings → **Threads Settings** → Threaded Mode **ON**\n"
            "2. In this bot's DM: tap the bot name on top → enable **Topics**\n"
            "Then retry.", parse_mode='Markdown')
        asyncio.create_task(delete_msg(m, 20))
        return
    await ensure_cache()
    await _db_conn.execute(
        "INSERT OR REPLACE INTO bindings (bot_id, chat_id, topic_name) VALUES (?,?,?)",
        (context.bot.id, update.message.chat_id, name))
    await _db_conn.commit()
    BIND_CACHE[(context.bot.id, update.message.chat_id)] = name
    m = await update.message.reply_text(f"✅ This chat → topic **{name}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def unbind_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    await ensure_cache()
    await _db_conn.execute(
        "DELETE FROM bindings WHERE bot_id=? AND chat_id=?",
        (context.bot.id, update.message.chat_id))
    await _db_conn.commit()
    BIND_CACHE.pop((context.bot.id, update.message.chat_id), None)
    m = await update.message.reply_text("🔓 Binding removed.")
    asyncio.create_task(delete_msg(m, 5))

async def use_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    name = update.message.text.replace("/use", "", 1).strip()
    if not name:
        state["manual_topic"] = None
        m = await update.message.reply_text("🧭 Manual topic cleared.")
        asyncio.create_task(delete_msg(m, 5))
        return
    thread = await topic_thread(context.bot, state, name)
    if thread is None:
        m = await update.message.reply_text(
            "⚠️ Chat is not a forum yet — enable Threaded Mode in BotFather "
            "(Bot Settings → Threads Settings) and the Topics toggle in this DM.")
        asyncio.create_task(delete_msg(m, 12))
        return
    state["manual_topic"] = name
    m = await update.message.reply_text(f"🧭 Manual topic: **{name}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def topics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    await ensure_cache()
    tlines = [f"• {n} — thread `{t}`" for (b, n), t in sorted(TOPIC_CACHE.items())
              if b == context.bot.id]
    blines = [f"• chat `{c}` → {n}" for (b, c), n in sorted(BIND_CACHE.items())
              if b == context.bot.id]
    text = "📚 **Topics**:\n" + ("\n".join(tlines) if tlines else "(none yet)")
    text += "\n\n🔗 **Bindings**:\n" + ("\n".join(blines) if blines else "(none)")
    m = await update.message.reply_text(text, parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 20))

async def trename_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Rename a topic: registry + Telegram title together, always in sync."""
    asyncio.create_task(delete_msg(update.message))
    parts = update.message.text.replace("/trename", "", 1).strip().split()
    if len(parts) < 2:
        m = await update.message.reply_text("Usage: /trename <old> <new>")
        asyncio.create_task(delete_msg(m, 8))
        return
    old, new = parts[0], " ".join(parts[1:])
    await ensure_cache()
    th = TOPIC_CACHE.get((context.bot.id, old))
    if th is None:
        m = await update.message.reply_text(f"❌ Topic '{old}' not found.")
        asyncio.create_task(delete_msg(m, 8))
        return
    if (context.bot.id, new) in TOPIC_CACHE:
        m = await update.message.reply_text(f"❌ A topic named '{new}' already exists.")
        asyncio.create_task(delete_msg(m, 8))
        return
    try:
        await context.bot.edit_forum_topic(
            chat_id=config.ADMIN_ID, message_thread_id=th, name=new)
    except TelegramError as e:
        log.warning(f"edit_forum_topic failed (registry still renamed): {e}")
    await _db_conn.execute(
        "UPDATE topics SET name=? WHERE bot_id=? AND name=?", (new, context.bot.id, old))
    await _db_conn.execute(
        "UPDATE bindings SET topic_name=? WHERE bot_id=? AND topic_name=?",
        (new, context.bot.id, old))
    await _db_conn.commit()
    TOPIC_CACHE.pop((context.bot.id, old), None)
    TOPIC_CACHE[(context.bot.id, new)] = th
    REV_CACHE[(context.bot.id, th)] = new
    for k in [k for k, v in BIND_CACHE.items() if k[0] == context.bot.id and v == old]:
        BIND_CACHE[k] = new
    st = get_state(context.bot.id)
    if st["manual_topic"] == old:
        st["manual_topic"] = new
    m = await update.message.reply_text(f"✅ Renamed: **{old}** → **{new}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def tdel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Archive a topic: bot forgets it; the Telegram topic + media stay untouched.
    (Real deleteForumTopic would ERASE all media inside — never used.)"""
    asyncio.create_task(delete_msg(update.message))
    name = update.message.text.replace("/tdel", "", 1).strip()
    if not name:
        m = await update.message.reply_text("Usage: /tdel <topic name>")
        asyncio.create_task(delete_msg(m, 8))
        return
    await ensure_cache()
    th = TOPIC_CACHE.get((context.bot.id, name))
    await _db_conn.execute(
        "DELETE FROM topics WHERE bot_id=? AND name=?", (context.bot.id, name))
    await _db_conn.execute(
        "DELETE FROM bindings WHERE bot_id=? AND topic_name=?", (context.bot.id, name))
    await _db_conn.commit()
    TOPIC_CACHE.pop((context.bot.id, name), None)
    if th is not None:
        REV_CACHE.pop((context.bot.id, th), None)
    for k in [k for k, v in BIND_CACHE.items() if k[0] == context.bot.id and v == name]:
        BIND_CACHE.pop(k, None)
    m = await update.message.reply_text(
        f"📦 **{name}** archived — bot forgot it.\n"
        "The Telegram topic and all its media remain untouched.", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 10))

async def migrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    arg = update.message.text.replace("/migrate", "", 1).strip().lower()
    if arg == "stats":
        new_c = (await db_fetchone("SELECT COUNT(*) FROM media_vault"))[0]
        old_c = 0
        if _old_conn:
            cur = await _old_conn.execute("SELECT COUNT(*) FROM media_vault")
            old_c = (await cur.fetchone())[0]
        m = await update.message.reply_text(
            f"📊 New vault: `{new_c}`\n⏳ Old db remaining: `{old_c}`\n"
            f"🔁 Migrate: `{'ON' if MIGRATE['on'] else 'OFF'}`", parse_mode="Markdown")
        asyncio.create_task(delete_msg(m, 15))
        return
    if arg in ("on", "off"):
        if arg == "on" and not _old_conn:
            m = await update.message.reply_text("⚠️ No old DB open.")
        else:
            MIGRATE["on"] = (arg == "on")
            m = await update.message.reply_text(f"🔁 Migrate: **{arg.upper()}**", parse_mode='Markdown')
        asyncio.create_task(delete_msg(m, 5))
        return
    m = await update.message.reply_text("Usage: /migrate on | off | stats")
    asyncio.create_task(delete_msg(m, 8))

async def gp_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["auto_group"] = not state["settings"]["auto_group"]
    status = "ON" if state["settings"]["auto_group"] else "OFF"
    m = await update.message.reply_text(f"Auto-grouper: **{status}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def autodelete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["autodelete"] = not state["settings"]["autodelete"]
    status = "ON" if state["settings"]["autodelete"] else "OFF"
    m = await update.message.reply_text(f"Auto-delete: **{status}**", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["db_enabled"] = not state["db_enabled"]
    status = "ON" if state["db_enabled"] else "OFF"
    note = "" if state["db_enabled"] else " (still recording stats)"
    m = await update.message.reply_text(f"Duplicate-check: **{status}**{note}", parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 5))

async def dbstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    try:
        total_files = (await db_fetchone("SELECT COUNT(*) FROM media_vault"))[0]
        total_dups = (await db_fetchone(
            "SELECT COALESCE(SUM(duplicate_count), 0) FROM media_vault"))[0]
        per_bot = await db_fetchall("""
            SELECT COALESCE(bot_name, 'Unknown'), COUNT(*), COALESCE(SUM(duplicate_count), 0)
            FROM media_vault GROUP BY COALESCE(bot_name, 'Unknown') ORDER BY COUNT(*) DESC""")
        top_dup = await db_fetchone("""
            SELECT file_hash, duplicate_count FROM media_vault
            WHERE duplicate_count > 0 ORDER BY duplicate_count DESC, last_duplicate_at DESC
            LIMIT 1""")
        recent = await db_fetchall("""
            SELECT file_hash, COALESCE(bot_name, 'Unknown'), created_at
            FROM media_vault WHERE created_at IS NOT NULL
            ORDER BY created_at DESC LIMIT 3""")
    except Exception as e:
        log.error(f"/dbstats query failed: {e}")
        m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
        asyncio.create_task(delete_msg(m, 8))
        return

    lines = ["📊 **DATABASE REPORT**", f"📦 Files saved: `{total_files}`",
             f"🚫 Duplicates blocked: `{total_dups}`", "", "🤖 **Per-bot:**"]
    for name, files, dups in per_bot:
        lines.append(f"• {name} — `{files}` files / `{dups}` dups")
    if top_dup:
        lines += ["", f"🔁 Most duplicated: `{short_hash(top_dup[0])}` — **{top_dup[1]}×**"]
    if recent:
        lines += ["", "🕒 **Latest 3:**"]
        for h, bn, ts in recent:
            lines.append(f"• `{short_hash(h)}` — {bn} — {fmt_time(ts)}")
    if _old_conn:
        cur = await _old_conn.execute("SELECT COUNT(*) FROM media_vault")
        lines += ["", f"⏳ **Old db remaining:** `{(await cur.fetchone())[0]}`"]

    m = await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 60))

async def dbclear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    try:
        await _db_conn.execute(
            "UPDATE media_vault SET duplicate_count = 0, last_duplicate_at = NULL")
        await _db_conn.commit()
        m = await update.message.reply_text("🧹 Stats reset. Hash vault untouched — blocking still active.")
    except Exception as e:
        log.error(f"/dbclear failed: {e}")
        m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
    asyncio.create_task(delete_msg(m, 8))

# --- INSPECT MODE (/dbfind /dbdel) ---
async def inspect_lookup(uid):
    f_hash = hash_id(uid)
    row = await db_fetchone(
        "SELECT bot_name, created_at, duplicate_count, last_duplicate_at "
        "FROM media_vault WHERE file_hash = ?", (f_hash,))
    return f_hash, row

def format_find_text(uid, f_hash, row):
    if row:
        bot_name, created_at, dup_count, last_dup = row
        return (f"🔍 **FOUND IN VAULT**\n"
                f"🆔 `file_unique_id`:\n`{uid}`\n"
                f"🔑 hash: `{short_hash(f_hash)}`\n"
                f"🤖 First saved by: `{bot_name or 'Unknown'}`\n"
                f"📅 First seen: `{fmt_time(created_at)}`\n"
                f"🔁 Duplicates blocked: `{dup_count}`\n"
                f"🕒 Last duplicate: `{fmt_time(last_dup)}`")
    return (f"❌ **NOT in vault** — this file is new.\n"
            f"🆔 `file_unique_id`:\n`{uid}`")

async def delete_record(f_hash) -> bool:
    cursor = await _db_conn.execute("DELETE FROM media_vault WHERE file_hash = ?", (f_hash,))
    await _db_conn.commit()
    return bool(cursor.rowcount and cursor.rowcount > 0)

async def dbfind_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    reply = update.message.reply_to_message
    uid = get_unique_id(reply) if reply else None
    if uid:
        try:
            f_hash, row = await inspect_lookup(uid)
        except Exception as e:
            log.error(f"/dbfind query failed: {e}")
            m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
        else:
            m = await update.message.reply_text(format_find_text(uid, f_hash, row), parse_mode="Markdown")
        asyncio.create_task(delete_msg(m, 20))
        return
    if reply is not None:
        # replied to a non-media message: no dead-end — just arm like "alone"
        arm_inspect("find", update.message.chat_id, context)
        m = await update.message.reply_text(
            "📥 **Find mode ON** — forward the media now (60s).\n"
            "It gets checked WITHOUT being saved, counted, grouped or deleted.",
            parse_mode="Markdown")
        asyncio.create_task(delete_msg(m, 20))
        return
    arm_inspect("find", update.message.chat_id, context)
    m = await update.message.reply_text(
        "📥 **Find mode ON** — forward the media now (60s).\n"
        "It gets checked WITHOUT being saved, counted, grouped or deleted.",
        parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 20))

async def dbdel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    asyncio.create_task(delete_msg(update.message))
    reply = update.message.reply_to_message
    uid = get_unique_id(reply) if reply else None
    if uid:
        try:
            f_hash, row = await inspect_lookup(uid)
            if row and await delete_record(f_hash):
                log.info(f"/dbdel removed {short_hash(f_hash)} from vault.")
                m = await update.message.reply_text(
                    f"🗑 **Deleted from vault:** `{short_hash(f_hash)}`\n"
                    f"This file will now pass as NEW if sent again.", parse_mode="Markdown")
            else:
                m = await update.message.reply_text("❌ Not found in vault — nothing deleted.")
        except Exception as e:
            log.error(f"/dbdel failed: {e}")
            m = await update.message.reply_text("❌ DB error — check vault_bot.log.")
        asyncio.create_task(delete_msg(m, 10))
        return
    if reply is not None:
        # replied to a non-media message: no dead-end — just arm like "alone"
        arm_inspect("del", update.message.chat_id, context)
        m = await update.message.reply_text(
            "🗑 **Delete mode ON** — forward the media now (60s).\n"
            "Only its vault RECORD is deleted — the media itself is never processed.",
            parse_mode="Markdown")
        asyncio.create_task(delete_msg(m, 20))
        return
    arm_inspect("del", update.message.chat_id, context)
    m = await update.message.reply_text(
        "🗑 **Delete mode ON** — forward the media now (60s).\n"
        "Only its vault RECORD is deleted — the media itself is never processed.",
        parse_mode="Markdown")
    asyncio.create_task(delete_msg(m, 20))

def arm_inspect(mode, chat_id, context):
    if INSPECT["task"]:
        INSPECT["task"].cancel()
    INSPECT["mode"] = mode
    INSPECT["responder"] = None
    INSPECT["grace_until"] = 0.0
    INSPECT["albums"].clear()
    INSPECT["task"] = asyncio.create_task(inspect_timeout(chat_id, context))
    log.info(f"Inspect mode armed: {mode}")

def disarm_inspect():
    INSPECT["mode"] = None
    INSPECT["responder"] = None
    if INSPECT["task"]:
        INSPECT["task"].cancel()
        INSPECT["task"] = None

async def inspect_timeout(chat_id, context):
    try:
        await asyncio.sleep(INSPECT_TIMEOUT)
        INSPECT["mode"] = None
        INSPECT["responder"] = None
        INSPECT["task"] = None
        INSPECT["grace_until"] = 0.0
        INSPECT["albums"].clear()
        m = await context.bot.send_message(chat_id, "⌛ Inspect mode timed out — nothing was touched.")
        asyncio.create_task(delete_msg(m, 5))
    except asyncio.CancelledError:
        pass

async def run_inspect_single(msg, context):
    uid = get_unique_id(msg)
    mode = INSPECT["mode"]
    try:
        f_hash, row = await inspect_lookup(uid)
        if mode == "find":
            m = await msg.reply_text(format_find_text(uid, f_hash, row), parse_mode="Markdown")
            asyncio.create_task(delete_msg(m, 20))
            return
        if row and await delete_record(f_hash):
            log.info(f"/dbdel (armed) removed {short_hash(f_hash)} from vault.")
            m = await msg.reply_text(
                f"🗑 **Deleted from vault:** `{short_hash(f_hash)}`\n"
                f"This file will now pass as NEW if sent again.", parse_mode="Markdown")
        else:
            m = await msg.reply_text("❌ Not found in vault — nothing deleted.")
        asyncio.create_task(delete_msg(m, 10))
    except Exception as e:
        log.error(f"armed inspect (single) failed: {e}")

async def run_inspect_album(mg_id, chat_id, context):
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        data = INSPECT["albums"].pop(mg_id, None)
        if not data or not INSPECT["mode"]:
            return
        msgs = data['m']
        mode = INSPECT["mode"]
        try:
            if mode == "find":
                lines, found = [], 0
                for i, m2 in enumerate(msgs, 1):
                    uid = get_unique_id(m2)
                    _, row = await inspect_lookup(uid)
                    found += 1 if row else 0
                    lines.append(f"{i}. {'✅' if row else '❌'} `{uid}`")
                text = (f"🔍 **Album check ({len(msgs)} items):** "
                        f"{found} in vault / {len(msgs) - found} new\n" + "\n".join(lines))
            else:
                deleted = 0
                for m2 in msgs:
                    uid = get_unique_id(m2)
                    f_hash, row = await inspect_lookup(uid)
                    if row and await delete_record(f_hash):
                        deleted += 1
                log.info(f"/dbdel (armed album) removed {deleted}/{len(msgs)} records.")
                text = (f"🗑 **Album delete done:** {deleted}/{len(msgs)} records removed.\n"
                        f"They will pass as NEW if sent again.")
            m = await context.bot.send_message(chat_id, text, parse_mode="Markdown")
            asyncio.create_task(delete_msg(m, 25))
        except Exception as e:
            log.error(f"armed inspect (album) failed: {e}")
    except asyncio.CancelledError:
        pass
    finally:
        disarm_inspect()

async def addcaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    text = update.message.text.replace("/addcaption", "").strip()
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["custom_caption"] = text if text else None
    m = await update.message.reply_text("✅ Caption updated." if text else "🗑 Caption cleared.")
    asyncio.create_task(delete_msg(m, 5))

async def removecaption_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    state["settings"]["custom_caption"] = None
    m = await update.message.reply_text("🗑 Caption cleared.")
    asyncio.create_task(delete_msg(m, 5))

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = get_state(context.bot.id)
    asyncio.create_task(delete_msg(update.message))
    await ensure_cache()
    bound = binding_for(context.bot.id, update.message.chat_id)
    text = (f"**Current Settings**:\n"
            f"- Relay: `{state['settings']['relay']}`\n"
            f"- GP: `{state['settings']['auto_group']}`\n"
            f"- Delete: `{state['settings']['autodelete']}`\n"
            f"- DB check: `{state['db_enabled']}`\n"
            f"- Caption: `{state['settings']['custom_caption']}`\n"
            f"- Manual topic: `{state['manual_topic']}`\n"
            f"- Bound here: `{bound}`\n"
            f"- Code: `{CODE_VERSION}`")
    m = await update.message.reply_text(text, parse_mode='Markdown')
    asyncio.create_task(delete_msg(m, 10))

# ================= GP ON: QUEUE FLUSHER =================
async def flush_queue_delayed(context: ContextTypes.DEFAULT_TYPE, qkey):
    state = get_state(context.bot.id)
    await asyncio.sleep(config.ALBUM_BATCH_DELAY)
    async with state["album_lock"]:
        q = state["queues"].get(qkey)
        if not q or not q.media:
            return
        m_list = q.media[:]
        ids = q.message_ids[:]
        c_id = q.chat_id
        dest_chat = q.dest_chat
        thread = q.thread
        q.media.clear()
        q.message_ids.clear()
        q.timer_task = None

        v = [m for m in m_list if not isinstance(m, InputMediaDocument)]
        d = [m for m in m_list if isinstance(m, InputMediaDocument)]
        chunks = [v[i:i+10] for i in range(0, len(v), 10)] + \
                 [d[i:i+10] for i in range(0, len(d), 10)]

        line = tag_line(q.topic_name)
        custom = state["settings"]["custom_caption"]
        log.info(f"flush: topic={q.topic_name!r} line={line!r} items={len(m_list)} "
                 f"first_orig_cap={repr(m_list[0].caption) if m_list else None}")
        ok = True
        for chunk in chunks:
            if not chunk:
                continue
            # ONE caption per sent group: first item only, rest clean
            base_cap, base_ents = album_first_caption(chunk)
            chunk = [first_input(m, custom, line, base_cap, base_ents) if i == 0
                     else type(m)(media=m.media)
                     for i, m in enumerate(chunk)]
            ok = await safe_send_media_group(context.bot, dest_chat, chunk, thread)
            await asyncio.sleep(config.QUEUE_COOLDOWN)

        if not ok and dest_chat == config.ADMIN_ID:
            await dm_warn_once(context.bot, state, c_id)

        if state["settings"]["autodelete"]:
            await safe_delete_messages(context.bot, c_id, ids)

        for pm_id in q.processing_msg_ids:
            try:
                await context.bot.delete_message(chat_id=c_id, message_id=pm_id)
            except TelegramError:
                pass
        q.processing_msg_ids.clear()

def enqueue(state, bot, chat_id, dest_chat, thread, obj, message_id, topic_name=None):
    qkey = (chat_id, thread)
    q = state["queues"].get(qkey)
    if q is None:
        q = state["queues"][qkey] = QueueState()
    q.chat_id = chat_id
    q.dest_chat = dest_chat
    q.thread = thread
    q.topic_name = topic_name
    q.media.append(obj)
    q.message_ids.append(message_id)
    if q.timer_task:
        q.timer_task.cancel()
    elif is_announcer(bot):
        async def post():
            try:
                p = await bot.send_message(
                    chat_id, "⏳ Grouping media...",
                    message_thread_id=thread if chat_id == config.ADMIN_ID else None)
                q.processing_msg_ids.append(p.message_id)
            except TelegramError:
                pass
        asyncio.create_task(post())
    q.timer_task = asyncio.create_task(flush_queue_delayed(_ctx_for(bot), qkey))

# context shim: flush needs a ContextTypes object with .bot
class _BotCtx:
    def __init__(self, bot):
        self.bot = bot

def _ctx_for(bot):
    return _BotCtx(bot)

# ================= GP ON: SMART ALBUM SORTER =================
async def route_gp_album_delayed(mg_id, context):
    state = get_state(context.bot.id)
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        if mg_id not in state["gp_albums"]:
            return
        data = state["gp_albums"].pop(mg_id)
        msgs, ids = data['m'], data['ids']
        chat_id = data['chat_id']
        dest_chat = data['dest_chat']
        thread = data['thread']

        # BIG ALBUM (6+): send as-is to destination
        # ONE caption per group: first item carries title + Topic line, rest clean
        if len(msgs) >= GP_ALBUM_SKIP_MIN:
            line = tag_line(data.get("topic_name"))
            custom = state["settings"]["custom_caption"]
            base_cap, base_ents = album_first_caption(msgs)
            final = []
            for i, m in enumerate(msgs):
                obj = first_obj(m, custom, line, base_cap, base_ents) if i == 0 \
                    else get_media_obj(m, None)
                if obj:
                    final.append(obj)
            log.info(f"send big-album: topic={data.get('topic_name')!r} line={line!r} "
                     f"n={len(final)} first_cap={repr(final[0].caption) if final else None}")
            if final:
                ok = await safe_send_media_group(context.bot, dest_chat, final, thread)
                if not ok and dest_chat == config.ADMIN_ID:
                    await dm_warn_once(context.bot, state, chat_id)
                if state["settings"]["autodelete"]:
                    await safe_delete_messages(context.bot, chat_id, ids)
            return

        # SMALL ALBUM (1-5): feed into the group queue
        for m in msgs:
            obj = get_media_obj(m, m.caption, m.caption_entities)
            if obj:
                enqueue(state, context.bot, chat_id, dest_chat, thread, obj,
                        m.message_id, topic_name=data.get("topic_name"))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"gp_album router error: {e}")

# ================= GP OFF: ALBUM DEBOUNCE =================
async def send_off_mode_album_delayed(mg_id, context):
    state = get_state(context.bot.id)
    try:
        await asyncio.sleep(config.ALBUM_BATCH_DELAY)
        if mg_id not in state["off_mode_albums"]:
            return
        data = state["off_mode_albums"].pop(mg_id)
        msgs, ids = data['m'], data['ids']
        chat_id = data['chat_id']
        dest_chat = data['dest_chat']
        thread = data['thread']
        line = tag_line(data.get("topic_name"))
        custom = state["settings"]["custom_caption"]
        base_cap, base_ents = album_first_caption(msgs)
        final = []
        for i, m in enumerate(msgs):
            obj = first_obj(m, custom, line, base_cap, base_ents) if i == 0 \
                else get_media_obj(m, None)
            if obj:
                final.append(obj)
        if final:
            ok = await safe_send_media_group(context.bot, dest_chat, final, thread)
            if not ok and dest_chat == config.ADMIN_ID:
                await dm_warn_once(context.bot, state, chat_id)
            if state["settings"]["autodelete"]:
                await safe_delete_messages(context.bot, chat_id, ids)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.error(f"off_mode_album error: {e}")

# ================= CENTRAL MESSAGE INTAKE =================
async def process_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return

    state = get_state(context.bot.id)
    chat_id = msg.chat_id

    # --- ARMED INSPECT INTERCEPT ---
    now = time.time()
    is_media = bool(get_unique_id(msg))
    if is_media and now < INSPECT["grace_until"]:
        return
    if INSPECT["mode"]:
        if not is_media:
            return
        if INSPECT["responder"] is None:
            INSPECT["responder"] = context.bot.id
            INSPECT["grace_until"] = now + INSPECT_GRACE
        if context.bot.id != INSPECT["responder"]:
            return
        if msg.media_group_id:
            mg_id = msg.media_group_id
            if mg_id not in INSPECT["albums"]:
                INSPECT["albums"][mg_id] = {'m': [], 'ids': [], 'task': None}
            INSPECT["albums"][mg_id]['m'].append(msg)
            INSPECT["albums"][mg_id]['ids'].append(msg.message_id)
            INSPECT["grace_until"] = now + config.ALBUM_BATCH_DELAY + INSPECT_GRACE
            if INSPECT["albums"][mg_id]['task']:
                INSPECT["albums"][mg_id]['task'].cancel()
            INSPECT["albums"][mg_id]['task'] = asyncio.create_task(
                run_inspect_album(mg_id, chat_id, context))
            return
        log.info(f"inspect: consuming forwarded media (mode={INSPECT['mode']})")
        await run_inspect_single(msg, context)
        disarm_inspect()
        return

    unique_id = get_unique_id(msg)
    if not unique_id:
        return  # non-media: nothing to relay

    f_hash = hash_id(unique_id)

    # --- RESOLVE DESTINATION ---
    relay = state["settings"]["relay"]
    src_thread = msg.message_thread_id
    chooser_mode = False
    if relay:
        await ensure_cache()
        if chat_id == config.ADMIN_ID and src_thread:
            # Inside a vault topic: clean-copy back into THE SAME topic,
            # then delete the original. (Bot's own copy never re-triggers.)
            thread = src_thread
            topic_name = REV_CACHE.get((context.bot.id, src_thread))
            if not topic_name:
                row = await db_fetchone(
                    "SELECT name FROM topics WHERE bot_id=? AND thread_id=?",
                    (context.bot.id, src_thread))
                if row:
                    topic_name = row[0]
                    REV_CACHE[(context.bot.id, src_thread)] = topic_name
        elif chat_id == config.ADMIN_ID:
            # All Messages: topic picker buttons decide (if topics exist)
            chooser_mode = any(b == context.bot.id for (b, n) in TOPIC_CACHE)
            topic_name, thread = None, None
        else:
            # Source chats (groups etc.): binding wins, then /use
            topic_name = binding_for(context.bot.id, chat_id) or state["manual_topic"]
            thread = await topic_thread(context.bot, state, topic_name) if topic_name else None
        dest_chat = config.ADMIN_ID
    else:
        thread, topic_name, dest_chat = None, None, chat_id

    # --- DUPLICATE CHECK (new db) ---
    try:
        cursor = await _db_conn.execute(
            "INSERT OR IGNORE INTO media_vault (file_hash, bot_name, created_at) VALUES (?, ?, ?)",
            (f_hash, bot_label(context.bot), time.time()))
        is_dup = (cursor.rowcount == 0)
        old_row = None
        if MIGRATE["on"]:
            old_row = await old_fetch(f_hash)
        if is_dup:
            log.info(f"duplicate blocked: {short_hash(f_hash)}")
            await _db_conn.execute(
                "UPDATE media_vault SET duplicate_count = duplicate_count + 1, "
                "last_duplicate_at = ? WHERE file_hash = ?", (time.time(), f_hash))
            if old_row:
                await old_delete(f_hash)  # keep old db consistent
            await _db_conn.commit()
            if state["db_enabled"]:
                current_time = time.time()
                if current_time - state["last_warn"] > 5.0:
                    state["last_warn"] = current_time
                    try:
                        warn_msg = await context.bot.send_message(
                            chat_id, "🗑️ **Duplicate — skipped.**", parse_mode="Markdown",
                            message_thread_id=src_thread)
                        asyncio.create_task(delete_msg(warn_msg, 3))
                    except TelegramError:
                        pass
                if state["settings"]["autodelete"]:
                    await delete_msg(msg)
                return
            # /db OFF: duplicate was counted above, but blocking is disabled —
            # fall through so the media is relayed normally.
        elif old_row:
            # MIGRATION: carry old created_at + bot_name, then drain old row
            await _db_conn.execute(
                "UPDATE media_vault SET bot_name = ?, created_at = ? WHERE file_hash = ?",
                (old_row[0] or bot_label(context.bot), old_row[1] or time.time(), f_hash))
            await _db_conn.commit()
            await old_delete(f_hash)
            log.info(f"Migrated {short_hash(f_hash)} (carried history)")
        else:
            await _db_conn.commit()
    except Exception as e:
        log.error(f"DB error during duplicate check: {e}")

    # --- ALL MESSAGES PICKER: hold media, ask which topic via buttons ---
    if chooser_mode:
        ch = state["chooser"]
        if ch is None:
            state["chooser"] = {"m": [], "ids": [], "task": None, "msg": None, "map": []}
            ch = state["chooser"]
        ch["m"].append(msg)
        ch["ids"].append(msg.message_id)
        if ch["task"]:
            ch["task"].cancel()
        ch["task"] = asyncio.create_task(show_chooser(context, state))
        return

    # --- GROUPING MODE ---
    if state["settings"]["auto_group"]:
        if msg.media_group_id:
            mg_id = msg.media_group_id
            if mg_id not in state["gp_albums"]:
                state["gp_albums"][mg_id] = {'m': [], 'ids': [], 'task': None,
                                             'chat_id': chat_id,
                                             'dest_chat': dest_chat,
                                             'thread': thread,
                                             'topic_name': topic_name}
            d = state["gp_albums"][mg_id]
            d['m'].append(msg)
            d['ids'].append(msg.message_id)
            if d['task']:
                d['task'].cancel()
            d['task'] = asyncio.create_task(route_gp_album_delayed(mg_id, context))
            return
        obj = get_media_obj(msg, msg.caption, msg.caption_entities)
        if not obj:
            return
        enqueue(state, context.bot, chat_id, dest_chat, thread, obj, msg.message_id,
                topic_name=topic_name)
        return

    # --- GP OFF: ALBUM PASSTHROUGH ---
    if msg.media_group_id:
        mg_id = msg.media_group_id
        if mg_id not in state["off_mode_albums"]:
            state["off_mode_albums"][mg_id] = {'m': [], 'ids': [], 'task': None,
                                               'chat_id': chat_id,
                                               'dest_chat': dest_chat,
                                               'thread': thread,
                                               'topic_name': topic_name}
        d = state["off_mode_albums"][mg_id]
        d['m'].append(msg)
        d['ids'].append(msg.message_id)
        if d['task']:
            d['task'].cancel()
        d['task'] = asyncio.create_task(send_off_mode_album_delayed(mg_id, context))
    else:
        # SINGLE MEDIA: copy to destination (strips forward tag)
        try:
            kwargs = {"chat_id": dest_chat, "from_chat_id": chat_id, "message_id": msg.message_id}
            if thread:
                kwargs["message_thread_id"] = thread
            tag = tag_line(topic_name)
            log.info(f"copy single: topic={topic_name!r} tag={tag!r} "
                     f"orig_cap={msg.caption!r}")
            if state["settings"]["custom_caption"] or tag:
                cap, ent, pm = cap_parts(msg.caption, msg.caption_entities,
                                         state["settings"]["custom_caption"], tag)
                kwargs["caption"] = cap
                if pm:
                    kwargs["parse_mode"] = pm
                elif ent:
                    kwargs["caption_entities"] = ent
            await context.bot.copy_message(**kwargs)
            if state["settings"]["autodelete"]:
                await delete_msg(msg)
            await asyncio.sleep(0.1)
        except RetryAfter as e:
            log.warning(f"FloodWait on copy_message — waiting {e.retry_after}s")
            await asyncio.sleep(e.retry_after + 1.5)
            try:
                await context.bot.copy_message(**kwargs)
                if state["settings"]["autodelete"]:
                    await delete_msg(msg)
            except TelegramError as e2:
                log.error(f"copy_message failed after retry: {e2}")
                if dest_chat == config.ADMIN_ID:
                    await dm_warn_once(context.bot, state, chat_id)
        except TelegramError as e:
            log.error(f"copy_message failed: {e}")
            if dest_chat == config.ADMIN_ID:
                await dm_warn_once(context.bot, state, chat_id)

# ================= STARTUP / SHUTDOWN =================
async def start_single_bot(app):
    await app.initialize()
    await app.start()
    await app.updater.start_polling(drop_pending_updates=True)

async def shutdown(apps):
    log.info("Shutdown signal received — stopping bots cleanly...")
    for app in apps:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception as e:
            log.error(f"Error stopping bot: {e}")
    await close_db()
    release_lock()
    log.info("All bots stopped. DB closed. Safe to exit.")

async def run_multiple_bots():
    global TOKEN_LABELS
    acquire_lock()
    log.info(f"Starting Vault Bots — code {CODE_VERSION}")
    TOKEN_LABELS = {token: f"Bot {i+1}" for i, token in enumerate(config.BOT_TOKENS)}

    await init_db()
    await init_old_db()

    apps = []
    for token in config.BOT_TOKENS:
        if not token:
            log.warning("Empty token in config — skipped.")
            continue
        app = (
            Application.builder()
            .token(token)
            .read_timeout(30.0)
            .write_timeout(30.0)
            .connect_timeout(30.0)
            .build()
        )
        app.add_handler(CommandHandler("start", start_command, filters=admin_filter))
        app.add_handler(CommandHandler("help", help_command, filters=admin_filter))
        app.add_handler(CommandHandler("relay", relay_command, filters=admin_filter))
        app.add_handler(CommandHandler("bind", bind_command, filters=admin_filter))
        app.add_handler(CommandHandler("unbind", unbind_command, filters=admin_filter))
        app.add_handler(CommandHandler("use", use_command, filters=admin_filter))
        app.add_handler(CommandHandler("topics", topics_command, filters=admin_filter))
        app.add_handler(CommandHandler("tdel", tdel_command, filters=admin_filter))
        app.add_handler(CommandHandler("trename", trename_command, filters=admin_filter))
        app.add_handler(CommandHandler("migrate", migrate_command, filters=admin_filter))
        app.add_handler(CommandHandler("gp", gp_command, filters=admin_filter))
        app.add_handler(CommandHandler("autodelete", autodelete_command, filters=admin_filter))
        app.add_handler(CommandHandler("db", db_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbstats", dbstats_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbclear", dbclear_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbfind", dbfind_command, filters=admin_filter))
        app.add_handler(CommandHandler("dbdel", dbdel_command, filters=admin_filter))
        app.add_handler(CommandHandler("addcaption", addcaption_command, filters=admin_filter))
        app.add_handler(CommandHandler("removecaption", removecaption_command, filters=admin_filter))
        app.add_handler(CommandHandler("settings", settings_command, filters=admin_filter))
        app.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, process_message))
        app.add_handler(CallbackQueryHandler(pick_callback, pattern="^pick:"))
        app.add_handler(CallbackQueryHandler(help_callback, pattern="^help:"))
        apps.append(app)

    if not apps:
        log.error("No valid tokens in config.BOT_TOKENS — nothing to start.")
        release_lock()
        return

    log.info(f"Connecting {len(apps)} bots to Telegram...")
    await asyncio.gather(*[start_single_bot(app) for app in apps])
    log.info(f"✅ {len(apps)} Vault Bots active. Code {CODE_VERSION}")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        log.info("Ctrl+C received — running clean shutdown...")
    finally:
        await shutdown(apps)

if __name__ == "__main__":
    try:
        asyncio.run(run_multiple_bots())
    except KeyboardInterrupt:
        release_lock()
        print("Bot stopped by user (Ctrl+C). Database closed safely.")

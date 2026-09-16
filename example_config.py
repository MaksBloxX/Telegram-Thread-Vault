# =====================================================
#  EXAMPLE CONFIG — copy this file as "config.py" and
#  fill in your own values. NEVER upload config.py!
# =====================================================

# --- Your bot tokens (get from @BotFather) ---
# Add one line per bot. Bot 1 = first token (posts "Grouping media..." status)
BOT_TOKENS = [
    "",   # Bot 1 — paste token here
    # "",  # Bot 2 (optional)
]

# --- Your Telegram user ID (get from @userinfobot) ---
# Only this user can control the bot. Everyone else is silently ignored.
ADMIN_ID = 123456789

# --- Databases ---
NEW_DB_NAME = "media_new.db"     # live vault (topics + bindings, no history table)
OLD_DB_NAME = "media.db"         # legacy vault — drain-only (read+delete).
                                 # When migration finishes, set "" and delete the file

# --- Timers & limits ---
ALBUM_BATCH_DELAY = 3.5   # seconds to wait before grouping incoming media
QUEUE_COOLDOWN    = 2.0   # pause after each sent album chunk (flood-safe)
MAX_DELETE_CHUNK  = 100   # max messages deleted per bulk call

# --- Migration default ---
MIGRATE_DEFAULT = True    # drain old DB automatically

# --- Default settings (per bot, changeable via commands) ---
DEFAULT_SETTINGS = {
    "autodelete"     : True,   # delete original after relay ("move" mode)
    "custom_caption" : None,   # custom caption (None = off)
    "auto_group"     : True,   # auto grouping (GP mode)
    "db_check"       : True,   # duplicate blocking (True = ON)
    "relay"          : True,   # relay to DM topics (False = legacy in-place mode)
}

# crawler_bot.py
# Telegram Silent Link Crawler Bot

import os
import sqlite3
import re
from datetime import datetime
from urllib.parse import urlparse

from telegram import Update, MessageEntity, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters

# === Database Setup ===
DB_FILE = "links.db"

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_link TEXT NOT NULL,
    original_link TEXT NOT NULL,
    platform TEXT NOT NULL,
    group_id INTEGER,
    group_name TEXT,
    sender_id INTEGER,
    sender_name TEXT,
    date_added TEXT NOT NULL,
    UNIQUE(normalized_link, group_id)
);
"""

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(CREATE_TABLE_QUERY)
    conn.commit()
    conn.close()

init_db()

# === Link Normalization Functions ===
def normalize_link(link):
    parsed = urlparse(link)
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"

def normalize_telegram_channel(link):
    parsed = urlparse(link)
    path = parsed.path.rstrip("/")
    special_prefixes = ["/joinchat/", "/addstickers/", "/s/", "/proxy"]
    if any(path.startswith(prefix) for prefix in special_prefixes):
        return f"{parsed.scheme}://{parsed.netloc}{path}"
    if parsed.netloc == "t.me" and "/" in path[1:]:
        parts = path.split("/")
        if len(parts) >= 3 and parts[2].isdigit():
            return f"{parsed.scheme}://{parsed.netloc}/{parts[1]}"
    return f"{parsed.scheme}://{parsed.netloc}{path}"

def detect_platform(link):
    if "t.me" in link:
        return "telegram"
    elif "chat.whatsapp.com" in link:
        return "whatsapp"
    return "other"

# === Link Extraction ===
def extract_all_links_from_message(message):
    links = []
    if message.text:
        links.extend(extract_links_from_entities(message.text, message.entities))
    if message.caption:
        links.extend(extract_links_from_entities(message.caption, message.caption_entities))
    if message.reply_markup:
        links.extend(extract_links_from_buttons(message.reply_markup))
    if message.reply_to_message:
        links.extend(extract_all_links_from_message(message.reply_to_message))
    return links

def extract_links_from_entities(text, entities):
    result = []
    if not entities:
        return result
    for entity in entities:
        if entity.type == MessageEntity.URL:
            result.append(text[entity.offset:entity.offset + entity.length])
        elif entity.type == MessageEntity.TEXT_LINK:
            result.append(entity.url)
    return result

def extract_links_from_buttons(markup):
    result = []
    for row in markup.inline_keyboard:
        for button in row:
            if button.url:
                result.append(button.url)
    return result

# === Save to Database ===
def save_links_to_db(original_links, chat, sender):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    saved_count = 0

    for link in original_links:
        normalized = normalize_telegram_channel(link) if "t.me" in link else normalize_link(link)
        platform = detect_platform(link)
        now = datetime.utcnow().isoformat()

        try:
            cursor.execute(
                "INSERT INTO links (normalized_link, original_link, platform, group_id, group_name, sender_id, sender_name, date_added) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    normalized, link, platform,
                    chat.id, chat.title or chat.username,
                    sender.id, sender.full_name,
                    now
                )
            )
            saved_count += 1
        except sqlite3.IntegrityError:
            continue  # Duplicate, skip

    conn.commit()
    conn.close()
    return saved_count

# === Bot Handler ===
async def handle_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat
    sender = update.effective_user

    if not message:
        return

    all_links = extract_all_links_from_message(message)
    if not all_links:
        return

    added = save_links_to_db(all_links, chat, sender)
    if added:
        print(f"✅ Saved {added} new link(s) from chat {chat.id} ({chat.title})")

# === Run Application ===
TOKEN = os.getenv("TOKEN")

if not TOKEN:
    print("❌ TOKEN not found in environment variables.")
    exit()

if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(MessageHandler(filters.ALL, handle_all_messages))

    print("🤖 Silent Link Crawler Bot is running...")
    app.run_polling()

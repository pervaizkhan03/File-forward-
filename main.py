import os
import re
import hashlib
import asyncio
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.messages import ImportChatInviteRequest

from motor.motor_asyncio import AsyncIOMotorClient

# ================= Config (from environment variables) =================
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])
MONGODB_URI = os.environ["MONGODB_URI"]
PORT = int(os.environ.get("PORT", 10000))
# How many messages can be in-flight (sending) at the same time.
# Higher = faster, but too high risks Telegram FloodWait penalties on big files.
# Lowered default from 15 -> 4 since large movie files were triggering frequent FloodWaits.
CONCURRENCY = int(os.environ.get("CONCURRENCY", 4))

# ================= MongoDB setup =================
mongo = AsyncIOMotorClient(MONGODB_URI)
db = mongo["telegram_forwarder"]
sources_col = db["sources"]           # {label, link, channel_id}
destinations_col = db["destinations"]  # {label, link, channel_id}
mappings_col = db["mappings"]          # {source_label, dest_label}
progress_col = db["progress"]          # {source_label, dest_label, last_id}
history_col = db["history"]            # {source_label, dest_label, message_id, snippet, ts}
counters_col = db["counters"]          # {_id: "source_seq"/"dest_seq", seq: N}
dedup_col = db["dedup"]                # {dest_label, file_id} — prevents forwarding the same post twice
settings_col = db["settings"]          # {dest_label, caption, button_text, button_url, allowed_ext, blocked_ext, min_size_mb, max_size_mb, block_keywords}

send_semaphore = asyncio.Semaphore(CONCURRENCY)


async def next_label(kind):
    """kind = 'source' or 'destination' -> returns 'C3' or 'D2' style label."""
    prefix = "C" if kind == "source" else "D"
    doc = await counters_col.find_one_and_update(
        {"_id": f"{kind}_seq"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=True,
    )
    return f"{prefix}{doc['seq']}"


# ================= Tiny HTTP server (keeps free Render Web Service alive) =================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Telegram forwarder is running.")

    def log_message(self, format, *args):
        pass


def start_health_server():
    HTTPServer(("0.0.0.0", PORT), HealthCheckHandler).serve_forever()


# ================= Telethon clients =================
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# userbot: does the actual reading/forwarding (must be a member of source/destination channels)
user_client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH, loop=loop)
# control bot: only talks to the owner to manage sources/destinations
bot_client = TelegramClient("bot_session", API_ID, API_HASH, loop=loop)
bot_client.parse_mode = "markdown"  # allows **bold**, `code`, etc. in all replies

# In-memory active state (kept in sync with MongoDB)
active_sources = {}       # label -> entity
active_destinations = {}  # label -> entity
active_mappings = {}      # source_label -> set of dest_labels
active_settings = {}      # dest_label -> settings dict (caption/button/filters)
pending_action = {}       # OWNER_ID -> "addsource" / "adddestination" / "link" / "unlink" (awaiting next plain message)


# ================= Helpers: join/resolve channels =================
def extract_invite_hash(link_or_id):
    if isinstance(link_or_id, str) and "t.me/+" in link_or_id:
        return link_or_id.split("t.me/+")[-1].strip("/")
    if isinstance(link_or_id, str) and link_or_id.startswith("+"):
        return link_or_id[1:]
    return None


async def join_and_resolve(link_or_id, label):
    invite_hash = extract_invite_hash(link_or_id)
    if invite_hash:
        try:
            result = await user_client(ImportChatInviteRequest(invite_hash))
            entity = result.chats[0]
            print(f"{label}: joined successfully (ID: {entity.id}).")
            return entity
        except UserAlreadyParticipantError:
            entity = await user_client.get_entity(link_or_id)
            print(f"{label}: already a member (ID: {entity.id}).")
            return entity
    else:
        try:
            entity = await user_client.get_entity(link_or_id)
        except ValueError:
            # Entity not cached yet — sync dialogs once and try again.
            # This handles the case where the account is already a member
            # of the channel but Telethon hasn't seen it in this session.
            print(f"{label}: entity not cached, syncing dialogs...")
            await user_client.get_dialogs(limit=None)
            entity = await user_client.get_entity(link_or_id)
        print(f"{label}: resolved (ID: {entity.id}).")
        return entity


# ================= Text cleaning =================
TELEGRAM_LINK_PATTERN = re.compile(r'(https?://)?t\.me/\S+', re.IGNORECASE)
MENTION_PATTERN = re.compile(r'@\w+')


def clean_text(text):
    if not text:
        return text
    text = TELEGRAM_LINK_PATTERN.sub('', text)
    text = MENTION_PATTERN.sub('', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


# ================= Per-destination settings: caption, button, filters =================
async def get_dest_settings(d_label):
    if d_label in active_settings:
        return active_settings[d_label]
    doc = await settings_col.find_one({"dest_label": d_label})
    settings = doc or {}
    active_settings[d_label] = settings
    return settings


async def set_dest_setting(d_label, key, value):
    await settings_col.update_one(
        {"dest_label": d_label},
        {"$set": {key: value}},
        upsert=True,
    )
    active_settings.setdefault(d_label, {})[key] = value


async def unset_dest_setting(d_label, key):
    await settings_col.update_one({"dest_label": d_label}, {"$unset": {key: ""}})
    if d_label in active_settings:
        active_settings[d_label].pop(key, None)


def get_message_extension(message):
    """Best-effort file extension, e.g. '.mp4'. Returns None if not a file/document."""
    fname = None
    if message.file and message.file.name:
        fname = message.file.name
    if not fname:
        return None
    if "." not in fname:
        return None
    return "." + fname.rsplit(".", 1)[-1].lower()


def passes_filters(message, settings):
    """Returns (True, None) if the message should be forwarded, or (False, reason) if it
    should be silently skipped for this destination based on its configured filters."""
    if not settings:
        return True, None

    # Extension allow/block list
    ext = get_message_extension(message)
    allowed_ext = settings.get("allowed_ext")
    blocked_ext = settings.get("blocked_ext")
    if allowed_ext:
        if ext is None or ext not in allowed_ext:
            return False, f"extension {ext or '(none)'} not in allow-list"
    if blocked_ext:
        if ext is not None and ext in blocked_ext:
            return False, f"extension {ext} is blocked"

    # File size limits (only applies to messages that actually have a file)
    if message.file:
        size_mb = (message.file.size or 0) / (1024 * 1024)
        min_mb = settings.get("min_size_mb")
        max_mb = settings.get("max_size_mb")
        if min_mb and size_mb < min_mb:
            return False, f"file size {size_mb:.1f}MB below min {min_mb}MB"
        if max_mb and size_mb > max_mb:
            return False, f"file size {size_mb:.1f}MB above max {max_mb}MB"

    # Keyword block list (checked against original caption/text, case-insensitive)
    block_keywords = settings.get("block_keywords")
    if block_keywords:
        text_lower = (message.text or "").lower()
        for kw in block_keywords:
            if kw.lower() in text_lower:
                return False, f"contains blocked keyword '{kw}'"

    return True, None


def build_caption_and_buttons(message, settings):
    """Applies link/mention cleaning + custom caption + custom button for a destination."""
    cleaned = clean_text(message.text)
    extra_caption = settings.get("caption")
    if extra_caption:
        cleaned = f"{cleaned}\n\n{extra_caption}" if cleaned else extra_caption

    buttons = None
    if settings.get("button_text") and settings.get("button_url"):
        buttons = [[Button.url(settings["button_text"], settings["button_url"])]]

    return cleaned, buttons


# ================= Progress & history (MongoDB) =================
async def get_progress(source_label, dest_label):
    doc = await progress_col.find_one({"source_label": source_label, "dest_label": dest_label})
    return doc["last_id"] if doc else 0


async def save_progress(source_label, dest_label, last_id):
    # $max ensures progress only ever moves forward, even if concurrent
    # sends finish out of order — avoids accidentally rewinding progress.
    await progress_col.update_one(
        {"source_label": source_label, "dest_label": dest_label},
        {"$max": {"last_id": last_id}, "$set": {"updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


async def log_history(source_label, dest_label, message_id, snippet):
    await history_col.insert_one({
        "source_label": source_label,
        "dest_label": dest_label,
        "message_id": message_id,
        "snippet": (snippet or "")[:100],
        "ts": datetime.now(timezone.utc),
    })


# ================= Duplicate detection =================
def get_file_id(message):
    """A stable identifier for a message's content, used to avoid forwarding
    the same file/text twice to the same destination (even from different sources)."""
    if message.file and message.file.id:
        return f"file:{message.file.id}"
    text = clean_text(message.text) or ""
    return "text:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


async def already_sent(dest_label, file_id):
    doc = await dedup_col.find_one({"dest_label": dest_label, "file_id": file_id})
    return doc is not None


async def mark_sent(dest_label, file_id):
    await dedup_col.update_one(
        {"dest_label": dest_label, "file_id": file_id},
        {"$setOnInsert": {"ts": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ================= Owner notifications (throttled) =================
_last_notify_time = 0
NOTIFY_COOLDOWN_SECONDS = 60  # don't spam the owner more than once a minute

# Global flood-wait cooldown: when Telegram rate-limits us, every other
# concurrent send waits until this time too, instead of hammering right
# back into another FloodWait.
_flood_until = 0.0


async def notify_owner(text):
    global _last_notify_time
    now = asyncio.get_event_loop().time()
    if now - _last_notify_time < NOTIFY_COOLDOWN_SECONDS:
        return  # skip to avoid flooding the owner's chat
    _last_notify_time = now
    try:
        await bot_client.send_message(OWNER_ID, text)
    except Exception as e:
        print(f"Failed to notify owner: {e}")


async def wait_out_global_cooldown():
    now = asyncio.get_event_loop().time()
    if now < _flood_until:
        await asyncio.sleep(_flood_until - now)


# ================= Sending =================
async def send_with_retry(message, dest_entity, max_retries=5, d_label=None):
    global _flood_until
    settings = await get_dest_settings(d_label) if d_label else {}
    cleaned, buttons = build_caption_and_buttons(message, settings)

    for attempt in range(max_retries):
        await wait_out_global_cooldown()
        try:
            if message.media:
                await user_client.send_file(dest_entity, message.media, caption=cleaned or None, buttons=buttons)
            else:
                if cleaned:
                    await user_client.send_message(dest_entity, cleaned, buttons=buttons)
            return True
        except FloodWaitError as e:
            wait_time = e.seconds + 5
            print(f"FloodWait hit. Sleeping {wait_time}s before retry...")
            _flood_until = max(_flood_until, asyncio.get_event_loop().time() + wait_time)
            dest_title = getattr(dest_entity, "title", str(getattr(dest_entity, "id", dest_entity)))
            asyncio.create_task(notify_owner(
                f"⏳ Rate limit (FloodWait) hit while sending to {dest_title}.\n"
                f"Waiting {wait_time}s before retrying automatically — no action needed."
            ))
            await asyncio.sleep(wait_time)
        except Exception as e:
            print(f"Error sending message {message.id}: {e}")
            return False
    return False


# ================= Backfill: one source -> its destinations =================
async def send_one_pair(source_label, message, d_label, d_entity, progress_tracker):
    """Send a single message to a single destination, respecting the concurrency
    limit and skipping if it's a duplicate (checked via the dedup DB, which is
    reliable even under concurrency — unlike comparing against the progress
    number, which can race ahead and cause messages to be silently skipped)."""
    try:
        async with send_semaphore:
            file_id = get_file_id(message)
            if await already_sent(d_label, file_id):
                # Already forwarded to this destination — just make sure progress reflects it.
                await save_progress(source_label, d_label, message.id)
                return

            settings = await get_dest_settings(d_label)
            ok_filter, reason = passes_filters(message, settings)
            if not ok_filter:
                # Filtered out on purpose — mark as handled so it's never retried/flagged as "missing".
                await mark_sent(d_label, file_id)
                await save_progress(source_label, d_label, message.id)
                return

            ok = await send_with_retry(message, d_entity, d_label=d_label)
            if ok:
                await mark_sent(d_label, file_id)
                await save_progress(source_label, d_label, message.id)
                await log_history(source_label, d_label, message.id, message.text)
    except Exception as e:
        # Never let one bad message (or a transient DB/network hiccup) kill the whole batch.
        print(f"[{source_label} -> {d_label}] Unexpected error on message {message.id}: {e}")


async def backfill_source(source_label, source_entity, dest_map, max_retries=10):
    """dest_map: {dest_label: entity}. Each (source, destination) pair tracks its own progress.
    Sends to all destinations concurrently (bounded by CONCURRENCY) for speed.
    Automatically resumes from wherever it left off if the connection drops mid-scan."""
    if not dest_map:
        return

    for attempt in range(1, max_retries + 1):
        try:
            progress = {d: await get_progress(source_label, d) for d in dest_map}
            start_id = min(progress.values())

            print(f"[{source_label}] Backfill scan starting from message ID {start_id} "
                  f"(covers {len(dest_map)} destination(s), concurrency={CONCURRENCY}, attempt {attempt})...")

            scanned = 0
            pending_tasks = []
            async for message in user_client.iter_messages(source_entity, reverse=True, min_id=start_id):
                for d_label, d_entity in dest_map.items():
                    task = asyncio.create_task(send_one_pair(source_label, message, d_label, d_entity, progress))
                    pending_tasks.append(task)

                scanned += 1
                if scanned % 100 == 0:
                    print(f"[{source_label}] Scanned {scanned} messages so far (at ID {message.id})...")
                    # Flush this batch, then a 1-second breather before the next 100
                    await asyncio.gather(*pending_tasks)
                    pending_tasks = []
                    await asyncio.sleep(1)

            if pending_tasks:
                await asyncio.gather(*pending_tasks)

            print(f"[{source_label}] Backfill scan complete ({scanned} messages scanned).")
            return  # success — no need to retry

        except Exception as e:
            wait_time = min(30 * attempt, 300)  # back off, cap at 5 minutes
            print(f"[{source_label}] Backfill interrupted ({e}). "
                  f"Resuming from last saved progress in {wait_time}s (attempt {attempt}/{max_retries})...")
            asyncio.create_task(notify_owner(
                f"⚠️ Forwarding for {source_label} was interrupted ({type(e).__name__}). "
                f"Auto-resuming in {wait_time}s — no action needed."
            ))
            await asyncio.sleep(wait_time)

    print(f"[{source_label}] Gave up after {max_retries} retries. Will retry again on next live event or restart.")


async def backfill_all():
    for s_label, s_entity in list(active_sources.items()):
        mapped_labels = active_mappings.get(s_label, set())
        dest_map = {d: active_destinations[d] for d in mapped_labels if d in active_destinations}
        await backfill_source(s_label, s_entity, dest_map)


# ================= Live listener =================
async def send_live_pair(source_label, message, d_label, d_entity):
    try:
        async with send_semaphore:
            file_id = get_file_id(message)
            if await already_sent(d_label, file_id):
                await save_progress(source_label, d_label, message.id)
                return

            settings = await get_dest_settings(d_label)
            ok_filter, reason = passes_filters(message, settings)
            if not ok_filter:
                await mark_sent(d_label, file_id)
                await save_progress(source_label, d_label, message.id)
                print(f"[{source_label} -> {d_label}] Skipped live message {message.id} ({reason})")
                return

            ok = await send_with_retry(message, d_entity, d_label=d_label)
            if ok:
                await mark_sent(d_label, file_id)
                await save_progress(source_label, d_label, message.id)
                await log_history(source_label, d_label, message.id, message.text)
                print(f"[{source_label} -> {d_label}] Forwarded new message {message.id}")
    except Exception as e:
        print(f"[{source_label} -> {d_label}] Unexpected error on live message {message.id}: {e}")


async def handle_new_message(event):
    try:
        source_label = None
        for label, entity in active_sources.items():
            if entity.id == event.chat_id:
                source_label = label
                break
        if not source_label:
            return

        mapped_labels = active_mappings.get(source_label, set())
        tasks = []
        for d_label in mapped_labels:
            d_entity = active_destinations.get(d_label)
            if not d_entity:
                continue
            tasks.append(asyncio.create_task(send_live_pair(source_label, event.message, d_label, d_entity)))
        if tasks:
            await asyncio.gather(*tasks)
    except Exception as e:
        print(f"Error in live message handler: {e}")


# ================= Bot commands (owner only) =================
def owner_only(func):
    async def wrapper(event):
        if event.sender_id != OWNER_ID:
            return
        await func(event)
    return wrapper


async def do_add_source(event, link):
    link = link.strip()
    label = await next_label("source")
    try:
        entity = await join_and_resolve(link, label)
    except Exception as e:
        await event.reply(f"❌ Failed to join/resolve: {e}")
        return
    await sources_col.insert_one({"label": label, "link": link, "channel_id": entity.id})
    active_sources[label] = entity
    await event.reply(
        f"✅ Source added as **{label}**.\n"
        f"It won't forward anywhere yet — link it to a destination with:\n"
        f"`/link {label} <destination_label>`"
    )


@bot_client.on(events.NewMessage(pattern=r'/addsource (.+)'))
@owner_only
async def cmd_addsource(event):
    await do_add_source(event, event.pattern_match.group(1))


@bot_client.on(events.NewMessage(pattern=r'/addsource$'))
@owner_only
async def cmd_addsource_prompt(event):
    pending_action[OWNER_ID] = "addsource"
    await event.reply("📥 Send the channel invite link (or @username) for the new **source** now.")


async def do_add_destination(event, link):
    link = link.strip()
    label = await next_label("destination")
    try:
        entity = await join_and_resolve(link, label)
    except Exception as e:
        await event.reply(f"❌ Failed to join/resolve: {e}")
        return
    await destinations_col.insert_one({"label": label, "link": link, "channel_id": entity.id})
    active_destinations[label] = entity
    await event.reply(
        f"✅ Destination added as **{label}**.\n"
        f"No source is sending to it yet — link one with:\n"
        f"`/link <source_label> {label}`"
    )


@bot_client.on(events.NewMessage(pattern=r'/adddestination (.+)'))
@owner_only
async def cmd_adddestination(event):
    await do_add_destination(event, event.pattern_match.group(1))


@bot_client.on(events.NewMessage(pattern=r'/adddestination$'))
@owner_only
async def cmd_adddestination_prompt(event):
    pending_action[OWNER_ID] = "adddestination"
    await event.reply("📤 Send the channel invite link (or @username) for the new **destination** now.")


@bot_client.on(events.NewMessage(pattern=r'/link (\S+) (\S+)'))
@owner_only
async def cmd_link(event):
    s_label, d_label = event.pattern_match.group(1), event.pattern_match.group(2)
    if s_label not in active_sources:
        await event.reply(f"⚠️ No source with label {s_label}. Check /sources")
        return
    if d_label not in active_destinations:
        await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
        return

    existing = await mappings_col.find_one({"source_label": s_label, "dest_label": d_label})
    if not existing:
        await mappings_col.insert_one({"source_label": s_label, "dest_label": d_label})
    active_mappings.setdefault(s_label, set()).add(d_label)

    await event.reply(f"🔗 Linked {s_label} -> {d_label}. Starting backfill for this pair in background...")
    asyncio.create_task(backfill_source(s_label, active_sources[s_label], {d_label: active_destinations[d_label]}))


@bot_client.on(events.NewMessage(pattern=r'/link$'))
@owner_only
async def cmd_link_prompt(event):
    pending_action[OWNER_ID] = "link"
    await event.reply("🔗 Send the source and destination labels separated by a space, e.g. `C1 D1`")


@bot_client.on(events.NewMessage(pattern=r'/unlink (\S+) (\S+)'))
@owner_only
async def cmd_unlink(event):
    s_label, d_label = event.pattern_match.group(1), event.pattern_match.group(2)
    await mappings_col.delete_one({"source_label": s_label, "dest_label": d_label})
    if s_label in active_mappings:
        active_mappings[s_label].discard(d_label)
    await event.reply(f"🔌 Unlinked {s_label} -> {d_label}. Forwarding stopped for this pair (progress kept).")


@bot_client.on(events.NewMessage(pattern=r'/unlink$'))
@owner_only
async def cmd_unlink_prompt(event):
    pending_action[OWNER_ID] = "unlink"
    await event.reply("🔌 Send the source and destination labels separated by a space, e.g. `C1 D1`")


@bot_client.on(events.NewMessage(pattern=r'/mappings'))
@owner_only
async def cmd_mappings(event):
    if not active_mappings or not any(active_mappings.values()):
        await event.reply(
            "🔗 **Mappings**\n━━━━━━━━━━━━━━━━━━━━\n"
            "No links set up yet. Use `/link <source> <dest>`"
        )
        return
    lines = []
    for s_label, d_labels in active_mappings.items():
        for d_label in d_labels:
            lines.append(f"• **{s_label}** → **{d_label}**")
    await event.reply("🔗 **Mappings**\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines))


async def do_remove_source(event, label):
    label = label.strip()
    if label in active_sources:
        del active_sources[label]
        active_mappings.pop(label, None)
        await sources_col.delete_one({"label": label})
        await mappings_col.delete_many({"source_label": label})
        await event.reply(f"🗑️ Source {label} removed (forwarding stopped, links cleared). Progress history kept in DB.")
    else:
        await event.reply(f"⚠️ No active source with label {label}.")


@bot_client.on(events.NewMessage(pattern=r'/removesource (.+)'))
@owner_only
async def cmd_removesource(event):
    await do_remove_source(event, event.pattern_match.group(1))


@bot_client.on(events.NewMessage(pattern=r'/removesource$'))
@owner_only
async def cmd_removesource_prompt(event):
    pending_action[OWNER_ID] = "removesource"
    await event.reply("🗑️ Send the source label to remove (check /sources), e.g. `C1`")


async def do_remove_destination(event, label):
    label = label.strip()
    if label in active_destinations:
        del active_destinations[label]
        for d_labels in active_mappings.values():
            d_labels.discard(label)
        await destinations_col.delete_one({"label": label})
        await mappings_col.delete_many({"dest_label": label})
        await event.reply(f"🗑️ Destination {label} removed (forwarding stopped, links cleared). Progress history kept in DB.")
    else:
        await event.reply(f"⚠️ No active destination with label {label}.")


@bot_client.on(events.NewMessage(pattern=r'/removedestination (.+)'))
@owner_only
async def cmd_removedestination(event):
    await do_remove_destination(event, event.pattern_match.group(1))


@bot_client.on(events.NewMessage(pattern=r'/removedestination$'))
@owner_only
async def cmd_removedestination_prompt(event):
    pending_action[OWNER_ID] = "removedestination"
    await event.reply("🗑️ Send the destination label to remove (check /destinations), e.g. `D1`")


# ================= Per-destination customization: caption, button, filters =================
@bot_client.on(events.NewMessage(pattern=r'/setcaption (\S+) ([\s\S]+)'))
@owner_only
async def cmd_setcaption(event):
    d_label, text = event.pattern_match.group(1), event.pattern_match.group(2)
    if d_label not in active_destinations:
        await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
        return
    await set_dest_setting(d_label, "caption", text)
    await event.reply(f"✅ Custom caption set for **{d_label}**. It'll be appended to every forwarded post.")


@bot_client.on(events.NewMessage(pattern=r'/setcaption$'))
@owner_only
async def cmd_setcaption_prompt(event):
    pending_action[OWNER_ID] = "setcaption"
    await event.reply("✏️ Send the destination label followed by the caption text, e.g. `D1 Join @MyChannel for more!`")


@bot_client.on(events.NewMessage(pattern=r'/removecaption (\S+)'))
@owner_only
async def cmd_removecaption(event):
    d_label = event.pattern_match.group(1).strip()
    await unset_dest_setting(d_label, "caption")
    await event.reply(f"🗑️ Custom caption removed for {d_label}.")


@bot_client.on(events.NewMessage(pattern=r'/setbutton (\S+) (\S+) (\S+)'))
@owner_only
async def cmd_setbutton(event):
    d_label, text, url = event.pattern_match.group(1), event.pattern_match.group(2), event.pattern_match.group(3)
    if d_label not in active_destinations:
        await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
        return
    if not url.startswith(("http://", "https://", "t.me/")):
        await event.reply("⚠️ Button URL must start with http://, https:// or t.me/")
        return
    await set_dest_setting(d_label, "button_text", text)
    await set_dest_setting(d_label, "button_url", url)
    await event.reply(f"✅ Custom button set for **{d_label}**: [{text}]({url})")


@bot_client.on(events.NewMessage(pattern=r'/setbutton$'))
@owner_only
async def cmd_setbutton_prompt(event):
    pending_action[OWNER_ID] = "setbutton"
    await event.reply(
        "🔘 Send the destination label, button text, and URL separated by spaces, e.g.\n"
        "`D1 Join_Channel https://t.me/mychannel`\n"
        "(use underscores instead of spaces in the button text)"
    )


@bot_client.on(events.NewMessage(pattern=r'/removebutton (\S+)'))
@owner_only
async def cmd_removebutton(event):
    d_label = event.pattern_match.group(1).strip()
    await unset_dest_setting(d_label, "button_text")
    await unset_dest_setting(d_label, "button_url")
    await event.reply(f"🗑️ Custom button removed for {d_label}.")


@bot_client.on(events.NewMessage(pattern=r'/setextfilter (\S+) (allow|block) (\S+)'))
@owner_only
async def cmd_setextfilter(event):
    d_label, mode, ext_list = event.pattern_match.group(1), event.pattern_match.group(2), event.pattern_match.group(3)
    if d_label not in active_destinations:
        await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
        return
    extensions = [("." + e.strip().lstrip(".").lower()) for e in ext_list.split(",") if e.strip()]
    if mode == "allow":
        await unset_dest_setting(d_label, "blocked_ext")
        await set_dest_setting(d_label, "allowed_ext", extensions)
    else:
        await unset_dest_setting(d_label, "allowed_ext")
        await set_dest_setting(d_label, "blocked_ext", extensions)
    await event.reply(f"✅ Extension filter set for **{d_label}**: {mode} {', '.join(extensions)}")


@bot_client.on(events.NewMessage(pattern=r'/setextfilter$'))
@owner_only
async def cmd_setextfilter_prompt(event):
    pending_action[OWNER_ID] = "setextfilter"
    await event.reply(
        "🎞 Send: `<dest_label> allow|block <ext1,ext2,...>`\n"
        "Example: `D1 allow mp4,mkv` or `D1 block pdf,exe`"
    )


@bot_client.on(events.NewMessage(pattern=r'/removeextfilter (\S+)'))
@owner_only
async def cmd_removeextfilter(event):
    d_label = event.pattern_match.group(1).strip()
    await unset_dest_setting(d_label, "allowed_ext")
    await unset_dest_setting(d_label, "blocked_ext")
    await event.reply(f"🗑️ Extension filter removed for {d_label}.")


@bot_client.on(events.NewMessage(pattern=r'/setsizefilter (\S+) (\S+) (\S+)'))
@owner_only
async def cmd_setsizefilter(event):
    d_label, min_str, max_str = event.pattern_match.group(1), event.pattern_match.group(2), event.pattern_match.group(3)
    if d_label not in active_destinations:
        await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
        return
    try:
        min_mb, max_mb = float(min_str), float(max_str)
    except ValueError:
        await event.reply("⚠️ Min and max must be numbers (MB). Use 0 for no limit.")
        return
    if min_mb > 0:
        await set_dest_setting(d_label, "min_size_mb", min_mb)
    else:
        await unset_dest_setting(d_label, "min_size_mb")
    if max_mb > 0:
        await set_dest_setting(d_label, "max_size_mb", max_mb)
    else:
        await unset_dest_setting(d_label, "max_size_mb")
    await event.reply(f"✅ Size filter set for **{d_label}**: min {min_mb}MB, max {max_mb}MB (0 = no limit)")


@bot_client.on(events.NewMessage(pattern=r'/setsizefilter$'))
@owner_only
async def cmd_setsizefilter_prompt(event):
    pending_action[OWNER_ID] = "setsizefilter"
    await event.reply(
        "📏 Send: `<dest_label> <min_MB> <max_MB>` (use 0 for no limit), e.g. `D1 0 2000`"
    )


@bot_client.on(events.NewMessage(pattern=r'/removesizefilter (\S+)'))
@owner_only
async def cmd_removesizefilter(event):
    d_label = event.pattern_match.group(1).strip()
    await unset_dest_setting(d_label, "min_size_mb")
    await unset_dest_setting(d_label, "max_size_mb")
    await event.reply(f"🗑️ Size filter removed for {d_label}.")


@bot_client.on(events.NewMessage(pattern=r'/setkeywords (\S+) ([\s\S]+)'))
@owner_only
async def cmd_setkeywords(event):
    d_label, kw_list = event.pattern_match.group(1), event.pattern_match.group(2)
    if d_label not in active_destinations:
        await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
        return
    keywords = [k.strip() for k in kw_list.split(",") if k.strip()]
    await set_dest_setting(d_label, "block_keywords", keywords)
    await event.reply(f"✅ Keyword filter set for **{d_label}**. Posts containing: {', '.join(keywords)} will be skipped.")


@bot_client.on(events.NewMessage(pattern=r'/setkeywords$'))
@owner_only
async def cmd_setkeywords_prompt(event):
    pending_action[OWNER_ID] = "setkeywords"
    await event.reply(
        "🚫 Send the destination label followed by comma-separated keywords to block, e.g.\n"
        "`D1 spam,promo,advertisement`"
    )


@bot_client.on(events.NewMessage(pattern=r'/removekeywords (\S+)'))
@owner_only
async def cmd_removekeywords(event):
    d_label = event.pattern_match.group(1).strip()
    await unset_dest_setting(d_label, "block_keywords")
    await event.reply(f"🗑️ Keyword filter removed for {d_label}.")


async def do_view_settings(event, d_label):
    settings = await get_dest_settings(d_label)
    if not settings:
        await event.reply(f"⚙️ **Settings — {d_label}**\n━━━━━━━━━━━━━━━━━━━━\nNothing customized yet (defaults apply).")
        return
    lines = [f"⚙️ **Settings — {d_label}**", "━━━━━━━━━━━━━━━━━━━━"]
    if settings.get("caption"):
        lines.append(f"📝 Caption: {settings['caption']}")
    if settings.get("button_text"):
        lines.append(f"🔘 Button: [{settings['button_text']}]({settings.get('button_url')})")
    if settings.get("allowed_ext"):
        lines.append(f"🎞 Allowed extensions: {', '.join(settings['allowed_ext'])}")
    if settings.get("blocked_ext"):
        lines.append(f"🎞 Blocked extensions: {', '.join(settings['blocked_ext'])}")
    if settings.get("min_size_mb") or settings.get("max_size_mb"):
        lines.append(f"📏 Size limit: min {settings.get('min_size_mb', 0)}MB, max {settings.get('max_size_mb', 0)}MB")
    if settings.get("block_keywords"):
        lines.append(f"🚫 Blocked keywords: {', '.join(settings['block_keywords'])}")
    await event.reply("\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/viewsettings (\S+)'))
@owner_only
async def cmd_viewsettings(event):
    await do_view_settings(event, event.pattern_match.group(1).strip())


@bot_client.on(events.NewMessage(pattern=r'/viewsettings$'))
@owner_only
async def cmd_viewsettings_prompt(event):
    pending_action[OWNER_ID] = "viewsettings"
    await event.reply("⚙️ Send the destination label, e.g. `D1`")


@bot_client.on(events.NewMessage(pattern=r'/sources'))
@owner_only
async def cmd_sources(event):
    if not active_sources:
        await event.reply("📥 No sources added yet.\nUse `/addsource <link>`")
        return
    lines = []
    for label, entity in active_sources.items():
        title = getattr(entity, "title", str(entity.id))
        lines.append(f"• **{label}** — {title}\n   `ID: {entity.id}`")
    await event.reply("📥 **Sources**\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/destinations'))
@owner_only
async def cmd_destinations(event):
    if not active_destinations:
        await event.reply("📤 No destinations added yet.\nUse `/adddestination <link>`")
        return
    lines = []
    for label, entity in active_destinations.items():
        title = getattr(entity, "title", str(entity.id))
        lines.append(f"• **{label}** — {title}\n   `ID: {entity.id}`")
    await event.reply("📤 **Destinations**\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/status'))
@owner_only
async def cmd_status(event):
    lines = [f"📥 Sources: **{len(active_sources)}**   📤 Destinations: **{len(active_destinations)}**", ""]
    any_mapping = False
    for s_label, d_labels in active_mappings.items():
        for d_label in d_labels:
            any_mapping = True
            p = await get_progress(s_label, d_label)
            lines.append(f"🔗 **{s_label} → {d_label}** — last forwarded ID `{p}`")
    if not any_mapping:
        lines.append("No links set up yet. Use `/link <source> <dest>`")
    await event.reply("📊 **Status**\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines))


async def do_history(event, s_label, d_label):
    cursor = history_col.find({"source_label": s_label, "dest_label": d_label}).sort("ts", -1).limit(5)
    lines = []
    async for doc in cursor:
        lines.append(f"• `{doc['message_id']}` — {doc['snippet'] or '(media, no caption)'}")
    if not lines:
        await event.reply(f"🕘 **History — {s_label} → {d_label}**\n━━━━━━━━━━━━━━━━━━━━\nNothing forwarded yet.")
    else:
        await event.reply(
            f"🕘 **History — {s_label} → {d_label}**\n━━━━━━━━━━━━━━━━━━━━\n" + "\n".join(lines)
        )


@bot_client.on(events.NewMessage(pattern=r'/history (\S+) (\S+)'))
@owner_only
async def cmd_history(event):
    await do_history(event, event.pattern_match.group(1), event.pattern_match.group(2))


@bot_client.on(events.NewMessage(pattern=r'/history$'))
@owner_only
async def cmd_history_prompt(event):
    pending_action[OWNER_ID] = "history"
    await event.reply("🕘 Send the source and destination labels separated by a space, e.g. `C1 D1`")


@bot_client.on(events.NewMessage(pattern=r'/clearhistory all$'))
@owner_only
async def cmd_clearhistory_all(event):
    result = await history_col.delete_many({})
    await event.reply(f"🗑️ Cleared ALL history records ({result.deleted_count} entries deleted).")


async def do_clear_history(event, s_label, d_label):
    result = await history_col.delete_many({"source_label": s_label, "dest_label": d_label})
    await event.reply(f"🗑️ Cleared history for {s_label} -> {d_label} ({result.deleted_count} entries deleted).")


@bot_client.on(events.NewMessage(pattern=r'/clearhistory (\S+) (\S+)'))
@owner_only
async def cmd_clearhistory(event):
    await do_clear_history(event, event.pattern_match.group(1), event.pattern_match.group(2))


@bot_client.on(events.NewMessage(pattern=r'/clearhistory$'))
@owner_only
async def cmd_clearhistory_prompt(event):
    pending_action[OWNER_ID] = "clearhistory"
    await event.reply(
        "🧹 Send the source and destination labels separated by a space (e.g. `C1 D1`), "
        "or send `all` to clear everything."
    )


async def run_check_missing(chat_id, s_label, s_entity, d_label, d_entity, always_notify=True):
    """chat_id=None means this was an automatic run — only message the owner if
    something was actually missing (avoids noisy notifications every cycle)."""
    checked = 0
    forwarded = 0
    settings = await get_dest_settings(d_label)
    async for message in user_client.iter_messages(s_entity, reverse=True):
        checked += 1
        try:
            file_id = get_file_id(message)
            if not await already_sent(d_label, file_id):
                ok_filter, _ = passes_filters(message, settings)
                if not ok_filter:
                    await mark_sent(d_label, file_id)
                    await save_progress(s_label, d_label, message.id)
                else:
                    ok = await send_with_retry(message, d_entity, d_label=d_label)
                    if ok:
                        await mark_sent(d_label, file_id)
                        await save_progress(s_label, d_label, message.id)
                        await log_history(s_label, d_label, message.id, message.text)
                        forwarded += 1
        except Exception as e:
            print(f"[checkmissing {s_label}->{d_label}] Error on message {message.id}: {e}")

        if checked % 200 == 0:
            print(f"[checkmissing {s_label}->{d_label}] Checked {checked}, forwarded {forwarded} missing so far...")
            await asyncio.sleep(1)

    print(f"[checkmissing {s_label}->{d_label}] Done. Checked {checked}, forwarded {forwarded} missing.")

    if always_notify or forwarded > 0:
        target = chat_id if chat_id else OWNER_ID
        try:
            await bot_client.send_message(
                target,
                f"✅ Missing-post check done for {s_label} -> {d_label}.\n"
                f"Checked {checked} total posts, forwarded {forwarded} that were missing."
            )
        except Exception as e:
            print(f"Failed to send checkmissing completion message: {e}")


# ================= Automatic periodic missing-post check =================
AUTO_CHECK_INTERVAL_HOURS = float(os.environ.get("AUTO_CHECK_INTERVAL_HOURS", 6))


async def auto_check_loop():
    if AUTO_CHECK_INTERVAL_HOURS <= 0:
        print("Auto-check disabled (AUTO_CHECK_INTERVAL_HOURS <= 0).")
        return
    while True:
        await asyncio.sleep(AUTO_CHECK_INTERVAL_HOURS * 3600)
        print("Running automatic missing-post check for all linked pairs...")
        for s_label, d_labels in list(active_mappings.items()):
            s_entity = active_sources.get(s_label)
            if not s_entity:
                continue
            for d_label in list(d_labels):
                d_entity = active_destinations.get(d_label)
                if not d_entity:
                    continue
                try:
                    # always_notify=False: stay quiet unless something was actually missing
                    await run_check_missing(None, s_label, s_entity, d_label, d_entity, always_notify=False)
                except Exception as e:
                    print(f"[auto-check] Error checking {s_label}->{d_label}: {e}")


async def do_check_missing(event, s_label, d_labels):
    if s_label not in active_sources:
        await event.reply(f"⚠️ No source with label {s_label}. Check /sources")
        return

    valid, invalid = [], []
    for d in d_labels:
        (valid if d in active_destinations else invalid).append(d)

    if invalid:
        await event.reply(f"⚠️ Unknown destination(s): {', '.join(invalid)}. Check /destinations")
        return
    if not valid:
        await event.reply("⚠️ No valid destination labels given.")
        return

    await event.reply(
        f"🔍 Checking all posts in **{s_label}** against **{', '.join(valid)}** for anything missed. "
        f"This runs in the background and can take a while for large channels — "
        f"I'll message you when each one is done."
    )
    for d_label in valid:
        asyncio.create_task(run_check_missing(
            event.chat_id, s_label, active_sources[s_label], d_label, active_destinations[d_label]
        ))


@bot_client.on(events.NewMessage(pattern=r'/checkmissing (\S+) (.+)'))
@owner_only
async def cmd_checkmissing(event):
    s_label = event.pattern_match.group(1)
    d_labels = event.pattern_match.group(2).split()
    await do_check_missing(event, s_label, d_labels)


@bot_client.on(events.NewMessage(pattern=r'/checkmissing$'))
@owner_only
async def cmd_checkmissing_prompt(event):
    pending_action[OWNER_ID] = "checkmissing"
    await event.reply(
        "🔍 Send the source label followed by one or more destination labels, "
        "separated by spaces, e.g. `C1 D1 D2`"
    )


@bot_client.on(events.NewMessage(pattern=r'/resetall$'))
@owner_only
async def cmd_resetall(event):
    await event.reply(
        "⚠️ This will permanently delete ALL sources, destinations, links, progress, "
        "history and duplicate-tracking data — everything restarts from zero (C1/D1 again).\n\n"
        "This cannot be undone. Type /resetall CONFIRM to proceed."
    )


@bot_client.on(events.NewMessage(pattern=r'/resetall CONFIRM$'))
@owner_only
async def cmd_resetall_confirm(event):
    await sources_col.delete_many({})
    await destinations_col.delete_many({})
    await mappings_col.delete_many({})
    await progress_col.delete_many({})
    await history_col.delete_many({})
    await dedup_col.delete_many({})
    await counters_col.delete_many({})

    active_sources.clear()
    active_destinations.clear()
    active_mappings.clear()

    await event.reply(
        "✅ Everything cleared. Labels will restart from C1/D1 next time you add a source/destination.\n"
        "Use /addsource and /adddestination to start fresh."
    )


@bot_client.on(events.NewMessage(pattern=r'/start'))
@owner_only
async def cmd_start(event):
    await event.reply(
        "🎬 **Forward Bot — Control Panel**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Manage sources, destinations and links right from here.\n\n"
        "**📥 Sources & 📤 Destinations**\n"
        "`/addsource <link>`\n"
        "`/adddestination <link>`\n"
        "`/removesource <label>`\n"
        "`/removedestination <label>`\n"
        "`/sources`  •  `/destinations`\n\n"
        "**🔗 Linking**\n"
        "`/link <source> <dest>`\n"
        "`/unlink <source> <dest>`\n"
        "`/mappings`\n\n"
        "**📊 Monitoring**\n"
        "`/status`\n"
        "`/history <source> <dest>`\n\n"
        "**🎨 Customize a destination**\n"
        "`/setcaption <dest> <text>` / `/removecaption <dest>`\n"
        "`/setbutton <dest> <text> <url>` / `/removebutton <dest>`\n"
        "`/setextfilter <dest> allow|block <ext1,ext2>` / `/removeextfilter <dest>`\n"
        "`/setsizefilter <dest> <min_MB> <max_MB>` / `/removesizefilter <dest>`\n"
        "`/setkeywords <dest> <word1,word2>` / `/removekeywords <dest>`\n"
        "`/viewsettings <dest>`\n\n"
        "**🧹 Maintenance**\n"
        "`/checkmissing <source> <dest1> [dest2] [dest3]...` — re-scan & forward anything missed\n"
        f"⏱ Auto-check runs every **{AUTO_CHECK_INTERVAL_HOURS}h** automatically\n"
        "`/clearhistory <source> <dest>` / `/clearhistory all`\n"
        "`/resetall` — wipe everything, start fresh from C1/D1\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 Tip: send any command with no arguments (e.g. just `/addsource`) and I'll ask you "
        "for the details step by step — or use the buttons below.",
        buttons=[
            [Button.text("➕ Add Source", resize=True), Button.text("➕ Add Destination", resize=True)],
            [Button.text("🗑️ Remove Source", resize=True), Button.text("🗑️ Remove Destination", resize=True)],
            [Button.text("🔗 Link", resize=True), Button.text("🔌 Unlink", resize=True)],
            [Button.text("📥 Sources", resize=True), Button.text("📤 Destinations", resize=True)],
            [Button.text("🔗 Mappings", resize=True), Button.text("📊 Status", resize=True)],
            [Button.text("🕘 History", resize=True), Button.text("🔍 Check Missing", resize=True)],
            [Button.text("🧹 Clear History", resize=True), Button.text("♻️ Reset All", resize=True)],
            [Button.text("📝 Set Caption", resize=True), Button.text("🔘 Set Button", resize=True)],
            [Button.text("🎞 Ext Filter", resize=True), Button.text("📏 Size Filter", resize=True)],
            [Button.text("🚫 Keyword Filter", resize=True), Button.text("⚙️ View Settings", resize=True)],
            [Button.text("❓ Help", resize=True)],
        ]
    )


@bot_client.on(events.NewMessage(pattern=r'^❓ Help$'))
@owner_only
async def btn_help(event):
    await cmd_start(event)


@bot_client.on(events.NewMessage(pattern=r'^📥 Sources$'))
@owner_only
async def btn_sources(event):
    await cmd_sources(event)


@bot_client.on(events.NewMessage(pattern=r'^📤 Destinations$'))
@owner_only
async def btn_destinations(event):
    await cmd_destinations(event)


@bot_client.on(events.NewMessage(pattern=r'^🔗 Mappings$'))
@owner_only
async def btn_mappings(event):
    await cmd_mappings(event)


@bot_client.on(events.NewMessage(pattern=r'^📊 Status$'))
@owner_only
async def btn_status(event):
    await cmd_status(event)


@bot_client.on(events.NewMessage(pattern=r'^➕ Add Source$'))
@owner_only
async def btn_addsource(event):
    pending_action[OWNER_ID] = "addsource"
    await event.reply("📥 Send the channel invite link (or @username) for the new **source** now.")


@bot_client.on(events.NewMessage(pattern=r'^➕ Add Destination$'))
@owner_only
async def btn_adddestination(event):
    pending_action[OWNER_ID] = "adddestination"
    await event.reply("📤 Send the channel invite link (or @username) for the new **destination** now.")


@bot_client.on(events.NewMessage(pattern=r'^🗑️ Remove Source$'))
@owner_only
async def btn_removesource(event):
    pending_action[OWNER_ID] = "removesource"
    await event.reply("🗑️ Send the source label to remove (check /sources), e.g. `C1`")


@bot_client.on(events.NewMessage(pattern=r'^🗑️ Remove Destination$'))
@owner_only
async def btn_removedestination(event):
    pending_action[OWNER_ID] = "removedestination"
    await event.reply("🗑️ Send the destination label to remove (check /destinations), e.g. `D1`")


@bot_client.on(events.NewMessage(pattern=r'^🔗 Link$'))
@owner_only
async def btn_link(event):
    pending_action[OWNER_ID] = "link"
    await event.reply("🔗 Send the source and destination labels separated by a space, e.g. `C1 D1`")


@bot_client.on(events.NewMessage(pattern=r'^🔌 Unlink$'))
@owner_only
async def btn_unlink(event):
    pending_action[OWNER_ID] = "unlink"
    await event.reply("🔌 Send the source and destination labels separated by a space, e.g. `C1 D1`")


@bot_client.on(events.NewMessage(pattern=r'^🕘 History$'))
@owner_only
async def btn_history(event):
    pending_action[OWNER_ID] = "history"
    await event.reply("🕘 Send the source and destination labels separated by a space, e.g. `C1 D1`")


@bot_client.on(events.NewMessage(pattern=r'^🔍 Check Missing$'))
@owner_only
async def btn_checkmissing(event):
    pending_action[OWNER_ID] = "checkmissing"
    await event.reply(
        "🔍 Send the source label followed by one or more destination labels, "
        "separated by spaces, e.g. `C1 D1 D2`"
    )


@bot_client.on(events.NewMessage(pattern=r'^🧹 Clear History$'))
@owner_only
async def btn_clearhistory(event):
    pending_action[OWNER_ID] = "clearhistory"
    await event.reply(
        "🧹 Send the source and destination labels separated by a space (e.g. `C1 D1`), "
        "or send `all` to clear everything."
    )


@bot_client.on(events.NewMessage(pattern=r'^♻️ Reset All$'))
@owner_only
async def btn_resetall(event):
    await cmd_resetall(event)


@bot_client.on(events.NewMessage(pattern=r'^📝 Set Caption$'))
@owner_only
async def btn_setcaption(event):
    pending_action[OWNER_ID] = "setcaption"
    await event.reply("✏️ Send the destination label followed by the caption text, e.g. `D1 Join @MyChannel for more!`")


@bot_client.on(events.NewMessage(pattern=r'^🔘 Set Button$'))
@owner_only
async def btn_setbutton(event):
    pending_action[OWNER_ID] = "setbutton"
    await event.reply(
        "🔘 Send the destination label, button text, and URL separated by spaces, e.g.\n"
        "`D1 Join_Channel https://t.me/mychannel`\n"
        "(use underscores instead of spaces in the button text)"
    )


@bot_client.on(events.NewMessage(pattern=r'^🎞 Ext Filter$'))
@owner_only
async def btn_setextfilter(event):
    pending_action[OWNER_ID] = "setextfilter"
    await event.reply(
        "🎞 Send: `<dest_label> allow|block <ext1,ext2,...>`\n"
        "Example: `D1 allow mp4,mkv` or `D1 block pdf,exe`"
    )


@bot_client.on(events.NewMessage(pattern=r'^📏 Size Filter$'))
@owner_only
async def btn_setsizefilter(event):
    pending_action[OWNER_ID] = "setsizefilter"
    await event.reply("📏 Send: `<dest_label> <min_MB> <max_MB>` (use 0 for no limit), e.g. `D1 0 2000`")


@bot_client.on(events.NewMessage(pattern=r'^🚫 Keyword Filter$'))
@owner_only
async def btn_setkeywords(event):
    pending_action[OWNER_ID] = "setkeywords"
    await event.reply(
        "🚫 Send the destination label followed by comma-separated keywords to block, e.g.\n"
        "`D1 spam,promo,advertisement`"
    )


@bot_client.on(events.NewMessage(pattern=r'^⚙️ View Settings$'))
@owner_only
async def btn_viewsettings(event):
    pending_action[OWNER_ID] = "viewsettings"
    await event.reply("⚙️ Send the destination label, e.g. `D1`")


@bot_client.on(events.NewMessage())
@owner_only
async def catch_pending_input(event):
    """Handles the plain-text follow-up after tapping a button like ➕ Add Source
    (or sending the bare command, e.g. /addsource with no link)."""
    text = event.raw_text.strip()
    if text.startswith('/') or text.startswith((
        '➕', '🔗', '🔌', '📥', '📤', '📊', '🔍', '🧹', '♻️', '❓', '🗑️', '🕘',
        '📝', '🔘', '🎞', '📏', '🚫', '⚙️'
    )):
        return  # a real command or button label — already handled elsewhere

    action = pending_action.get(OWNER_ID)
    if not action:
        return  # no pending action, nothing to do with random text

    pending_action.pop(OWNER_ID, None)

    if action == "addsource":
        await do_add_source(event, text)
    elif action == "adddestination":
        await do_add_destination(event, text)
    elif action == "removesource":
        await do_remove_source(event, text)
    elif action == "removedestination":
        await do_remove_destination(event, text)
    elif action in ("link", "unlink"):
        parts = text.split()
        if len(parts) != 2:
            await event.reply("⚠️ Please send exactly two labels separated by a space, e.g. `C1 D1`")
            return
        s_label, d_label = parts
        if action == "link":
            if s_label not in active_sources:
                await event.reply(f"⚠️ No source with label {s_label}. Check /sources")
                return
            if d_label not in active_destinations:
                await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
                return
            existing = await mappings_col.find_one({"source_label": s_label, "dest_label": d_label})
            if not existing:
                await mappings_col.insert_one({"source_label": s_label, "dest_label": d_label})
            active_mappings.setdefault(s_label, set()).add(d_label)
            await event.reply(f"🔗 Linked {s_label} -> {d_label}. Starting backfill for this pair in background...")
            asyncio.create_task(backfill_source(
                s_label, active_sources[s_label], {d_label: active_destinations[d_label]}
            ))
        else:
            await mappings_col.delete_one({"source_label": s_label, "dest_label": d_label})
            if s_label in active_mappings:
                active_mappings[s_label].discard(d_label)
            await event.reply(f"🔌 Unlinked {s_label} -> {d_label}. Forwarding stopped (progress kept).")
    elif action == "history":
        parts = text.split()
        if len(parts) != 2:
            await event.reply("⚠️ Please send exactly two labels separated by a space, e.g. `C1 D1`")
            return
        await do_history(event, parts[0], parts[1])
    elif action == "clearhistory":
        if text.lower() == "all":
            result = await history_col.delete_many({})
            await event.reply(f"🗑️ Cleared ALL history records ({result.deleted_count} entries deleted).")
            return
        parts = text.split()
        if len(parts) != 2:
            await event.reply("⚠️ Please send exactly two labels (e.g. `C1 D1`) or `all`.")
            return
        await do_clear_history(event, parts[0], parts[1])
    elif action == "checkmissing":
        parts = text.split()
        if len(parts) < 2:
            await event.reply("⚠️ Please send a source label followed by one or more destination labels, e.g. `C1 D1 D2`")
            return
        await do_check_missing(event, parts[0], parts[1:])
    elif action == "setcaption":
        parts = text.split(maxsplit=1)
        if len(parts) != 2:
            await event.reply("⚠️ Please send the destination label followed by the caption text.")
            return
        d_label, caption_text = parts
        if d_label not in active_destinations:
            await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
            return
        await set_dest_setting(d_label, "caption", caption_text)
        await event.reply(f"✅ Custom caption set for **{d_label}**.")
    elif action == "setbutton":
        parts = text.split()
        if len(parts) != 3:
            await event.reply("⚠️ Please send: `<dest_label> <button_text> <url>`")
            return
        d_label, btn_text, url = parts
        if d_label not in active_destinations:
            await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
            return
        if not url.startswith(("http://", "https://", "t.me/")):
            await event.reply("⚠️ Button URL must start with http://, https:// or t.me/")
            return
        await set_dest_setting(d_label, "button_text", btn_text)
        await set_dest_setting(d_label, "button_url", url)
        await event.reply(f"✅ Custom button set for **{d_label}**.")
    elif action == "setextfilter":
        parts = text.split()
        if len(parts) != 3 or parts[1] not in ("allow", "block"):
            await event.reply("⚠️ Please send: `<dest_label> allow|block <ext1,ext2,...>`")
            return
        d_label, mode, ext_list = parts
        if d_label not in active_destinations:
            await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
            return
        extensions = [("." + e.strip().lstrip(".").lower()) for e in ext_list.split(",") if e.strip()]
        if mode == "allow":
            await unset_dest_setting(d_label, "blocked_ext")
            await set_dest_setting(d_label, "allowed_ext", extensions)
        else:
            await unset_dest_setting(d_label, "allowed_ext")
            await set_dest_setting(d_label, "blocked_ext", extensions)
        await event.reply(f"✅ Extension filter set for **{d_label}**: {mode} {', '.join(extensions)}")
    elif action == "setsizefilter":
        parts = text.split()
        if len(parts) != 3:
            await event.reply("⚠️ Please send: `<dest_label> <min_MB> <max_MB>` (0 = no limit)")
            return
        d_label, min_str, max_str = parts
        if d_label not in active_destinations:
            await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
            return
        try:
            min_mb, max_mb = float(min_str), float(max_str)
        except ValueError:
            await event.reply("⚠️ Min and max must be numbers (MB).")
            return
        if min_mb > 0:
            await set_dest_setting(d_label, "min_size_mb", min_mb)
        else:
            await unset_dest_setting(d_label, "min_size_mb")
        if max_mb > 0:
            await set_dest_setting(d_label, "max_size_mb", max_mb)
        else:
            await unset_dest_setting(d_label, "max_size_mb")
        await event.reply(f"✅ Size filter set for **{d_label}**.")
    elif action == "setkeywords":
        parts = text.split(maxsplit=1)
        if len(parts) != 2:
            await event.reply("⚠️ Please send the destination label followed by comma-separated keywords.")
            return
        d_label, kw_list = parts
        if d_label not in active_destinations:
            await event.reply(f"⚠️ No destination with label {d_label}. Check /destinations")
            return
        keywords = [k.strip() for k in kw_list.split(",") if k.strip()]
        await set_dest_setting(d_label, "block_keywords", keywords)
        await event.reply(f"✅ Keyword filter set for **{d_label}**.")
    elif action == "viewsettings":
        await do_view_settings(event, text.strip())


# ================= Startup =================
async def main():
    await user_client.start()
    print("Userbot client started.")
    await bot_client.start(bot_token=BOT_TOKEN)
    print("Control bot started.")

    # Index for fast duplicate lookups
    await dedup_col.create_index([("dest_label", 1), ("file_id", 1)], unique=True)

    # Load existing config from MongoDB
    async for doc in sources_col.find({}):
        try:
            entity = await join_and_resolve(doc["link"], doc["label"])
            active_sources[doc["label"]] = entity
        except Exception as e:
            print(f"Failed to load source {doc['label']}: {e}")

    async for doc in destinations_col.find({}):
        try:
            entity = await join_and_resolve(doc["link"], doc["label"])
            active_destinations[doc["label"]] = entity
        except Exception as e:
            print(f"Failed to load destination {doc['label']}: {e}")

    # Load existing source->destination mappings
    async for doc in mappings_col.find({}):
        active_mappings.setdefault(doc["source_label"], set()).add(doc["dest_label"])

    # Register live listener (checks active_sources/mappings dynamically inside handler)
    user_client.add_event_handler(handle_new_message, events.NewMessage())

    print(f"Loaded {len(active_sources)} source(s), {len(active_destinations)} destination(s), "
          f"{sum(len(v) for v in active_mappings.values())} mapping(s).")
    print("Running initial backfill for all linked source->destination pairs...")
    await backfill_all()

    print(f"All backfills complete. Starting automatic missing-post checks every "
          f"{AUTO_CHECK_INTERVAL_HOURS}h, and listening for new posts and bot commands...")
    asyncio.create_task(auto_check_loop())
    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected(),
    )


if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()
    # Outer safety net: if anything unexpected crashes main() (e.g. a startup-time
    # Mongo/Telegram hiccup), wait a bit and restart instead of dying for good.
    while True:
        try:
            loop.run_until_complete(main())
            break  # main() only returns normally if run_until_disconnected() finishes cleanly
        except Exception as e:
            print(f"Fatal error in main(): {e}. Restarting in 15s...")
            loop.run_until_complete(asyncio.sleep(15))

import os
import re
import hashlib
import asyncio
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from telethon import TelegramClient, events
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
CONCURRENCY = int(os.environ.get("CONCURRENCY", 15))

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

# In-memory active state (kept in sync with MongoDB)
active_sources = {}       # label -> entity
active_destinations = {}  # label -> entity
active_mappings = {}      # source_label -> set of dest_labels


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


# ================= Sending =================
async def send_with_retry(message, dest_entity, max_retries=5):
    cleaned = clean_text(message.text)
    for attempt in range(max_retries):
        try:
            if message.media:
                await user_client.send_file(dest_entity, message.media, caption=cleaned or None)
            else:
                if cleaned:
                    await user_client.send_message(dest_entity, cleaned)
            return True
        except FloodWaitError as e:
            wait_time = e.seconds + 5
            print(f"FloodWait hit. Sleeping {wait_time}s before retry...")
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
    limit and skipping if it's a duplicate or already past this destination's progress."""
    async with send_semaphore:
        if message.id <= progress_tracker.get(d_label, 0):
            return  # this destination already has this message

        file_id = get_file_id(message)
        if await already_sent(d_label, file_id):
            # Same content already forwarded to this destination (maybe from
            # another source) — skip sending but still move progress forward.
            await save_progress(source_label, d_label, message.id)
            return

        ok = await send_with_retry(message, d_entity)
        if ok:
            await mark_sent(d_label, file_id)
            await save_progress(source_label, d_label, message.id)
            await log_history(source_label, d_label, message.id, message.text)


async def backfill_source(source_label, source_entity, dest_map):
    """dest_map: {dest_label: entity}. Each (source, destination) pair tracks its own progress.
    Sends to all destinations concurrently (bounded by CONCURRENCY) for speed."""
    if not dest_map:
        return

    progress = {d: await get_progress(source_label, d) for d in dest_map}
    start_id = min(progress.values())

    print(f"[{source_label}] Backfill scan starting from message ID {start_id} "
          f"(covers {len(dest_map)} destination(s), concurrency={CONCURRENCY})...")

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


async def backfill_all():
    for s_label, s_entity in list(active_sources.items()):
        mapped_labels = active_mappings.get(s_label, set())
        dest_map = {d: active_destinations[d] for d in mapped_labels if d in active_destinations}
        await backfill_source(s_label, s_entity, dest_map)


# ================= Live listener =================
async def send_live_pair(source_label, message, d_label, d_entity):
    async with send_semaphore:
        file_id = get_file_id(message)
        if await already_sent(d_label, file_id):
            await save_progress(source_label, d_label, message.id)
            return
        ok = await send_with_retry(message, d_entity)
        if ok:
            await mark_sent(d_label, file_id)
            await save_progress(source_label, d_label, message.id)
            await log_history(source_label, d_label, message.id, message.text)
            print(f"[{source_label} -> {d_label}] Forwarded new message {message.id}")


async def handle_new_message(event):
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


# ================= Bot commands (owner only) =================
def owner_only(func):
    async def wrapper(event):
        if event.sender_id != OWNER_ID:
            return
        await func(event)
    return wrapper


@bot_client.on(events.NewMessage(pattern=r'/addsource (.+)'))
@owner_only
async def cmd_addsource(event):
    link = event.pattern_match.group(1).strip()
    label = await next_label("source")
    try:
        entity = await join_and_resolve(link, label)
    except Exception as e:
        await event.reply(f"❌ Failed to join/resolve: {e}")
        return
    await sources_col.insert_one({"label": label, "link": link, "channel_id": entity.id})
    active_sources[label] = entity
    await event.reply(
        f"✅ Source added as {label}.\n"
        f"It won't forward anywhere yet — link it to a destination with:\n"
        f"/link {label} <destination_label>"
    )


@bot_client.on(events.NewMessage(pattern=r'/adddestination (.+)'))
@owner_only
async def cmd_adddestination(event):
    link = event.pattern_match.group(1).strip()
    label = await next_label("destination")
    try:
        entity = await join_and_resolve(link, label)
    except Exception as e:
        await event.reply(f"❌ Failed to join/resolve: {e}")
        return
    await destinations_col.insert_one({"label": label, "link": link, "channel_id": entity.id})
    active_destinations[label] = entity
    await event.reply(
        f"✅ Destination added as {label}.\n"
        f"No source is sending to it yet — link one with:\n"
        f"/link <source_label> {label}"
    )


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


@bot_client.on(events.NewMessage(pattern=r'/unlink (\S+) (\S+)'))
@owner_only
async def cmd_unlink(event):
    s_label, d_label = event.pattern_match.group(1), event.pattern_match.group(2)
    await mappings_col.delete_one({"source_label": s_label, "dest_label": d_label})
    if s_label in active_mappings:
        active_mappings[s_label].discard(d_label)
    await event.reply(f"🔌 Unlinked {s_label} -> {d_label}. Forwarding stopped for this pair (progress kept).")


@bot_client.on(events.NewMessage(pattern=r'/mappings'))
@owner_only
async def cmd_mappings(event):
    if not active_mappings or not any(active_mappings.values()):
        await event.reply("No links set up yet. Use /link <source_label> <dest_label>")
        return
    lines = []
    for s_label, d_labels in active_mappings.items():
        for d_label in d_labels:
            lines.append(f"{s_label} -> {d_label}")
    await event.reply("🔗 Active mappings:\n" + "\n".join(lines) if lines else "No links set up yet.")


@bot_client.on(events.NewMessage(pattern=r'/removesource (.+)'))
@owner_only
async def cmd_removesource(event):
    label = event.pattern_match.group(1).strip()
    if label in active_sources:
        del active_sources[label]
        active_mappings.pop(label, None)
        await sources_col.delete_one({"label": label})
        await mappings_col.delete_many({"source_label": label})
        await event.reply(f"🗑️ Source {label} removed (forwarding stopped, links cleared). Progress history kept in DB.")
    else:
        await event.reply(f"⚠️ No active source with label {label}.")


@bot_client.on(events.NewMessage(pattern=r'/removedestination (.+)'))
@owner_only
async def cmd_removedestination(event):
    label = event.pattern_match.group(1).strip()
    if label in active_destinations:
        del active_destinations[label]
        for d_labels in active_mappings.values():
            d_labels.discard(label)
        await destinations_col.delete_one({"label": label})
        await mappings_col.delete_many({"dest_label": label})
        await event.reply(f"🗑️ Destination {label} removed (forwarding stopped, links cleared). Progress history kept in DB.")
    else:
        await event.reply(f"⚠️ No active destination with label {label}.")


@bot_client.on(events.NewMessage(pattern=r'/sources'))
@owner_only
async def cmd_sources(event):
    if not active_sources:
        await event.reply("No sources added yet. Use /addsource <link>")
        return
    lines = []
    for label, entity in active_sources.items():
        title = getattr(entity, "title", str(entity.id))
        lines.append(f"{label}: {title} (ID: {entity.id})")
    await event.reply("📥 Sources:\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/destinations'))
@owner_only
async def cmd_destinations(event):
    if not active_destinations:
        await event.reply("No destinations added yet. Use /adddestination <link>")
        return
    lines = []
    for label, entity in active_destinations.items():
        title = getattr(entity, "title", str(entity.id))
        lines.append(f"{label}: {title} (ID: {entity.id})")
    await event.reply("📤 Destinations:\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/status'))
@owner_only
async def cmd_status(event):
    lines = [f"Sources: {len(active_sources)} | Destinations: {len(active_destinations)}", ""]
    any_mapping = False
    for s_label, d_labels in active_mappings.items():
        for d_label in d_labels:
            any_mapping = True
            p = await get_progress(s_label, d_label)
            lines.append(f"{s_label} -> {d_label}: last forwarded ID {p}")
    if not any_mapping:
        lines.append("No links set up yet. Use /link <source_label> <dest_label>")
    await event.reply("📊 Status:\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/history (\S+) (\S+)'))
@owner_only
async def cmd_history(event):
    s_label, d_label = event.pattern_match.group(1), event.pattern_match.group(2)
    cursor = history_col.find({"source_label": s_label, "dest_label": d_label}).sort("ts", -1).limit(5)
    lines = []
    async for doc in cursor:
        lines.append(f"ID {doc['message_id']}: {doc['snippet']}")
    if not lines:
        await event.reply("No history found for this pair yet.")
    else:
        await event.reply(f"🕘 Last forwards ({s_label} -> {d_label}):\n" + "\n".join(lines))


@bot_client.on(events.NewMessage(pattern=r'/start'))
@owner_only
async def cmd_start(event):
    await event.reply(
        "👋 Forwarder control bot ready.\n\n"
        "/addsource <link>\n"
        "/adddestination <link>\n"
        "/link <source_label> <dest_label>\n"
        "/unlink <source_label> <dest_label>\n"
        "/mappings\n"
        "/removesource <label>\n"
        "/removedestination <label>\n"
        "/sources\n"
        "/destinations\n"
        "/status\n"
        "/history <source_label> <dest_label>"
    )


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

    print("All backfills complete. Listening for new posts and bot commands...")
    await asyncio.gather(
        user_client.run_until_disconnected(),
        bot_client.run_until_disconnected(),
    )


if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()
    loop.run_until_complete(main())

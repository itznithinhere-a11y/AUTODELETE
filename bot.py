import asyncio
import logging
import re
import time
from collections import defaultdict, deque

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


# =========================================================
# CONFIG
# =========================================================

# IMPORTANT:
# Apna NEW BotFather token yahan daalo.
BOT_TOKEN = "8845241436:AAHEesCMnaZjVF3QIxUPsSF0HeoM4Gb8GZo"


# =========================================================
# ADMIN CONFIG
# =========================================================

# Yahan apna Telegram numeric user ID daalo.
#
# Example:
# ADMIN_IDS = {
#     123456789,
#     987654321,
# }
#
# Multiple admins bhi add kar sakte ho.

ADMIN_IDS = {
    6594401737,
}


# =========================================================
# SETTINGS
# =========================================================

# Maximum /delete duration
MAX_TRACK_DAYS = 30

TRACK_SECONDS = MAX_TRACK_DAYS * 24 * 60 * 60

# Delay between delete requests
DELETE_DELAY = 0.05

# Worker check interval
JOB_CHECK_INTERVAL = 1

# Broadcast delay
# Telegram flood-limit avoid karne ke liye
BROADCAST_DELAY = 0.05


# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("DeleteBot")


# =========================================================
# TELEGRAM
# =========================================================

bot = Bot(token=BOT_TOKEN)

dp = Dispatcher()

router = Router()

dp.include_router(router)


# =========================================================
# MEMORY STORAGE
# =========================================================

# ---------------------------------------------------------
# GROUP MESSAGE HISTORY
# ---------------------------------------------------------
#
# chat_id -> deque([
#     {
#         "message_id": 123,
#         "timestamp": 1234567890
#     }
# ])
#
# ---------------------------------------------------------

message_history = defaultdict(deque)

delete_locks = defaultdict(asyncio.Lock)

pending_jobs = deque()


# =========================================================
# BROADCAST USER STORAGE
# =========================================================
#
# RAM ONLY
#
# User /start karega to uska ID yahan store hoga.
#
# Bot restart hone ke baad users reset ho jayenge.
#
# =========================================================

broadcast_users = set()


# =========================================================
# BROADCAST LOCK
# =========================================================

broadcast_lock = asyncio.Lock()


# =========================================================
# DURATION PARSER
# =========================================================

def parse_duration(value: str):

    value = value.lower().strip()

    match = re.fullmatch(
        r"(\d+)([mhd])",
        value
    )

    if not match:
        return None

    number = int(match.group(1))

    unit = match.group(2)

    if number <= 0:
        return None

    if unit == "m":
        seconds = number * 60

    elif unit == "h":
        seconds = number * 60 * 60

    elif unit == "d":
        seconds = number * 24 * 60 * 60

    else:
        return None

    if seconds > TRACK_SECONDS:
        return None

    return seconds


# =========================================================
# ADD MESSAGE TO MEMORY
# =========================================================

def add_message_to_history(
    chat_id: int,
    message_id: int,
    message_time: float,
):

    history = message_history[chat_id]

    history.append(
        {
            "message_id": message_id,
            "timestamp": message_time,
        }
    )

    cutoff = message_time - TRACK_SECONDS

    while history and history[0]["timestamp"] < cutoff:
        history.popleft()


# =========================================================
# GET MESSAGES FOR DELETE RANGE
# =========================================================

def get_messages_for_range(
    chat_id: int,
    start_time: float,
    end_time: float,
):

    history = message_history.get(
        chat_id,
        deque()
    )

    result = []

    for item in history:

        timestamp = item["timestamp"]

        if start_time <= timestamp <= end_time:

            result.append(item)

    return result


# =========================================================
# CHECK ADMIN
# =========================================================

def is_admin(user_id: int):

    return user_id in ADMIN_IDS


# =========================================================
# DELETE JOB
# =========================================================

async def process_delete_job(job):

    chat_id = job["chat_id"]

    start_time = job["start_time"]

    end_time = job["end_time"]

    duration_text = job["duration_text"]

    logger.info(
        "DELETE JOB START | chat=%s | duration=%s",
        chat_id,
        duration_text,
    )

    async with delete_locks[chat_id]:

        rows = get_messages_for_range(
            chat_id=chat_id,
            start_time=start_time,
            end_time=end_time,
        )

        total = len(rows)

        logger.info(
            "MESSAGES FOUND | chat=%s | total=%s",
            chat_id,
            total,
        )

        deleted = 0

        failed = 0

        successfully_deleted_ids = set()

        # -------------------------------------------------
        # DELETE
        # -------------------------------------------------

        for index, row in enumerate(rows, start=1):

            message_id = row["message_id"]

            try:

                await bot.delete_message(
                    chat_id=chat_id,
                    message_id=message_id,
                )

                deleted += 1

                successfully_deleted_ids.add(
                    message_id
                )

                logger.info(
                    "DELETED | chat=%s | msg=%s | %s/%s",
                    chat_id,
                    message_id,
                    index,
                    total,
                )

            except Exception as e:

                failed += 1

                logger.warning(
                    "DELETE FAILED | chat=%s | msg=%s | %s",
                    chat_id,
                    message_id,
                    e,
                )

            await asyncio.sleep(
                DELETE_DELAY
            )

        # -------------------------------------------------
        # REMOVE ONLY SUCCESSFULLY DELETED MESSAGES
        # -------------------------------------------------

        history = message_history.get(
            chat_id
        )

        if history and successfully_deleted_ids:

            remaining = deque()

            for item in history:

                if item["message_id"] not in successfully_deleted_ids:

                    remaining.append(item)

            message_history[chat_id] = remaining

        # -------------------------------------------------
        # RESULT
        # -------------------------------------------------

        logger.info(
            "DELETE JOB COMPLETE | chat=%s | total=%s | deleted=%s | failed=%s",
            chat_id,
            total,
            deleted,
            failed,
        )

        return deleted, failed


# =========================================================
# DELETE WORKER
# =========================================================

async def delete_worker():

    logger.info(
        "DELETE WORKER STARTED"
    )

    while True:

        try:

            if pending_jobs:

                job = pending_jobs.popleft()

                try:

                    deleted, failed = await process_delete_job(
                        job
                    )

                    logger.info(
                        "JOB RESULT | deleted=%s | failed=%s",
                        deleted,
                        failed,
                    )

                except Exception as e:

                    logger.exception(
                        "DELETE JOB ERROR | chat=%s | %s",
                        job["chat_id"],
                        e,
                    )

            else:

                await asyncio.sleep(
                    JOB_CHECK_INTERVAL
                )

        except Exception as e:

            logger.exception(
                "WORKER ERROR | %s",
                e,
            )

            await asyncio.sleep(2)


# =========================================================
# START COMMAND
# =========================================================

@router.message(CommandStart())
async def start_handler(
    message: Message
):

    # -----------------------------------------------------
    # SAVE USER FOR BROADCAST
    # -----------------------------------------------------

    if message.from_user:

        broadcast_users.add(
            message.from_user.id
        )

        logger.info(
            "USER REGISTERED | user=%s | total=%s",
            message.from_user.id,
            len(broadcast_users),
        )

    # -----------------------------------------------------
    # ADD TO GROUP URL
    # -----------------------------------------------------

    me = await bot.get_me()

    add_url = (
        f"https://t.me/{me.username}"
        f"?startgroup=true"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Add Me to Group",
                    url=add_url,
                )
            ],
            [
                InlineKeyboardButton(
                    text="📖 Help",
                    callback_data="help",
                )
            ],
        ]
    )

    await message.answer(

        "🤖 Message Delete Bot\n\n"

        "👋 Welcome!\n\n"

        "Group me bot add karo aur "
        "Administrator + Delete Messages "
        "permission do.\n\n"

        "Uske baad group me koi bhi user "
        "delete command use kar sakta hai.\n\n"

        "🗑 Delete Commands\n\n"

        "🗑 /delete 5m\n"
        "🗑 /delete 10m\n"
        "🗑 /delete 1h\n"
        "🗑 /delete 2h\n"
        "🗑 /delete 1d\n"
        "🗑 /delete 7d\n\n"

        "✨ Custom Duration\n\n"

        "🔹 /delete 7m\n"
        "🔹 /delete 37m\n"
        "🔹 /delete 13h\n"
        "🔹 /delete 5d\n\n"

        f"📌 Maximum: {MAX_TRACK_DAYS} days\n\n"

        "⚡ Messages RAM me temporarily track hote hain.\n"
        "⚠️ Bot restart hone par old tracking reset ho jayegi.\n\n"

        "📢 Broadcast: Admin broadcast system enabled.",

        reply_markup=keyboard,
    )


# =========================================================
# HELP COMMAND
# =========================================================

@router.message(Command("help"))
async def help_handler(
    message: Message
):

    await message.answer(

        "📖 Message Delete Bot Help\n\n"

        "🗑 COMMAND\n"
        "/delete TIME\n\n"

        "Examples:\n"

        "/delete 1m → 1 minute\n"
        "/delete 5m → 5 minutes\n"
        "/delete 10m → 10 minutes\n"
        "/delete 30m → 30 minutes\n\n"

        "/delete 1h → 1 hour\n"
        "/delete 2h → 2 hours\n"
        "/delete 10h → 10 hours\n\n"

        "/delete 1d → 1 day\n"
        "/delete 2d → 2 days\n"
        "/delete 7d → 7 days\n"
        "/delete 30d → 30 days\n\n"

        "✨ Custom Values\n\n"

        "🔹 /delete 7m\n"
        "🔹 /delete 17m\n"
        "🔹 /delete 43m\n"
        "🔹 /delete 3h\n"
        "🔹 /delete 11h\n"
        "🔹 /delete 5d\n\n"

        "📌 Units\n"
        "🕐 m = minutes\n"
        "🕐 h = hours\n"
        "🕐 d = days\n\n"

        "📦 Text, photo, video, sticker, GIF, "
        "document, audio aur supported messages "
        "delete kiye ja sakte hain.\n\n"

        "⚠️ Bot ko Administrator + "
        "Delete Messages permission chahiye.\n\n"

        "📢 Admin: /broadcast\n"
        "📊 Admin: /stats\n\n"

        "⚠️ Bot restart hone par old message tracking "
        "aur broadcast users reset ho jayenge."
    )


# =========================================================
# HELP BUTTON
# =========================================================

@router.callback_query(F.data == "help")
async def help_button_handler(
    callback
):

    await callback.answer()

    await callback.message.answer(

        "📖 Quick Help\n\n"

        "🗑 /delete 5m → 5 minutes\n"
        "🗑 /delete 1h → 1 hour\n"
        "🗑 /delete 1d → 1 day\n"
        "🗑 /delete 7d → 7 days\n\n"

        "✨ Custom Time\n"

        "🔹 /delete 17m\n"
        "🔹 /delete 3h\n"
        "🔹 /delete 5d\n\n"

        "⚠️ Bot ko Administrator + "
        "Delete Messages permission chahiye."
    )


# =========================================================
# TRACK GROUP MESSAGES
# =========================================================

@router.message(
    F.chat.type.in_({
        "group",
        "supergroup"
    }),
    ~F.text.startswith("/"),
)
async def track_group_message(
    message: Message
):

    chat_id = message.chat.id

    message_id = message.message_id

    now = time.time()

    add_message_to_history(
        chat_id=chat_id,
        message_id=message_id,
        message_time=now,
    )

    logger.debug(
        "TRACKED | chat=%s | msg=%s",
        chat_id,
        message_id,
    )


# =========================================================
# DELETE COMMAND
# =========================================================

@router.message(
    Command("delete"),
    F.chat.type.in_({
        "group",
        "supergroup"
    }),
)
async def delete_handler(
    message: Message
):

    chat_id = message.chat.id

    # -----------------------------------------------------
    # ARGUMENT
    # -----------------------------------------------------

    if not message.text:

        return

    args = message.text.split()

    if len(args) < 2:

        await message.reply(

            "❌ Time missing.\n\n"

            "Examples:\n"

            "/delete 5m\n"
            "/delete 1h\n"
            "/delete 1d\n"
            "/delete 7d"
        )

        return

    duration_text = (
        args[1]
        .lower()
        .strip()
    )

    seconds = parse_duration(
        duration_text
    )

    if seconds is None:

        await message.reply(

            "❌ Invalid time.\n\n"

            "Use:\n"
            "• m = minutes\n"
            "• h = hours\n"
            "• d = days\n\n"

            "Examples:\n"

            "🗑 /delete 7m\n"
            "🗑 /delete 2h\n"
            "🗑 /delete 1d\n"
            "🗑 /delete 7d\n\n"

            f"📌 Maximum: {MAX_TRACK_DAYS}d"
        )

        return

    # -----------------------------------------------------
    # COMMAND TIMESTAMP
    # -----------------------------------------------------

    command_time = time.time()

    start_time = (
        command_time - seconds
    )

    # -----------------------------------------------------
    # CHECK BOT ADMIN
    # -----------------------------------------------------

    try:

        me = await bot.get_me()

        member = await bot.get_chat_member(
            chat_id=chat_id,
            user_id=me.id,
        )

        if member.status not in (
            "administrator",
            "creator"
        ):

            await message.reply(

                "❌ Bot admin nahi hai.\n\n"

                "Bot ko Administrator banao aur "
                "Delete Messages permission ON karo."
            )

            return

        if member.status == "administrator":

            can_delete = getattr(
                member,
                "can_delete_messages",
                False
            )

            if not can_delete:

                await message.reply(

                    "❌ Bot ke paas "
                    "Delete Messages "
                    "permission nahi hai."
                )

                return

    except Exception as e:

        logger.exception(
            "PERMISSION CHECK ERROR | %s",
            e,
        )

        await message.reply(
            "❌ Bot permission check nahi ho paayi."
        )

        return

    # -----------------------------------------------------
    # SAVE /DELETE COMMAND ITSELF
    # -----------------------------------------------------

    add_message_to_history(
        chat_id=chat_id,
        message_id=message.message_id,
        message_time=command_time,
    )

    # -----------------------------------------------------
    # CREATE MEMORY JOB
    # -----------------------------------------------------

    job = {

        "chat_id": chat_id,

        "requested_by": (
            message.from_user.id
            if message.from_user
            else 0
        ),

        "duration_text": duration_text,

        "duration_seconds": seconds,

        "start_time": start_time,

        "end_time": command_time,
    }

    pending_jobs.append(job)

    # -----------------------------------------------------
    # CONFIRM
    # -----------------------------------------------------

    await message.reply(

        "🗑 Delete request received\n\n"

        f"⏱ Range: {duration_text}\n"

        "⚡ Delete process queue me hai.\n\n"

        "📌 Command ke baad aane wale "
        "messages is request me delete nahi honge."
    )

    logger.info(

        "DELETE REQUEST | chat=%s | duration=%s | start=%s | end=%s",

        chat_id,

        duration_text,

        start_time,

        command_time,
    )


# =========================================================
# PRIVATE DELETE COMMAND
# =========================================================

@router.message(
    Command("delete")
)
async def private_delete_handler(
    message: Message
):

    if message.chat.type == "private":

        await message.answer(

            "ℹ️ /delete group/supergroup me use karo.\n\n"

            "Example:\n"

            "/delete 5m"
        )


# =========================================================
# BROADCAST COMMAND
# =========================================================
#
# METHOD 1:
#
# /broadcast Hello everyone
#
# METHOD 2:
#
# Kisi message ko reply karo:
# /broadcast
#
# Isse replied message broadcast hoga.
#
# =========================================================

@router.message(
    Command("broadcast"),
    F.chat.type == "private",
)
async def broadcast_handler(
    message: Message
):

    # -----------------------------------------------------
    # ADMIN CHECK
    # -----------------------------------------------------

    if not message.from_user:

        return

    admin_id = message.from_user.id

    if not is_admin(admin_id):

        await message.answer(
            "❌ Unauthorized.\n\n"
            "Sirf bot admin broadcast use kar sakta hai."
        )

        return

    # -----------------------------------------------------
    # CHECK USERS
    # -----------------------------------------------------

    if not broadcast_users:

        await message.answer(

            "⚠️ No users found.\n\n"

            "Abhi kisi user ne bot me "
            "/start nahi kiya."
        )

        return

    # -----------------------------------------------------
    # DETERMINE BROADCAST MESSAGE
    # -----------------------------------------------------

    source_message = None

    # -----------------------------------------------------
    # REPLY MODE
    # -----------------------------------------------------

    if message.reply_to_message:

        source_message = message.reply_to_message

    # -----------------------------------------------------
    # TEXT MODE
    # -----------------------------------------------------

    elif message.text:

        parts = message.text.split(
            maxsplit=1
        )

        if len(parts) >= 2:

            broadcast_text = parts[1].strip()

            if broadcast_text:

                # Text broadcast handled separately below
                source_message = None

            else:

                broadcast_text = None

        else:

            broadcast_text = None

    else:

        broadcast_text = None

    # -----------------------------------------------------
    # TEXT VALUE
    # -----------------------------------------------------

    if not message.reply_to_message:

        if message.text:

            parts = message.text.split(
                maxsplit=1
            )

            if len(parts) >= 2:

                broadcast_text = parts[1].strip()

            else:

                broadcast_text = None

        else:

            broadcast_text = None

    else:

        broadcast_text = None

    # -----------------------------------------------------
    # NOTHING PROVIDED
    # -----------------------------------------------------

    if source_message is None and not broadcast_text:

        await message.answer(

            "📢 Broadcast Usage\n\n"

            "Text:\n"
            "/broadcast Hello everyone\n\n"

            "Photo / Video / Document / Audio:\n"
            "1. Pehle message send karo\n"
            "2. Us message ko reply karo\n"
            "3. /broadcast bhejo"
        )

        return

    # -----------------------------------------------------
    # COPY USER LIST
    # -----------------------------------------------------

    users = list(
        broadcast_users
    )

    total = len(users)

    sent = 0

    failed = 0

    blocked_users = []

    # -----------------------------------------------------
    # START MESSAGE
    # -----------------------------------------------------

    progress_message = await message.answer(

        "📢 Broadcast Started\n\n"

        f"👥 Users: {total}\n"
        "✅ Sent: 0\n"
        "❌ Failed: 0\n"
        "⏳ Progress: 0%"
    )

    # -----------------------------------------------------
    # LOCK
    # -----------------------------------------------------

    async with broadcast_lock:

        for index, user_id in enumerate(
            users,
            start=1
        ):

            try:

                # -------------------------------------------------
                # COPY REPLIED MESSAGE
                # -------------------------------------------------

                if source_message:

                    await bot.copy_message(

                        chat_id=user_id,

                        from_chat_id=source_message.chat.id,

                        message_id=source_message.message_id,
                    )

                # -------------------------------------------------
                # SEND TEXT
                # -------------------------------------------------

                else:

                    await bot.send_message(

                        chat_id=user_id,

                        text=broadcast_text,
                    )

                sent += 1

                logger.info(

                    "BROADCAST SENT | user=%s | %s/%s",

                    user_id,

                    index,

                    total,
                )

            except Exception as e:

                failed += 1

                blocked_users.append(
                    user_id
                )

                logger.warning(

                    "BROADCAST FAILED | user=%s | %s",

                    user_id,

                    e,
                )

            # -------------------------------------------------
            # PROGRESS UPDATE
            # -------------------------------------------------

            if (
                index == 1
                or index % 10 == 0
                or index == total
            ):

                percent = int(
                    (index / total) * 100
                )

                try:

                    await progress_message.edit_text(

                        "📢 Broadcasting...\n\n"

                        f"👥 Total: {total}\n"
                        f"📨 Processed: {index}\n"
                        f"✅ Sent: {sent}\n"
                        f"❌ Failed: {failed}\n"
                        f"⏳ Progress: {percent}%"
                    )

                except Exception:

                    pass

            await asyncio.sleep(
                BROADCAST_DELAY
            )

    # -----------------------------------------------------
    # REMOVE FAILED USERS
    # -----------------------------------------------------

    for user_id in blocked_users:

        broadcast_users.discard(
            user_id
        )

    # -----------------------------------------------------
    # FINAL RESULT
    # -----------------------------------------------------

    await progress_message.edit_text(

        "✅ Broadcast Completed\n\n"

        f"👥 Total: {total}\n"
        f"📨 Sent: {sent}\n"
        f"❌ Failed: {failed}\n"
        f"🚫 Removed: {len(blocked_users)}\n\n"

        f"📊 Current Users: {len(broadcast_users)}"
    )

    logger.info(

        "BROADCAST COMPLETE | total=%s | sent=%s | failed=%s",

        total,

        sent,

        failed,
    )


# =========================================================
# STATS COMMAND
# =========================================================

@router.message(
    Command("stats"),
    F.chat.type == "private",
)
async def stats_handler(
    message: Message
):

    if not message.from_user:

        return

    if not is_admin(
        message.from_user.id
    ):

        await message.answer(
            "❌ Unauthorized."
        )

        return

    await message.answer(

        "📊 Bot Statistics\n\n"

        f"👥 Broadcast Users: "
        f"{len(broadcast_users)}\n\n"

        f"🗑 Pending Delete Jobs: "
        f"{len(pending_jobs)}\n\n"

        f"💾 Storage: RAM Only\n"

        f"⏱ Max Tracking: "
        f"{MAX_TRACK_DAYS} days"
    )


# =========================================================
# MAIN
# =========================================================

async def main():

    if (
        not BOT_TOKEN
        or BOT_TOKEN == "PUT_YOUR_NEW_BOT_TOKEN_HERE"
    ):

        raise RuntimeError(

            "BOT_TOKEN me apna NEW Telegram "
            "BotFather token daalo."
        )

    # -----------------------------------------------------
    # CHECK ADMIN CONFIG
    # -----------------------------------------------------

    if not ADMIN_IDS:

        raise RuntimeError(

            "ADMIN_IDS me apna Telegram numeric "
            "user ID daalo."
        )

    # -----------------------------------------------------
    # BOT INFO
    # -----------------------------------------------------

    me = await bot.get_me()

    logger.info(
        "========================================"
    )

    logger.info(
        "BOT CONNECTED: @%s",
        me.username,
    )

    logger.info(
        "STORAGE: RAM ONLY"
    )

    logger.info(
        "BROADCAST USERS: %s",
        len(broadcast_users),
    )

    logger.info(
        "MAX TRACK: %s DAYS",
        MAX_TRACK_DAYS,
    )

    logger.info(
        "========================================"
    )

    # -----------------------------------------------------
    # START DELETE WORKER
    # -----------------------------------------------------

    worker_task = asyncio.create_task(
        delete_worker()
    )

    try:

        await dp.start_polling(

            bot,

            allowed_updates=dp.resolve_used_update_types(),
        )

    finally:

        worker_task.cancel()

        try:

            await worker_task

        except asyncio.CancelledError:

            pass

        await bot.session.close()


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "BOT STOPPED"
        )
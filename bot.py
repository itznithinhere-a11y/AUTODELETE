import asyncio
import logging
import re
import time
from collections import defaultdict

import httpx

from aiogram import Bot, Dispatcher, Router, F
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramUnauthorizedError,
)
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)


# =========================================================
# CONFIG
# =========================================================

# IMPORTANT:
# Apna NEW regenerated Telegram bot token yahan lagao.
BOT_TOKEN = "PUT_NEW_BOT_TOKEN_HERE"

SUPABASE_URL = "https://isgnfbbxkarlomtueyzd.supabase.co"

# IMPORTANT:
# Apni NEW regenerated Supabase SERVICE ROLE KEY yahan lagao.
SUPABASE_SERVICE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlzZ25mYmJ4a2FybG9tdHVleXpkIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4OTMwMTIzMSwiZXhwIjoyMTA0ODc3MjMxfQ.JkHK0cbvl6olJY817BPpiWM9xkMo-lWgbO7gcc3NQzI"

ADMIN_IDS = {
    6594401737,
}

UPDATE_CHANNEL_URL = "https://t.me/YOUR_UPDATE_CHANNEL"


# =========================================================
# GENERAL CONFIG
# =========================================================

MAX_TRACK_DAYS = 30
TRACK_SECONDS = MAX_TRACK_DAYS * 24 * 60 * 60

# Message DB batching
MESSAGE_BATCH_SIZE = 100
MESSAGE_BATCH_INTERVAL = 0.25
MESSAGE_QUEUE_MAX = 20000

# Telegram deletion
DELETE_DELAY = 0.05

# Maximum simultaneous delete jobs
MAX_PARALLEL_DELETE_JOBS = 10

# Job checking
JOB_CHECK_INTERVAL = 1.0

# Broadcast
BROADCAST_DELAY = 0.05
BROADCAST_PROGRESS_EVERY = 10

# Poll reconnect
POLL_RETRY_MIN = 2
POLL_RETRY_MAX = 30


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

dp = Dispatcher()
router = Router()

dp.include_router(router)


# =========================================================
# RUNTIME STORAGE
# =========================================================

delete_locks = defaultdict(asyncio.Lock)

job_semaphore = asyncio.Semaphore(
    MAX_PARALLEL_DELETE_JOBS
)

broadcast_lock = asyncio.Lock()

message_queue = asyncio.Queue(
    maxsize=MESSAGE_QUEUE_MAX
)

active_jobs = set()

# Jobs which are requested to stop.
# job_id -> True
cancelled_jobs = set()


# =========================================================
# SUPABASE CLIENT
# =========================================================

supabase_client = None


def create_supabase_client():

    return httpx.AsyncClient(
        base_url=f"{SUPABASE_URL.rstrip('/')}/rest/v1",
        headers={
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        timeout=httpx.Timeout(
            connect=10,
            read=60,
            write=60,
            pool=60,
        ),
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=30,
        ),
    )


# =========================================================
# DATABASE REQUEST
# =========================================================

async def db_request(
    method: str,
    path: str,
    *,
    params=None,
    json_data=None,
    headers=None,
):
    """
    Async Supabase REST request.
    """

    if supabase_client is None:
        raise RuntimeError(
            "Supabase client is not initialized."
        )

    request_headers = {}

    if headers:
        request_headers.update(headers)

    last_error = None

    for attempt in range(5):

        try:

            response = await supabase_client.request(
                method=method,
                url=f"/{path}",
                params=params,
                json=json_data,
                headers=request_headers,
            )

            # Success
            if 200 <= response.status_code < 300:

                if not response.content:
                    return []

                try:
                    return response.json()
                except Exception:
                    return []

            # Rate limit
            if response.status_code == 429:

                logger.warning(
                    "SUPABASE RATE LIMIT | path=%s | attempt=%s",
                    path,
                    attempt + 1,
                )

                await asyncio.sleep(
                    min(2 + attempt, 10)
                )

                continue

            # Server error
            if response.status_code >= 500:

                logger.warning(
                    "SUPABASE SERVER ERROR | path=%s | status=%s | attempt=%s",
                    path,
                    response.status_code,
                    attempt + 1,
                )

                await asyncio.sleep(
                    min(1 + attempt, 5)
                )

                continue

            raise RuntimeError(
                f"Supabase HTTP {response.status_code}: "
                f"{response.text[:1500]}"
            )

        except (
            httpx.TimeoutException,
            httpx.NetworkError,
        ) as e:

            last_error = e

            logger.warning(
                "SUPABASE NETWORK ERROR | path=%s | attempt=%s | %s",
                path,
                attempt + 1,
                e,
            )

            if attempt < 4:
                await asyncio.sleep(
                    min(1 + attempt, 5)
                )

    raise RuntimeError(
        f"Supabase request failed: {last_error}"
    )


# =========================================================
# DATABASE HELPERS
# =========================================================

async def db_insert(
    table,
    data,
    *,
    upsert=False,
    return_data=False,
):
    prefer_parts = []

    if upsert:
        prefer_parts.append(
            "resolution=merge-duplicates"
        )

    if return_data:
        prefer_parts.append(
            "return=representation"
        )
    else:
        prefer_parts.append(
            "return=minimal"
        )

    return await db_request(
        "POST",
        table,
        json_data=data,
        headers={
            "Prefer": ",".join(
                prefer_parts
            )
        },
    )


async def db_select(
    table,
    params=None,
):
    return await db_request(
        "GET",
        table,
        params=params,
    )


async def db_update(
    table,
    params,
    data,
):
    return await db_request(
        "PATCH",
        table,
        params=params,
        json_data=data,
        headers={
            "Prefer": "return=minimal",
        },
    )


async def db_delete(
    table,
    params,
):
    return await db_request(
        "DELETE",
        table,
        params=params,
        headers={
            "Prefer": "return=minimal",
        },
    )


# =========================================================
# HELPERS
# =========================================================

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def parse_duration(value: str):

    if not value:
        return None

    value = value.lower().strip()

    match = re.fullmatch(
        r"(\d+)([mhd])",
        value,
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

    else:
        seconds = number * 24 * 60 * 60

    if seconds > TRACK_SECONDS:
        return None

    return seconds


def utc_iso(timestamp: float) -> str:

    return time.strftime(
        "%Y-%m-%dT%H:%M:%SZ",
        time.gmtime(timestamp),
    )


def get_media_type(message: Message) -> str:

    if message.video:
        return "video"

    if message.photo:
        return "photo"

    if message.document:
        return "document"

    if message.audio:
        return "audio"

    if message.voice:
        return "voice"

    if message.video_note:
        return "video_note"

    if message.animation:
        return "animation"

    if message.sticker:
        return "sticker"

    if message.contact:
        return "contact"

    if message.location:
        return "location"

    if message.poll:
        return "poll"

    if message.dice:
        return "dice"

    if message.text:
        return "text"

    return "other"


async def safe_sleep(seconds):

    await asyncio.sleep(
        max(0, seconds)
    )


# =========================================================
# MAIN UI
# =========================================================

MAIN_TEXT = (
    "🤖 Message Delete Bot\n\n"
    "👋 Welcome!\n\n"
    "Group me bot add karo aur "
    "Administrator + Delete Messages "
    "permission do.\n\n"
    "Uske baad group me:\n"
    "/delete 10m\n"
    "/delete 1h\n"
    "/delete 1d\n\n"
    "🛑 Running delete stop karne ke liye:\n"
    "/stopdelete\n\n"
    "📌 Maximum: 30 days"
)


GUIDE_TEXT = (
    "📖 Guide\n\n"
    "1️⃣ Bot ko group me add karo.\n\n"
    "2️⃣ Bot ko Administrator banao.\n\n"
    "3️⃣ Delete Messages permission ON karo.\n\n"
    "4️⃣ BotFather me /setprivacy ko "
    "Disable karo.\n\n"
    "5️⃣ Group me command use karo:\n"
    "/delete 10m\n\n"
    "Custom examples:\n"
    "/delete 37m\n"
    "/delete 13h\n"
    "/delete 5d\n\n"
    "🛑 Delete process stop karne ke liye:\n"
    "/stopdelete\n\n"
    "📌 Maximum: 30 days\n\n"
    "⚡ Message tracking Supabase me save hoti hai.\n"
    "🔄 Bot restart ke baad data safe rahega."
)


def main_keyboard(bot_username: str):

    add_url = (
        f"https://t.me/"
        f"{bot_username}"
        f"?startgroup=true"
    )

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Add Me to Group",
                    url=add_url,
                ),
                InlineKeyboardButton(
                    text="📢 Update Channel",
                    url=UPDATE_CHANNEL_URL,
                ),
            ],
            [
                InlineKeyboardButton(
                    text="📖 Guide",
                    callback_data="guide",
                )
            ],
        ]
    )


def guide_keyboard():

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🔙 Back",
                    callback_data="back_main",
                )
            ]
        ]
    )


# =========================================================
# SAVE USER
# =========================================================

async def save_user(message: Message):

    if not message.from_user:
        return

    user = message.from_user

    try:

        await db_insert(
            "bot_users",
            {
                "user_id": user.id,
            },
            upsert=True,
        )

    except Exception:

        logger.exception(
            "SAVE USER ERROR | user=%s",
            user.id,
        )


# =========================================================
# START
# =========================================================

@router.message(CommandStart())
async def start_handler(message: Message):

    if not message.from_user:
        return

    await save_user(message)

    try:

        bot_user = await message.bot.get_me()

        if not bot_user.username:

            await message.answer(
                "❌ Bot username available nahi hai."
            )

            return

        await message.answer(
            MAIN_TEXT,
            reply_markup=main_keyboard(
                bot_user.username
            ),
        )

    except Exception:

        logger.exception(
            "START HANDLER ERROR"
        )


# =========================================================
# GUIDE
# =========================================================

@router.callback_query(
    F.data == "guide"
)
async def guide_button_handler(
    callback: CallbackQuery,
):

    await callback.answer()

    if callback.message:

        await callback.message.edit_text(
            GUIDE_TEXT,
            reply_markup=guide_keyboard(),
        )


# =========================================================
# BACK
# =========================================================

@router.callback_query(
    F.data == "back_main"
)
async def back_main_handler(
    callback: CallbackQuery,
):

    await callback.answer()

    if not callback.message:
        return

    bot_user = await callback.bot.get_me()

    if bot_user.username:

        await callback.message.edit_text(
            MAIN_TEXT,
            reply_markup=main_keyboard(
                bot_user.username
            ),
        )


# =========================================================
# MESSAGE QUEUE
# =========================================================

async def queue_message_for_db(
    message: Message,
):

    sender_id = (
        message.from_user.id
        if message.from_user
        else 0
    )

    item = {
        "chat_id": message.chat.id,
        "message_id": message.message_id,
        "sender_id": sender_id,
        "media_type": get_media_type(message),
        "message_time": utc_iso(
            message.date.timestamp()
        ),
    }

    try:

        message_queue.put_nowait(
            item
        )

    except asyncio.QueueFull:

        logger.error(
            "MESSAGE QUEUE FULL | chat=%s | msg=%s",
            message.chat.id,
            message.message_id,
        )


# =========================================================
# MESSAGE DB WORKER
# =========================================================

async def message_db_worker():

    logger.info(
        "MESSAGE DB WORKER STARTED"
    )

    batch = []

    while True:

        try:

            try:

                item = await asyncio.wait_for(
                    message_queue.get(),
                    timeout=MESSAGE_BATCH_INTERVAL,
                )

                batch.append(item)

            except asyncio.TimeoutError:

                pass

            if (
                batch
                and (
                    len(batch)
                    >= MESSAGE_BATCH_SIZE
                    or message_queue.empty()
                )
            ):

                current_batch = batch
                batch = []

                try:

                    await db_insert(
                        "tracked_messages",
                        current_batch,
                    )

                    logger.debug(
                        "MESSAGE BATCH SAVED | count=%s",
                        len(current_batch),
                    )

                except Exception:

                    logger.exception(
                        "MESSAGE BATCH SAVE ERROR"
                    )

                    for item in current_batch:

                        try:

                            await db_insert(
                                "tracked_messages",
                                item,
                            )

                        except Exception:

                            logger.exception(
                                "MESSAGE SAVE FAILED | chat=%s | msg=%s",
                                item["chat_id"],
                                item["message_id"],
                            )

                finally:

                    for _ in current_batch:

                        try:
                            message_queue.task_done()
                        except Exception:
                            pass

        except asyncio.CancelledError:

            if batch:

                try:

                    await db_insert(
                        "tracked_messages",
                        batch,
                    )

                except Exception:

                    logger.exception(
                        "FINAL MESSAGE FLUSH ERROR"
                    )

            raise

        except Exception:

            logger.exception(
                "MESSAGE DB WORKER ERROR"
            )

            await asyncio.sleep(1)


# =========================================================
# TRACK GROUP MESSAGES
# =========================================================

@router.message(
    F.chat.type.in_({
        "group",
        "supergroup",
    }),
    ~F.text.startswith("/"),
)
async def track_group_message(
    message: Message,
):

    await queue_message_for_db(
        message
    )


# =========================================================
# PROTECTED MESSAGES
# =========================================================

async def get_protected_message_ids(
    bot: Bot,
    chat_id: int,
):

    admin_ids = set()
    pinned_ids = set()

    # -----------------------------------------------------
    # Administrators
    # -----------------------------------------------------

    try:

        admins = await bot.get_chat_administrators(
            chat_id
        )

        admin_ids = {
            member.user.id
            for member in admins
            if member.status in {
                "administrator",
                "creator",
            }
        }

    except Exception:

        logger.exception(
            "GET ADMINS ERROR | chat=%s",
            chat_id,
        )

    # -----------------------------------------------------
    # Pinned message
    # -----------------------------------------------------

    try:

        chat = await bot.get_chat(
            chat_id
        )

        pinned = getattr(
            chat,
            "pinned_message",
            None,
        )

        if pinned:

            pinned_ids.add(
                pinned.message_id
            )

    except Exception:

        logger.exception(
            "GET PINNED ERROR | chat=%s",
            chat_id,
        )

    return admin_ids, pinned_ids


# =========================================================
# DELETE ONE MESSAGE
# =========================================================

async def delete_one_message(
    bot: Bot,
    chat_id: int,
    message_id: int,
):

    while True:

        try:

            await bot.delete_message(
                chat_id=chat_id,
                message_id=message_id,
            )

            return True

        except TelegramRetryAfter as e:

            wait = (
                float(e.retry_after)
                + 0.5
            )

            logger.warning(
                "DELETE FLOOD WAIT | chat=%s | msg=%s | wait=%.1f",
                chat_id,
                message_id,
                wait,
            )

            await safe_sleep(
                wait
            )

        except TelegramNetworkError as e:

            logger.warning(
                "DELETE NETWORK ERROR | chat=%s | msg=%s | %s",
                chat_id,
                message_id,
                e,
            )

            await safe_sleep(2)

        except TelegramForbiddenError as e:

            logger.warning(
                "DELETE FORBIDDEN | chat=%s | msg=%s | %s",
                chat_id,
                message_id,
                e,
            )

            return False

        except TelegramBadRequest as e:

            logger.warning(
                "DELETE BAD REQUEST | chat=%s | msg=%s | %s",
                chat_id,
                message_id,
                e,
            )

            return False

        except Exception:

            logger.exception(
                "DELETE UNKNOWN ERROR | chat=%s | msg=%s",
                chat_id,
                message_id,
            )

            return False


# =========================================================
# CHECK IF JOB IS CANCELLED
# =========================================================

async def is_job_cancelled(job_id):

    # Fast local check
    if job_id in cancelled_jobs:
        return True

    # Persistent DB check
    try:

        rows = await db_select(
            "delete_jobs",
            params=[
                (
                    "id",
                    f"eq.{job_id}",
                ),
                (
                    "select",
                    "status",
                ),
                (
                    "limit",
                    "1",
                ),
            ],
        )

        if rows:

            status = rows[0].get(
                "status"
            )

            if status == "cancelled":

                cancelled_jobs.add(
                    job_id
                )

                return True

    except Exception:

        logger.exception(
            "CHECK JOB CANCELLED ERROR | job=%s",
            job_id,
        )

    return False


# =========================================================
# GET JOB MESSAGES
# =========================================================

async def get_job_messages(job):

    all_rows = []

    offset = 0
    page_size = 1000

    while True:

        # Stop loading more DB pages if job
        # was cancelled.
        if await is_job_cancelled(
            job["id"]
        ):
            break

        rows = await db_select(
            "tracked_messages",
            params=[
                (
                    "chat_id",
                    f"eq.{job['chat_id']}",
                ),
                (
                    "message_time",
                    f"gte.{job['start_time']}",
                ),
                (
                    "message_time",
                    f"lte.{job['end_time']}",
                ),
                (
                    "order",
                    "message_time.asc",
                ),
                (
                    "limit",
                    str(page_size),
                ),
                (
                    "offset",
                    str(offset),
                ),
            ],
        )

        if not rows:
            break

        all_rows.extend(rows)

        if len(rows) < page_size:
            break

        offset += page_size

        if offset >= 200000:
            break

    return all_rows


# =========================================================
# DELETE TRACKED RECORDS IN BATCH
# =========================================================

async def delete_tracked_records(
    chat_id: int,
    message_ids,
):

    if not message_ids:
        return

    chunk_size = 100

    for i in range(
        0,
        len(message_ids),
        chunk_size,
    ):

        chunk = message_ids[
            i:i + chunk_size
        ]

        ids_text = ",".join(
            str(int(x))
            for x in chunk
        )

        try:

            await db_delete(
                "tracked_messages",
                {
                    "chat_id": (
                        f"eq.{chat_id}"
                    ),
                    "message_id": (
                        f"in.({ids_text})"
                    ),
                },
            )

        except Exception:

            logger.exception(
                "TRACKED RECORD DELETE ERROR | chat=%s",
                chat_id,
            )


# =========================================================
# PROCESS DELETE JOB
# =========================================================

async def process_delete_job(
    bot: Bot,
    job,
):

    job_id = job["id"]

    chat_id = int(
        job["chat_id"]
    )

    if job_id in active_jobs:
        return

    active_jobs.add(job_id)

    try:

        async with job_semaphore:

            async with delete_locks[chat_id]:

                # -------------------------------------------------
                # Check cancellation before starting
                # -------------------------------------------------

                if await is_job_cancelled(
                    job_id
                ):

                    logger.info(
                        "DELETE JOB ALREADY CANCELLED | job=%s | chat=%s",
                        job_id,
                        chat_id,
                    )

                    return

                logger.info(
                    "DELETE JOB START | job=%s | chat=%s | duration=%s",
                    job_id,
                    chat_id,
                    job["duration_text"],
                )

                # -------------------------------------------------
                # Mark processing
                # -------------------------------------------------

                await db_update(
                    "delete_jobs",
                    {
                        "id": f"eq.{job_id}",
                        "status": "eq.pending",
                    },
                    {
                        "status": "processing",
                        "started_at": utc_iso(
                            time.time()
                        ),
                    },
                )

                # Check again after DB update
                if await is_job_cancelled(
                    job_id
                ):
                    return

                # -------------------------------------------------
                # Get messages
                # -------------------------------------------------

                rows = await get_job_messages(
                    job
                )

                # If cancelled while fetching
                if await is_job_cancelled(
                    job_id
                ):

                    logger.info(
                        "DELETE JOB CANCELLED DURING FETCH | job=%s",
                        job_id,
                    )

                    return

                total = len(rows)

                deleted = 0
                failed = 0
                skipped_admin = 0
                skipped_pinned = 0

                successfully_deleted_ids = []

                media_counts = defaultdict(int)

                logger.info(
                    "MESSAGES FOUND | job=%s | total=%s",
                    job_id,
                    total,
                )

                # -------------------------------------------------
                # Protected IDs
                # -------------------------------------------------

                admin_ids, pinned_ids = (
                    await get_protected_message_ids(
                        bot,
                        chat_id,
                    )
                )

                # -------------------------------------------------
                # Delete
                # -------------------------------------------------

                for index, row in enumerate(
                    rows,
                    start=1,
                ):

                    # =================================================
                    # STOP CHECK
                    # =================================================

                    if await is_job_cancelled(
                        job_id
                    ):

                        logger.info(
                            "DELETE JOB STOPPED | job=%s | deleted=%s/%s",
                            job_id,
                            deleted,
                            total,
                        )

                        # Remove DB records for messages
                        # that were successfully deleted.
                        await delete_tracked_records(
                            chat_id,
                            successfully_deleted_ids,
                        )

                        # Persistent cancelled status
                        await db_update(
                            "delete_jobs",
                            {
                                "id": f"eq.{job_id}",
                            },
                            {
                                "status": "cancelled",
                                "deleted": deleted,
                                "failed": failed,
                                "skipped_admin": skipped_admin,
                                "skipped_pinned": skipped_pinned,
                                "media_counts": dict(
                                    media_counts
                                ),
                                "completed_at": utc_iso(
                                    time.time()
                                ),
                                "error_message": (
                                    "Stopped by admin."
                                ),
                            },
                        )

                        try:

                            await bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    "🛑 Delete Stopped\n\n"
                                    f"🗑 Deleted: {deleted}\n"
                                    f"📦 Found: {total}\n"
                                    f"👮 Admin skipped: {skipped_admin}\n"
                                    f"📌 Pinned skipped: {skipped_pinned}\n"
                                    f"❌ Failed: {failed}\n\n"
                                    "⛔ Delete process admin ne stop kar diya."
                                ),
                            )

                        except Exception:

                            logger.exception(
                                "STOP MESSAGE ERROR"
                            )

                        return

                    message_id = int(
                        row["message_id"]
                    )

                    sender_id = int(
                        row.get(
                            "sender_id",
                            0,
                        )
                    )

                    media_type = row.get(
                        "media_type",
                        "text",
                    )

                    # Pinned
                    if message_id in pinned_ids:

                        skipped_pinned += 1
                        continue

                    # Admin
                    if (
                        sender_id
                        and sender_id in admin_ids
                    ):

                        skipped_admin += 1
                        continue

                    ok = await delete_one_message(
                        bot=bot,
                        chat_id=chat_id,
                        message_id=message_id,
                    )

                    if ok:

                        deleted += 1

                        media_counts[
                            media_type
                        ] += 1

                        successfully_deleted_ids.append(
                            message_id
                        )

                    else:

                        failed += 1

                    await safe_sleep(
                        DELETE_DELAY
                    )

                    if index % 100 == 0:

                        logger.info(
                            "DELETE PROGRESS | job=%s | %s/%s",
                            job_id,
                            index,
                            total,
                        )

                # -------------------------------------------------
                # Final stop check
                # -------------------------------------------------

                if await is_job_cancelled(
                    job_id
                ):

                    await delete_tracked_records(
                        chat_id,
                        successfully_deleted_ids,
                    )

                    await db_update(
                        "delete_jobs",
                        {
                            "id": f"eq.{job_id}",
                        },
                        {
                            "status": "cancelled",
                            "deleted": deleted,
                            "failed": failed,
                            "skipped_admin": skipped_admin,
                            "skipped_pinned": skipped_pinned,
                            "media_counts": dict(
                                media_counts
                            ),
                            "completed_at": utc_iso(
                                time.time()
                            ),
                            "error_message": (
                                "Stopped by admin."
                            ),
                        },
                    )

                    return

                # -------------------------------------------------
                # Remove successfully deleted DB records
                # -------------------------------------------------

                await delete_tracked_records(
                    chat_id,
                    successfully_deleted_ids,
                )

                # -------------------------------------------------
                # Result
                # -------------------------------------------------

                result = {
                    "total": total,
                    "deleted": deleted,
                    "failed": failed,
                    "skipped_admin": skipped_admin,
                    "skipped_pinned": skipped_pinned,
                    "media_counts": dict(
                        media_counts
                    ),
                }

                # -------------------------------------------------
                # Complete Job
                # -------------------------------------------------

                await db_update(
                    "delete_jobs",
                    {
                        "id": f"eq.{job_id}",
                    },
                    {
                        "status": "completed",
                        "total": total,
                        "deleted": deleted,
                        "failed": failed,
                        "skipped_admin": skipped_admin,
                        "skipped_pinned": skipped_pinned,
                        "media_counts": dict(
                            media_counts
                        ),
                        "completed_at": utc_iso(
                            time.time()
                        ),
                        "error_message": None,
                    },
                )

                logger.info(
                    "DELETE JOB COMPLETE | job=%s | chat=%s | total=%s | deleted=%s | failed=%s",
                    job_id,
                    chat_id,
                    total,
                    deleted,
                    failed,
                )

                # -------------------------------------------------
                # Completion Message
                # -------------------------------------------------

                try:

                    labels = {
                        "text": "💬 Text",
                        "video": "🎥 Video",
                        "photo": "🖼 Photo",
                        "document": "📄 Document",
                        "audio": "🎵 Audio",
                        "voice": "🎙 Voice",
                        "video_note": "⭕ Video Note",
                        "animation": "🎞 Animation",
                        "sticker": "🏷 Sticker",
                        "contact": "👤 Contact",
                        "location": "📍 Location",
                        "poll": "📊 Poll",
                        "dice": "🎲 Dice",
                        "other": "📦 Other",
                    }

                    media_lines = []

                    for key, label in labels.items():

                        count = media_counts.get(
                            key,
                            0,
                        )

                        if count:

                            media_lines.append(
                                f"{label}: {count}"
                            )

                    media_text = (
                        "\n".join(
                            media_lines
                        )
                        if media_lines
                        else "—"
                    )

                    await bot.send_message(
                        chat_id=chat_id,
                        text=(
                            "✅ Delete Complete\n\n"
                            f"🗑 Deleted: {deleted}\n"
                            f"📦 Found: {total}\n"
                            f"👮 Admin skipped: {skipped_admin}\n"
                            f"📌 Pinned skipped: {skipped_pinned}\n"
                            f"❌ Failed: {failed}\n\n"
                            "Deleted Types\n"
                            f"{media_text}"
                        ),
                    )

                except Exception:

                    logger.exception(
                        "COMPLETION MESSAGE ERROR"
                    )

    except asyncio.CancelledError:

        raise

    except Exception as e:

        logger.exception(
            "DELETE JOB ERROR | job=%s | chat=%s",
            job_id,
            chat_id,
        )

        try:

            await db_update(
                "delete_jobs",
                {
                    "id": f"eq.{job_id}",
                },
                {
                    "status": "pending",
                    "error_message": str(e)[:2000],
                },
            )

        except Exception:

            logger.exception(
                "FAILED TO RESET JOB | job=%s",
                job_id,
            )

    finally:

        active_jobs.discard(
            job_id
        )


# =========================================================
# START JOB
# =========================================================

def start_job(
    bot: Bot,
    job,
):

    job_id = job["id"]

    if job_id in active_jobs:
        return

    # Never start cancelled jobs
    if job_id in cancelled_jobs:
        return

    asyncio.create_task(
        process_delete_job(
            bot,
            job,
        )
    )


# =========================================================
# RECOVER JOBS
# =========================================================

async def recover_jobs(
    bot: Bot,
):

    try:

        # Processing jobs left by previous
        # crashed/restarted instance become pending.
        await db_update(
            "delete_jobs",
            {
                "status": "eq.processing",
            },
            {
                "status": "pending",
                "error_message": "Recovered after restart.",
            },
        )

        jobs = await db_select(
            "delete_jobs",
            params=[
                (
                    "status",
                    "eq.pending",
                ),
                (
                    "order",
                    "created_at.asc",
                ),
                (
                    "limit",
                    "100",
                ),
            ],
        )

        logger.info(
            "JOB RECOVERY | found=%s",
            len(jobs),
        )

        for job in jobs:

            start_job(
                bot,
                job,
            )

    except Exception:

        logger.exception(
            "JOB RECOVERY ERROR"
        )


# =========================================================
# DELETE JOB WORKER
# =========================================================

async def delete_job_worker(
    bot: Bot,
):

    logger.info(
        "DELETE JOB WORKER STARTED"
    )

    await recover_jobs(
        bot
    )

    while True:

        try:

            jobs = await db_select(
                "delete_jobs",
                params=[
                    (
                        "status",
                        "eq.pending",
                    ),
                    (
                        "order",
                        "created_at.asc",
                    ),
                    (
                        "limit",
                        "20",
                    ),
                ],
            )

            for job in jobs:

                start_job(
                    bot,
                    job,
                )

            await safe_sleep(
                JOB_CHECK_INTERVAL
            )

        except asyncio.CancelledError:

            raise

        except Exception:

            logger.exception(
                "DELETE JOB WORKER ERROR"
            )

            await safe_sleep(2)


# =========================================================
# DELETE COMMAND
# =========================================================

@router.message(
    Command("delete"),
    F.chat.type.in_({
        "group",
        "supergroup",
    }),
)
async def delete_handler(
    message: Message,
):

    if not message.text:
        return

    # =====================================================
    # USER CHECK
    # =====================================================

    if not message.from_user:
        return

    user_id = message.from_user.id

    # =====================================================
    # GROUP ADMIN CHECK
    # =====================================================

    try:

        user_member = await message.bot.get_chat_member(
            chat_id=message.chat.id,
            user_id=user_id,
        )

        if user_member.status not in {
            "administrator",
            "creator",
        }:

            await message.reply(
                "❌ Sirf Group Admin / Owner "
                "/delete command use kar sakta hai."
            )

            return

    except TelegramNetworkError:

        await message.reply(
            "⚠️ Telegram network error aaya.\n"
            "Thodi der baad try karo."
        )

        return

    except TelegramForbiddenError:

        await message.reply(
            "❌ Admin status check nahi ho paaya."
        )

        return

    except TelegramBadRequest:

        await message.reply(
            "❌ User admin status check nahi ho paaya."
        )

        return

    except Exception:

        logger.exception(
            "GROUP ADMIN CHECK ERROR | chat=%s | user=%s",
            message.chat.id,
            user_id,
        )

        await message.reply(
            "❌ Admin permission check failed."
        )

        return

    # =====================================================
    # DURATION
    # =====================================================

    args = message.text.split()

    if len(args) < 2:

        await message.reply(
            "❌ Time missing.\n\n"
            "Examples:\n"
            "/delete 5m\n"
            "/delete 10m\n"
            "/delete 1h\n"
            "/delete 2h\n"
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
            "/delete 7m\n"
            "/delete 2h\n"
            "/delete 1d\n"
            "/delete 7d\n\n"
            f"📌 Maximum: {MAX_TRACK_DAYS}d"
        )

        return

    command_time = time.time()

    start_time = (
        command_time - seconds
    )

    # =====================================================
    # BOT PERMISSION CHECK
    # =====================================================

    try:

        me = await message.bot.get_me()

        member = await message.bot.get_chat_member(
            chat_id=message.chat.id,
            user_id=me.id,
        )

        if member.status not in {
            "administrator",
            "creator",
        }:

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
                False,
            )

            if not can_delete:

                await message.reply(
                    "❌ Bot ke paas "
                    "Delete Messages "
                    "permission nahi hai."
                )

                return

    except TelegramNetworkError:

        await message.reply(
            "⚠️ Telegram network error aaya.\n"
            "Thodi der baad try karo."
        )

        return

    except TelegramForbiddenError:

        await message.reply(
            "❌ Permission check reject hua.\n"
            "Bot ko group Administrator banao."
        )

        return

    except TelegramBadRequest:

        await message.reply(
            "❌ Bot permission check failed."
        )

        return

    except Exception:

        logger.exception(
            "BOT PERMISSION CHECK ERROR"
        )

        await message.reply(
            "❌ Bot permission check nahi ho paayi."
        )

        return

    # =====================================================
    # SAVE COMMAND MESSAGE
    # =====================================================

    try:

        await db_insert(
            "tracked_messages",
            {
                "chat_id": message.chat.id,
                "message_id": message.message_id,
                "sender_id": user_id,
                "media_type": "text",
                "message_time": utc_iso(
                    command_time
                ),
            },
        )

    except Exception:

        logger.exception(
            "COMMAND MESSAGE SAVE ERROR"
        )

    # =====================================================
    # CREATE PERSISTENT JOB
    # =====================================================

    job = None

    try:

        rows = await db_insert(
            "delete_jobs",
            {
                "chat_id": message.chat.id,
                "requested_by": user_id,
                "duration_text": duration_text,
                "duration_seconds": seconds,
                "start_time": utc_iso(
                    start_time
                ),
                "end_time": utc_iso(
                    command_time
                ),
                "status": "pending",
            },
            return_data=True,
        )

        if rows and isinstance(rows, list):

            job = rows[0]

    except Exception:

        logger.exception(
            "CREATE DELETE JOB ERROR"
        )

        await message.reply(
            "❌ Delete job create nahi ho paayi.\n"
            "Database error."
        )

        return

    # =====================================================
    # FALLBACK JOB FETCH
    # =====================================================

    if not job:

        try:

            latest = await db_select(
                "delete_jobs",
                params=[
                    (
                        "chat_id",
                        f"eq.{message.chat.id}",
                    ),
                    (
                        "requested_by",
                        f"eq.{user_id}",
                    ),
                    (
                        "status",
                        "eq.pending",
                    ),
                    (
                        "order",
                        "created_at.desc",
                    ),
                    (
                        "limit",
                        "1",
                    ),
                ],
            )

            if latest:
                job = latest[0]

        except Exception:

            logger.exception(
                "GET CREATED JOB ERROR"
            )

    # =====================================================
    # CONFIRMATION
    # =====================================================

    await message.reply(
        "🗑 Delete request received\n\n"
        f"👤 Admin: {message.from_user.full_name}\n"
        f"⏱ Range: {duration_text}\n"
        "⚡ Delete process queue me hai.\n"
        "🔄 Restart ke baad bhi job recover hogi.\n\n"
        "🛑 Stop karne ke liye:\n"
        "/stopdelete\n\n"
        "📌 Command ke baad aane wale "
        "messages is request me delete nahi honge."
    )

    # =====================================================
    # START JOB
    # =====================================================

    if job:

        start_job(
            message.bot,
            job,
        )

    logger.info(
        "DELETE REQUEST | chat=%s | admin=%s | duration=%s | job=%s",
        message.chat.id,
        user_id,
        duration_text,
        job.get("id") if job else None,
    )


# =========================================================
# STOP DELETE COMMAND
# =========================================================

@router.message(
    Command("stopdelete"),
    F.chat.type.in_({
        "group",
        "supergroup",
    }),
)
async def stop_delete_handler(
    message: Message,
):

    if not message.from_user:
        return

    user_id = message.from_user.id
    chat_id = message.chat.id

    # =====================================================
    # GROUP ADMIN CHECK
    # =====================================================

    try:

        user_member = await message.bot.get_chat_member(
            chat_id=chat_id,
            user_id=user_id,
        )

        if user_member.status not in {
            "administrator",
            "creator",
        }:

            await message.reply(
                "❌ Sirf Group Admin / Owner "
                "/stopdelete command use kar sakta hai."
            )

            return

    except TelegramNetworkError:

        await message.reply(
            "⚠️ Telegram network error aaya.\n"
            "Thodi der baad try karo."
        )

        return

    except TelegramForbiddenError:

        await message.reply(
            "❌ Admin status check nahi ho paaya."
        )

        return

    except TelegramBadRequest:

        await message.reply(
            "❌ User admin status check nahi ho paaya."
        )

        return

    except Exception:

        logger.exception(
            "STOP GROUP ADMIN CHECK ERROR | chat=%s | user=%s",
            chat_id,
            user_id,
        )

        await message.reply(
            "❌ Admin permission check failed."
        )

        return

    # =====================================================
    # FIND ACTIVE/PENDING JOBS
    # =====================================================

    try:

        jobs = await db_select(
            "delete_jobs",
            params=[
                (
                    "chat_id",
                    f"eq.{chat_id}",
                ),
                (
                    "status",
                    "in.(pending,processing)",
                ),
                (
                    "order",
                    "created_at.asc",
                ),
                (
                    "limit",
                    "100",
                ),
            ],
        )

    except Exception:

        logger.exception(
            "GET ACTIVE JOBS FOR STOP ERROR | chat=%s",
            chat_id,
        )

        await message.reply(
            "❌ Active delete jobs database se fetch nahi ho paayi."
        )

        return

    if not jobs:

        await message.reply(
            "ℹ️ Is group me koi active delete process nahi chal raha."
        )

        return

    # =====================================================
    # CANCEL JOBS
    # =====================================================

    stopped_count = 0

    for job in jobs:

        job_id = job["id"]

        # Local immediate cancellation
        cancelled_jobs.add(
            job_id
        )

        try:

            await db_update(
                "delete_jobs",
                {
                    "id": f"eq.{job_id}",
                    "status": "in.(pending,processing)",
                },
                {
                    "status": "cancelled",
                    "completed_at": utc_iso(
                        time.time()
                    ),
                    "error_message": (
                        f"Stopped by group admin {user_id}."
                    ),
                },
            )

            stopped_count += 1

            logger.info(
                "DELETE JOB CANCEL REQUESTED | job=%s | chat=%s | admin=%s",
                job_id,
                chat_id,
                user_id,
            )

        except Exception:

            logger.exception(
                "STOP JOB DB UPDATE ERROR | job=%s | chat=%s",
                job_id,
                chat_id,
            )

    # =====================================================
    # RESULT
    # =====================================================

    if stopped_count:

        await message.reply(
            "🛑 Delete Process Stopped\n\n"
            f"⛔ Stopped Jobs: {stopped_count}\n\n"
            "Jo messages already delete ho chuke hain "
            "unhe restore nahi kiya ja sakta.\n\n"
            "Agar delete process chal raha tha, "
            "woh next stop-check par terminate ho jayega."
        )

    else:

        await message.reply(
            "ℹ️ Koi delete job stop nahi hui."
        )


# =========================================================
# PRIVATE DELETE
# =========================================================

@router.message(
    Command("delete"),
)
async def private_delete_handler(
    message: Message,
):

    if message.chat.type == "private":

        await message.answer(
            "ℹ️ /delete group/supergroup me use karo.\n\n"
            "👮 Sirf Group Admin / Owner command use kar sakta hai.\n\n"
            "Example:\n"
            "/delete 5m"
        )


# =========================================================
# PRIVATE STOPDELETE
# =========================================================

@router.message(
    Command("stopdelete"),
    F.chat.type == "private",
)
async def private_stop_delete_handler(
    message: Message,
):

    await message.answer(
        "ℹ️ /stopdelete group/supergroup me use karo.\n\n"
        "👮 Sirf Group Admin / Owner active delete process ko stop kar sakta hai."
    )


# =========================================================
# GET BROADCAST USERS
# =========================================================

async def get_broadcast_users():

    rows = await db_select(
        "bot_users",
        params=[
            (
                "select",
                "user_id",
            ),
            (
                "order",
                "user_id.asc",
            ),
            (
                "limit",
                "200000",
            ),
        ],
    )

    users = []

    for row in rows:

        try:

            users.append(
                int(
                    row["user_id"]
                )
            )

        except Exception:

            pass

    return users


# =========================================================
# BROADCAST SEND ONE
# =========================================================

async def broadcast_to_user(
    bot: Bot,
    user_id: int,
    source_message,
    broadcast_text,
):

    while True:

        try:

            if source_message:

                await bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=(
                        source_message.chat.id
                    ),
                    message_id=(
                        source_message.message_id
                    ),
                )

            else:

                await bot.send_message(
                    chat_id=user_id,
                    text=broadcast_text,
                )

            return "sent"

        except TelegramRetryAfter as e:

            wait = (
                float(e.retry_after)
                + 0.5
            )

            logger.warning(
                "BROADCAST FLOOD WAIT | user=%s | wait=%.1f",
                user_id,
                wait,
            )

            await safe_sleep(
                wait
            )

        except TelegramNetworkError as e:

            logger.warning(
                "BROADCAST NETWORK ERROR | user=%s | %s",
                user_id,
                e,
            )

            await safe_sleep(2)

        except TelegramForbiddenError:

            return "blocked"

        except TelegramBadRequest as e:

            logger.warning(
                "BROADCAST BAD REQUEST | user=%s | %s",
                user_id,
                e,
            )

            return "failed"

        except Exception:

            logger.exception(
                "BROADCAST UNKNOWN ERROR | user=%s",
                user_id,
            )

            return "failed"


# =========================================================
# BROADCAST
# =========================================================

@router.message(
    Command("broadcast"),
    F.chat.type == "private",
)
async def broadcast_handler(
    message: Message,
):

    if not message.from_user:
        return

    if not is_admin(
        message.from_user.id
    ):

        await message.answer(
            "❌ Unauthorized.\n\n"
            "Sirf bot admin broadcast use kar sakta hai."
        )

        return

    source_message = (
        message.reply_to_message
    )

    broadcast_text = None

    if (
        not source_message
        and message.text
    ):

        parts = message.text.split(
            maxsplit=1
        )

        if len(parts) == 2:

            broadcast_text = (
                parts[1].strip()
            )

    if (
        not source_message
        and not broadcast_text
    ):

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

    try:

        users = await get_broadcast_users()

    except Exception:

        logger.exception(
            "GET BROADCAST USERS ERROR"
        )

        await message.answer(
            "❌ Database se users fetch nahi ho paaye."
        )

        return

    if not users:

        await message.answer(
            "⚠️ No users found.\n\n"
            "Abhi kisi user ne bot me /start nahi kiya."
        )

        return

    users = list(
        dict.fromkeys(users)
    )

    total = len(users)

    sent = 0
    failed = 0
    blocked = 0
    processed = 0

    progress_message = await message.answer(
        "📢 Broadcast Started\n\n"
        f"👥 Users: {total}\n"
        "📨 Processed: 0\n"
        "✅ Sent: 0\n"
        "❌ Failed: 0\n"
        "🚫 Blocked: 0\n"
        "⏳ Progress: 0%"
    )

    async with broadcast_lock:

        concurrency = 8

        for start in range(
            0,
            total,
            concurrency,
        ):

            batch_users = users[
                start:start + concurrency
            ]

            tasks = [
                broadcast_to_user(
                    message.bot,
                    user_id,
                    source_message,
                    broadcast_text,
                )
                for user_id in batch_users
            ]

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            for user_id, result in zip(
                batch_users,
                results,
            ):

                processed += 1

                if result == "sent":

                    sent += 1

                elif result == "blocked":

                    blocked += 1
                    failed += 1

                else:

                    failed += 1

            if (
                processed == total
                or processed % BROADCAST_PROGRESS_EVERY == 0
                or processed >= BROADCAST_PROGRESS_EVERY
            ):

                percent = int(
                    (
                        processed
                        / total
                    ) * 100
                )

                try:

                    await progress_message.edit_text(
                        "📢 Broadcasting...\n\n"
                        f"👥 Total: {total}\n"
                        f"📨 Processed: {processed}\n"
                        f"✅ Sent: {sent}\n"
                        f"❌ Failed: {failed}\n"
                        f"🚫 Blocked: {blocked}\n"
                        f"⏳ Progress: {percent}%"
                    )

                except TelegramRetryAfter as e:

                    await safe_sleep(
                        float(e.retry_after)
                        + 0.5
                    )

                except Exception:

                    pass

            await safe_sleep(
                BROADCAST_DELAY
            )

    try:

        await progress_message.edit_text(
            "✅ Broadcast Completed\n\n"
            f"👥 Total: {total}\n"
            f"📨 Sent: {sent}\n"
            f"❌ Failed: {failed}\n"
            f"🚫 Blocked/Unavailable: {blocked}\n"
        )

    except Exception:

        logger.exception(
            "FINAL BROADCAST MESSAGE ERROR"
        )

    logger.info(
        "BROADCAST COMPLETE | total=%s | sent=%s | failed=%s",
        total,
        sent,
        failed,
    )


# =========================================================
# EXACT DATABASE COUNT
# =========================================================

async def get_exact_count(
    table: str,
    params=None,
):

    response = await supabase_client.get(
        f"/{table}",
        params=params,
        headers={
            "Prefer": "count=exact",
        },
    )

    if not (
        200 <= response.status_code < 300
    ):

        raise RuntimeError(
            f"Count failed: "
            f"{response.status_code} "
            f"{response.text[:500]}"
        )

    content_range = response.headers.get(
        "content-range",
        "",
    )

    if "/" in content_range:

        try:

            return int(
                content_range.split(
                    "/"
                )[1]
            )

        except Exception:

            pass

    try:

        data = response.json()

        return len(data)

    except Exception:

        return 0


# =========================================================
# STATS
# =========================================================

@router.message(
    Command("stats"),
    F.chat.type == "private",
)
async def stats_handler(
    message: Message,
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

    try:

        users = await get_exact_count(
            "bot_users",
            params=[
                (
                    "select",
                    "user_id",
                )
            ],
        )

        tracked = await get_exact_count(
            "tracked_messages",
            params=[
                (
                    "select",
                    "id",
                )
            ],
        )

        pending = await get_exact_count(
            "delete_jobs",
            params=[
                (
                    "select",
                    "id",
                ),
                (
                    "status",
                    "eq.pending",
                ),
            ],
        )

        processing = await get_exact_count(
            "delete_jobs",
            params=[
                (
                    "select",
                    "id",
                ),
                (
                    "status",
                    "eq.processing",
                ),
            ],
        )

        completed = await get_exact_count(
            "delete_jobs",
            params=[
                (
                    "select",
                    "id",
                ),
                (
                    "status",
                    "eq.completed",
                ),
            ],
        )

        cancelled = await get_exact_count(
            "delete_jobs",
            params=[
                (
                    "select",
                    "id",
                ),
                (
                    "status",
                    "eq.cancelled",
                ),
            ],
        )

        await message.answer(
            "📊 Bot Statistics\n\n"
            f"👥 Users: {users}\n\n"
            f"💬 Tracked Messages: {tracked}\n\n"
            f"⏳ Pending Jobs: {pending}\n"
            f"⚙️ Processing Jobs: {processing}\n"
            f"✅ Completed Jobs: {completed}\n"
            f"🛑 Cancelled Jobs: {cancelled}\n\n"
            "💾 Storage: Supabase\n"
            f"⏱ Tracking: {MAX_TRACK_DAYS} days"
        )

    except Exception:

        logger.exception(
            "STATS ERROR"
        )

        await message.answer(
            "❌ Stats database se fetch nahi ho paaye."
        )


# =========================================================
# CLEANUP OLD MESSAGES
# =========================================================

async def cleanup_worker():

    logger.info(
        "DATABASE CLEANUP WORKER STARTED"
    )

    while True:

        try:

            cutoff = utc_iso(
                time.time()
                - TRACK_SECONDS
            )

            await db_delete(
                "tracked_messages",
                {
                    "message_time": (
                        f"lt.{cutoff}"
                    ),
                },
            )

            logger.info(
                "OLD MESSAGE CLEANUP COMPLETE | before=%s",
                cutoff,
            )

        except Exception:

            logger.exception(
                "CLEANUP ERROR"
            )

        await asyncio.sleep(
            6 * 60 * 60
        )


# =========================================================
# DATABASE CHECK
# =========================================================

async def database_check():

    try:

        await db_select(
            "tracked_messages",
            params=[
                (
                    "select",
                    "id",
                ),
                (
                    "limit",
                    "1",
                ),
            ],
        )

        logger.info(
            "SUPABASE CONNECTED"
        )

        return True

    except Exception:

        logger.exception(
            "SUPABASE CONNECTION FAILED"
        )

        return False


# =========================================================
# POLLING
# =========================================================

async def polling_loop(
    bot: Bot,
):

    retry_delay = POLL_RETRY_MIN

    while True:

        try:

            logger.info(
                "START POLLING"
            )

            await dp.start_polling(
                bot,
                allowed_updates=(
                    dp.resolve_used_update_types()
                ),
                handle_signals=False,
            )

            logger.warning(
                "POLLING STOPPED | reconnect in %ss",
                retry_delay,
            )

            await safe_sleep(
                retry_delay
            )

            retry_delay = min(
                retry_delay * 2,
                POLL_RETRY_MAX,
            )

        except TelegramUnauthorizedError:

            logger.critical(
                "TELEGRAM UNAUTHORIZED | "
                "BOT TOKEN INVALID/REVOKED"
            )

            raise

        except TelegramNetworkError as e:

            logger.error(
                "TELEGRAM NETWORK ERROR | %s | reconnect in %ss",
                e,
                retry_delay,
            )

            await safe_sleep(
                retry_delay
            )

            retry_delay = min(
                retry_delay * 2,
                POLL_RETRY_MAX,
            )

        except asyncio.CancelledError:

            raise

        except Exception:

            logger.exception(
                "POLLING UNKNOWN ERROR | reconnect in %ss",
                retry_delay,
            )

            await safe_sleep(
                retry_delay
            )

            retry_delay = min(
                retry_delay * 2,
                POLL_RETRY_MAX,
            )


# =========================================================
# MAIN
# =========================================================

async def main():

    global supabase_client

    # =====================================================
    # CONFIG VALIDATION
    # =====================================================

    if (
        not BOT_TOKEN
        or BOT_TOKEN == "PUT_NEW_BOT_TOKEN_HERE"
    ):

        raise RuntimeError(
            "BOT_TOKEN set karo."
        )

    if (
        not SUPABASE_URL
        or "YOUR_PROJECT" in SUPABASE_URL
    ):

        raise RuntimeError(
            "SUPABASE_URL set karo."
        )

    if (
        not SUPABASE_SERVICE_KEY
        or SUPABASE_SERVICE_KEY
        == "PUT_NEW_SUPABASE_SERVICE_ROLE_KEY_HERE"
    ):

        raise RuntimeError(
            "SUPABASE_SERVICE_KEY set karo."
        )

    if not ADMIN_IDS:

        raise RuntimeError(
            "ADMIN_IDS empty hai."
        )

    # =====================================================
    # SUPABASE
    # =====================================================

    supabase_client = (
        create_supabase_client()
    )

    db_ok = await database_check()

    if not db_ok:

        await supabase_client.aclose()

        raise RuntimeError(
            "Supabase connection failed."
        )

    # =====================================================
    # BOT
    # =====================================================

    bot = Bot(
        token=BOT_TOKEN
    )

    # =====================================================
    # BACKGROUND TASKS
    # =====================================================

    message_worker_task = (
        asyncio.create_task(
            message_db_worker()
        )
    )

    delete_worker_task = (
        asyncio.create_task(
            delete_job_worker(bot)
        )
    )

    cleanup_task = (
        asyncio.create_task(
            cleanup_worker()
        )
    )

    try:

        me = await bot.get_me()

        logger.info(
            "========================================"
        )

        logger.info(
            "BOT CONNECTED: @%s | id=%s",
            me.username,
            me.id,
        )

        logger.info(
            "SUPABASE: CONNECTED"
        )

        logger.info(
            "STORAGE: SUPABASE"
        )

        logger.info(
            "MAX TRACK: %s DAYS",
            MAX_TRACK_DAYS,
        )

        logger.info(
            "MAX PARALLEL DELETE JOBS: %s",
            MAX_PARALLEL_DELETE_JOBS,
        )

        logger.info(
            "ADMIN IDS: %s",
            list(ADMIN_IDS),
        )

        logger.info(
            "STOP COMMAND: /stopdelete"
        )

        logger.info(
            "========================================"
        )

        await polling_loop(
            bot
        )

    finally:

        logger.info(
            "SHUTTING DOWN..."
        )

        for task in (
            message_worker_task,
            delete_worker_task,
            cleanup_task,
        ):

            task.cancel()

        for task in (
            message_worker_task,
            delete_worker_task,
            cleanup_task,
        ):

            try:

                await task

            except asyncio.CancelledError:

                pass

            except Exception:

                logger.exception(
                    "BACKGROUND TASK SHUTDOWN ERROR"
                )

        try:

            await bot.session.close()

        except Exception:

            logger.exception(
                "BOT SESSION CLOSE ERROR"
            )

        try:

            await supabase_client.aclose()

        except Exception:

            logger.exception(
                "SUPABASE CLIENT CLOSE ERROR"
            )

        logger.info(
            "BOT STOPPED"
        )


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except TelegramUnauthorizedError:

        logger.critical(
            "BOT STOPPED: INVALID/REVOKED TOKEN."
        )

    except KeyboardInterrupt:

        logger.info(
            "BOT STOPPED BY USER"
        )

    except Exception:

        logger.exception(
            "FATAL BOT ERROR"
        )

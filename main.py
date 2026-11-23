import asyncio
import hashlib
import hmac
import html
import json
import os
import re
import secrets
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl

import httpx
import uvicorn
from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BotCommand,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonWebApp,
    WebAppInfo,
)
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


# --- БЕЗОПАСНОЕ ЧТЕНИЕ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ---
def env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name, "").strip()
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def env_float(name: str, default: float = 0.05) -> float:
    raw = os.getenv(name, "").strip().replace(",", ".")
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def normalize_owner(raw: str) -> tuple[str, str]:
    """Из @nick, nick или https://t.me/nick возвращает ('@nick', 'https://t.me/nick')."""
    cleaned = raw.strip()
    cleaned = re.sub(r"^https?://t\.me/", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.lstrip("@").split("/")[0].split("?")[0].strip()
    if not cleaned:
        cleaned = "owner"
    return f"@{cleaned}", f"https://t.me/{cleaned}"


# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
OWNER_DISPLAY, OWNER_URL = normalize_owner(os.getenv("OWNER_USERNAME", "@owner"))
COMMISSION_RATE = env_float("COMMISSION_RATE", 0.05)
DB_PATH = os.getenv("DB_PATH", "gateway.db").strip() or "gateway.db"
DB_CHANNEL_ID = env_int("DB_CHANNEL_ID", 0)
ADMIN_ID = env_int("ADMIN_ID", 0)

if not BOT_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN!")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
BOT_USERNAME = ""
db_sync_lock = asyncio.Lock()


# --- FSM СОСТОЯНИЯ ---
class CreateProjectFSM(StatesGroup):
    waiting_title = State()
    waiting_webhook = State()


class EditWebhookFSM(StatesGroup):
    waiting_url = State()


class QuickInvoiceFSM(StatesGroup):
    waiting_title = State()
    waiting_amount = State()


class TopUpFSM(StatesGroup):
    waiting_amount = State()
    waiting_comment = State()


# --- ХЕЛПЕРЫ И БЕЗОПАСНОСТЬ ---
def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID


def esc(text: Optional[str]) -> str:
    return html.escape(str(text or ""))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def safe_edit(message: types.Message, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup, disable_web_page_preview=True)
    except TelegramBadRequest:
        pass


def verify_telegram_init_data(init_data: str) -> dict:
    if not init_data:
        raise HTTPException(status_code=401, detail="Откройте приложение внутри Telegram")
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            raise HTTPException(status_code=401, detail="Отсутствует подпись Telegram")

        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
        calculated_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(calculated_hash, received_hash):
            raise HTTPException(status_code=401, detail="Невалидная подпись Telegram")

        user_raw = parsed.get("user")
        if not user_raw:
            raise HTTPException(status_code=401, detail="Нет данных пользователя")
        return json.loads(user_raw)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Ошибка авторизации Telegram")


# --- БАЗА ДАННЫХ С АВТОМИГРАЦИЕЙ СТАРЫХ СНИМКОВ ---
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def get_table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                wallet_balance INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS merchants (
                merchant_id TEXT PRIMARY KEY,
                owner_tg_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                api_key TEXT UNIQUE NOT NULL,
                webhook_url TEXT,
                balance_rp INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                merchant_id TEXT NOT NULL,
                external_order_id TEXT,
                title TEXT NOT NULL,
                description TEXT,
                amount_rp INTEGER NOT NULL,
                fee_rp INTEGER NOT NULL,
                net_rp INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                payer_tg_id INTEGER,
                created_at TEXT NOT NULL,
                paid_at TEXT
            );

            CREATE TABLE IF NOT EXISTS topup_requests (
                request_id TEXT PRIMARY KEY,
                user_tg_id INTEGER NOT NULL,
                amount_rp INTEGER NOT NULL,
                comment TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS transfers (
                transfer_id TEXT PRIMARY KEY,
                from_tg_id INTEGER NOT NULL,
                to_tg_id INTEGER NOT NULL,
                amount_rp INTEGER NOT NULL,
                fee_rp INTEGER NOT NULL,
                net_rp INTEGER NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL
            );
            """
        )

        # --- АВТОМАТИЧЕСКАЯ МИГРАЦИЯ СТАРЫХ БД (из ТГ-канала прошлых версий) ---
        u_cols = get_table_cols(conn, "users")
        if "wallet_balance" not in u_cols:
            conn.execute("ALTER TABLE users ADD COLUMN wallet_balance INTEGER DEFAULT 0")

        m_cols = get_table_cols(conn, "merchants")
        if "balance_rp" not in m_cols:
            conn.execute("ALTER TABLE merchants ADD COLUMN balance_rp INTEGER DEFAULT 0")
            if "balance_stars" in m_cols:
                conn.execute("UPDATE merchants SET balance_rp = COALESCE(balance_stars, 0)")

        o_cols = get_table_cols(conn, "orders")
        if "amount_rp" not in o_cols or "invoice_link" in o_cols:
            # Пересобираем старую таблицу orders без устаревшего NOT NULL у invoice_link
            amt_src = "amount_rp" if "amount_rp" in o_cols else "COALESCE(amount_stars, 0)"
            fee_src = "fee_rp" if "fee_rp" in o_cols else "COALESCE(fee_stars, 0)"
            net_src = "net_rp" if "net_rp" in o_cols else "COALESCE(net_stars, 0)"
            conn.executescript(
                f"""
                CREATE TABLE orders_migrated (
                    order_id TEXT PRIMARY KEY,
                    merchant_id TEXT NOT NULL,
                    external_order_id TEXT,
                    title TEXT NOT NULL,
                    description TEXT,
                    amount_rp INTEGER NOT NULL,
                    fee_rp INTEGER NOT NULL,
                    net_rp INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    payer_tg_id INTEGER,
                    created_at TEXT NOT NULL,
                    paid_at TEXT
                );
                INSERT INTO orders_migrated (
                    order_id, merchant_id, external_order_id, title, description,
                    amount_rp, fee_rp, net_rp, status, payer_tg_id, created_at, paid_at
                )
                SELECT
                    order_id, merchant_id, external_order_id, title, description,
                    {amt_src}, {fee_src}, {net_src}, status, payer_tg_id, created_at, paid_at
                FROM orders;
                DROP TABLE orders;
                ALTER TABLE orders_migrated RENAME TO orders;
                """
            )
            print("[DB Migration] Старая таблица orders успешно мигрирована на RP.")


def ensure_user(tg_id: int, username: Optional[str], first_name: Optional[str]) -> None:
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO users (tg_id, username, first_name, wallet_balance, created_at)
            VALUES (?, ?, ?, 0, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                username = COALESCE(excluded.username, users.username),
                first_name = COALESCE(excluded.first_name, users.first_name)
            """,
            (tg_id, username, first_name, utc_now()),
        )


def create_atomic_snapshot(snapshot_path: str) -> None:
    with get_db() as src, sqlite3.connect(snapshot_path) as dst:
        src.backup(dst)


async def restore_db_from_telegram() -> None:
    if not DB_CHANNEL_ID:
        init_db()
        return
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
        pinned = chat.pinned_message
        if pinned and pinned.document:
            await bot.download(pinned.document, destination=DB_PATH)
            print(f"[DB Sync] Восстановлена БД из канала (msg_id={pinned.message_id}).")
            init_db()
            return
    except Exception as e:
        print(f"[DB Sync] Не удалось скачать БД из канала: {e}")

    init_db()
    await sync_db_to_telegram("Инициализация базы данных RoboPay")


async def sync_db_to_telegram(reason: str = "Авто-синхронизация") -> Optional[int]:
    if not DB_CHANNEL_ID:
        return None
    async with db_sync_lock:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        snapshot_path = f"backup_{ts}.db"
        try:
            create_atomic_snapshot(snapshot_path)
            caption = f"🗄 <b>RoboPay DB</b>\n• {esc(reason)}\n• UTC: <code>{ts}</code>"
            msg = await bot.send_document(
                chat_id=DB_CHANNEL_ID,
                document=FSInputFile(snapshot_path, filename="gateway.db"),
                caption=caption,
                parse_mode="HTML",
            )
            await bot.pin_chat_message(chat_id=DB_CHANNEL_ID, message_id=msg.message_id, disable_notification=True)
            return msg.message_id
        except Exception as e:
            print(f"[DB Sync Error] {e}")
            return None
        finally:
            if os.path.exists(snapshot_path):
                os.remove(snapshot_path)


# --- БИЗНЕС-ЛОГИКА ---
def create_order_record(
    merchant_id: str,
    title: str,
    description: str,
    amount_rp: int,
    external_order_id: Optional[str] = None,
) -> dict:
    order_id = f"ord_{secrets.token_hex(6)}"
    fee_rp = int(amount_rp * COMMISSION_RATE)
    net_rp = max(1, amount_rp - fee_rp)

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, merchant_id, external_order_id, title, description,
                amount_rp, fee_rp, net_rp, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (order_id, merchant_id, external_order_id, title[:48], description[:255], amount_rp, fee_rp, net_rp, utc_now()),
        )

    bot_link = f"https://t.me/{BOT_USERNAME}?start=pay_{order_id}"
    return {
        "order_id": order_id,
        "status": "pending",
        "amount": amount_rp,
        "amount_rp": amount_rp,
        "fee_rp": fee_rp,
        "net_rp": net_rp,
        "checkout_deeplink": bot_link,
        "webapp_url": f"{PUBLIC_URL}/?order_id={order_id}",
        "qr_payload": bot_link,
    }


async def create_topup_request_and_notify(user_tg_id: int, amount_rp: int, comment: str) -> dict:
    req_id = f"top_{secrets.token_hex(5)}"
    now = utc_now()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO topup_requests (request_id, user_tg_id, amount_rp, comment, status, created_at)
            VALUES (?, ?, ?, ?, 'pending', ?)
            """,
            (req_id, user_tg_id, amount_rp, comment[:200], now),
        )
        user_row = conn.execute("SELECT username, first_name FROM users WHERE tg_id = ?", (user_tg_id,)).fetchone()

    asyncio.create_task(sync_db_to_telegram(f"Заявка на пополнение {req_id} (+{amount_rp} RP)"))

    if ADMIN_ID:
        uname = f"@{user_row['username']}" if user_row and user_row["username"] else "без username"
        fname = esc(user_row["first_name"] if user_row else "Пользователь")
        admin_text = (
            f"🎁 <b>Новая заявка на пополнение</b>\n\n"
            f"• Заявка: <code>{req_id}</code>\n"
            f"• Клиент: <b>{fname}</b> ({uname})\n"
            f"• ID: <code>{user_tg_id}</code>\n"
            f"• Сумма: <b>{amount_rp} ⭐ → {amount_rp} RP</b>\n"
            f"• Комментарий: <i>{esc(comment or '—')}</i>\n\n"
            f"Проверьте подарок/Stars на <b>{esc(OWNER_DISPLAY)}</b>:"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text=f"✅ Одобрить (+{amount_rp} RP)", callback_data=f"topup_ok:{req_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"topup_no:{req_id}"),
                ]
            ]
        )
        try:
            await bot.send_message(ADMIN_ID, admin_text, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            print(f"[Admin Notify Error] {e}")

    return {"request_id": req_id, "amount_rp": amount_rp, "status": "pending", "created_at": now}


def sign_payload(secret_key: str, payload_bytes: bytes) -> str:
    return hmac.new(secret_key.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()


async def dispatch_partner_notification(order: dict, merchant: dict) -> None:
    signal_text = (
        f"⚡ <b>Оплата получена — {esc(merchant['title'])}</b>\n\n"
        f"• Услуга: <b>{esc(order['title'])}</b>\n"
        f"• Сумма: <b>{order['amount_rp']} RP</b> (зачислено: <b>+{order['net_rp']} RP</b>)\n"
        f"• Заказ: <code>{order['order_id']}</code>\n"
        f"• Покупатель ID: <code>{order['payer_tg_id']}</code>"
    )
    try:
        await bot.send_message(merchant["owner_tg_id"], signal_text, parse_mode="HTML")
    except Exception as e:
        print(f"[Telegram Signal Error] {e}")

    if merchant.get("webhook_url"):
        event_data = {
            "event": "payment.succeeded",
            "order_id": order["order_id"],
            "external_order_id": order["external_order_id"],
            "amount_rp": order["amount_rp"],
            "net_rp": order["net_rp"],
            "payer_tg_id": order["payer_tg_id"],
            "paid_at": order["paid_at"],
        }
        raw_body = json.dumps(event_data, ensure_ascii=False).encode("utf-8")
        signature = sign_payload(merchant["api_key"], raw_body)
        headers = {
            "Content-Type": "application/json",
            "X-RoboPay-Signature": signature,
            "X-RoboPay-Event": "payment.succeeded",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(3):
                try:
                    resp = await client.post(merchant["webhook_url"], content=raw_body, headers=headers)
                    if 200 <= resp.status_code < 300:
                        break
                except Exception:
                    await asyncio.sleep(1.5 * (attempt + 1))


# --- ИНТЕРФЕЙС БОТА ---
def build_main_menu(user: types.User) -> tuple[str, InlineKeyboardMarkup]:
    with get_db() as conn:
        u = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (user.id,)).fetchone()
        m_stats = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(balance_rp), 0) AS m_bal FROM merchants WHERE owner_tg_id = ?",
            (user.id,),
        ).fetchone()

    w_bal = u["wallet_balance"] if u else 0
    m_bal = m_stats["m_bal"] if m_stats else 0
    m_cnt = m_stats["cnt"] if m_stats else 0

    text = (
        f"<b>RoboPay</b> — платёжный терминал\n\n"
        f"Аккаунт <b>{esc(user.first_name)}</b> привязан (ID: <code>{user.id}</code>).\n"
        f"Оплачивайте услуги, принимайте переводы по QR и подключайте шлюз к своим проектам.\n\n"
        f"• Кошелёк: <b>{w_bal} RP</b>\n"
        f"• Проекты ({m_cnt}): <b>{m_bal} RP</b>"
    )

    buttons = []
    if PUBLIC_URL.startswith("https://"):
        buttons.append([InlineKeyboardButton(text="Открыть терминал RoboPay", web_app=WebAppInfo(url=f"{PUBLIC_URL}/"))])

    buttons.extend(
        [
            [
                InlineKeyboardButton(text="💳 Пополнить / Кошелёк", callback_data="wallet"),
                InlineKeyboardButton(text="⚡ Выставить счёт", callback_data="quick_pay"),
            ],
            [
                InlineKeyboardButton(text="📦 Мои проекты", callback_data="projects"),
                InlineKeyboardButton(text="📖 API", callback_data="api_docs"),
            ],
        ]
    )
    if is_admin(user.id):
        buttons.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_panel")])

    return text, InlineKeyboardMarkup(inline_keyboard=buttons)


def build_project_card(merchant_id: str, owner_id: int) -> Optional[tuple[str, InlineKeyboardMarkup]]:
    with get_db() as conn:
        m = conn.execute(
            "SELECT * FROM merchants WHERE merchant_id = ? AND owner_tg_id = ?",
            (merchant_id, owner_id),
        ).fetchone()
        if not m:
            return None
        paid = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(amount_rp), 0) AS vol FROM orders WHERE merchant_id = ? AND status = 'paid'",
            (merchant_id,),
        ).fetchone()

    wh = f"<code>{esc(m['webhook_url'])}</code>" if m["webhook_url"] else "<i>Только Telegram-сигналы</i>"
    text = (
        f"📦 <b>{esc(m['title'])}</b>\n\n"
        f"• Баланс проекта: <b>{m['balance_rp']} RP</b>\n"
        f"• Оплат: <b>{paid['cnt']}</b> (оборот {paid['vol']} RP)\n"
        f"• Комиссия: <b>{int(COMMISSION_RATE * 100)}%</b>\n\n"
        f"🆔 ID: <code>{m['merchant_id']}</code>\n"
        f"🔑 API Key: <tg-spoiler><code>{m['api_key']}</code></tg-spoiler>\n"
        f"🌐 Webhook: {wh}"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Создать счёт / QR", callback_data=f"qp_select:{merchant_id}")],
            [InlineKeyboardButton(text="💸 Вывести на личный кошелёк", callback_data=f"withdraw_proj:{merchant_id}")],
            [
                InlineKeyboardButton(text="🔗 Webhook", callback_data=f"edit_wh:{merchant_id}"),
                InlineKeyboardButton(text="🔄 Сбросить ключ", callback_data=f"reset_key:{merchant_id}"),
            ],
            [InlineKeyboardButton(text="◀️ К проектам", callback_data="projects")],
        ]
    )
    return text, kb


@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext) -> None:
    await state.clear()
    ensure_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

    if command.args and command.args.startswith("pay_"):
        order_id = command.args[4:]
        with get_db() as conn:
            order = conn.execute(
                """
                SELECT o.*, m.title AS merchant_title
                FROM orders o JOIN merchants m ON o.merchant_id = m.merchant_id
                WHERE o.order_id = ?
                """,
                (order_id,),
            ).fetchone()

        if not order:
            await message.answer("Счёт не найден или устарел.")
            return
        if order["status"] == "paid":
            await message.answer(f"✅ Счёт <b>«{esc(order['title'])}»</b> уже оплачен.", parse_mode="HTML")
            return

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"Оплатить {order['amount_rp']} RP",
                        web_app=WebAppInfo(url=f"{PUBLIC_URL}/?order_id={order_id}"),
                    )
                ]
            ]
        )
        await message.answer(
            f"<b>Счёт на оплату • {esc(order['merchant_title'])}</b>\n\n"
            f"• Услуга: <b>{esc(order['title'])}</b>\n"
            f"• К оплате: <b>{order['amount_rp']} RP</b>",
            parse_mode="HTML",
            reply_markup=kb,
        )
        return

    text, kb = build_main_menu(message.from_user)
    await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    text, kb = build_main_menu(callback.from_user)
    await safe_edit(callback.message, text, kb)
    await callback.answer()


# --- КОШЕЛЁК И ПОПОЛНЕНИЕ ПОДАРКОМ ---
@dp.callback_query(F.data == "wallet")
async def cb_wallet(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    uid = callback.from_user.id
    with get_db() as conn:
        u = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (uid,)).fetchone()
        reqs = conn.execute(
            "SELECT request_id, amount_rp, status FROM topup_requests WHERE user_tg_id = ? ORDER BY created_at DESC LIMIT 4",
            (uid,),
        ).fetchall()

    bal = u["wallet_balance"] if u else 0
    lines = [
        f"💳 <b>Личный кошелёк RoboPay</b>\n",
        f"• Баланс: <b>{bal} RP</b>",
        f"• Курс: <b>1 ⭐ = 1 RP</b>\n",
        f"<b>Как пополнить баланс:</b>",
        f"1. Перейдите в профиль владельца по ссылке: <a href=\"{OWNER_URL}\">{esc(OWNER_DISPLAY)}</a> ({OWNER_URL})",
        f"2. Отправьте Telegram-подарок на нужную сумму звёзд.",
        f"3. Нажмите <b>«✅ Я отправил подарок»</b> — владелец проверит и зачислит RP.",
    ]
    if reqs:
        status_map = {"pending": "⏳ На проверке", "approved": "✅ Одобрено", "rejected": "❌ Отклонено"}
        lines.append("\n<b>Последние заявки:</b>")
        for r in reqs:
            lines.append(f"• <code>{r['request_id']}</code>: {r['amount_rp']} RP — {status_map.get(r['status'], r['status'])}")

    buttons = [
        [InlineKeyboardButton(text=f"🎁 Отправить подарок ({OWNER_DISPLAY})", url=OWNER_URL)],
        [InlineKeyboardButton(text="✅ Я отправил подарок (подать заявку)", callback_data="topup_start")],
    ]
    if PUBLIC_URL.startswith("https://"):
        buttons.append([InlineKeyboardButton(text="Открыть кошелёк и QR в Mini App", web_app=WebAppInfo(url=f"{PUBLIC_URL}/#wallet"))])
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])

    await safe_edit(callback.message, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@dp.callback_query(F.data == "topup_start")
async def cb_topup_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.set_state(TopUpFSM.waiting_amount)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🎁 Открыть профиль {OWNER_DISPLAY}", url=OWNER_URL)],
            [
                InlineKeyboardButton(text="15 ⭐", callback_data="top_amt:15"),
                InlineKeyboardButton(text="25 ⭐", callback_data="top_amt:25"),
                InlineKeyboardButton(text="50 ⭐", callback_data="top_amt:50"),
                InlineKeyboardButton(text="100 ⭐", callback_data="top_amt:100"),
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="wallet")],
        ]
    )
    await safe_edit(
        callback.message,
        f"🎁 <b>Заявка на пополнение (шаг 1 из 2)</b>\n\n"
        f"Ссылка для отправки подарка: <a href=\"{OWNER_URL}\">{OWNER_URL}</a>\n\n"
        f"Выберите сумму отправленного подарка кнопкой ниже или введите число вручную:",
        kb,
    )
    await callback.answer()


async def ask_topup_comment(target: types.Message, state: FSMContext, amount: int, is_edit: bool) -> None:
    await state.update_data(amount_rp=amount)
    await state.set_state(TopUpFSM.waiting_comment)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Отправить без комментария", callback_data="topup_no_comment")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="wallet")],
        ]
    )
    text = (
        f"🎁 <b>Сумма: {amount} ⭐ → {amount} RP (шаг 2 из 2)</b>\n\n"
        f"Укажите какой подарок вы отправили (например: <i>мишка за 15</i>) или нажмите «Отправить без комментария»:"
    )
    if is_edit:
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.callback_query(TopUpFSM.waiting_amount, F.data.startswith("top_amt:"))
async def fsm_topup_amount_btn(callback: types.CallbackQuery, state: FSMContext) -> None:
    amt = int(callback.data.split(":", 1)[1])
    await ask_topup_comment(callback.message, state, amt, is_edit=True)
    await callback.answer()


@dp.message(TopUpFSM.waiting_amount)
async def fsm_topup_amount_txt(message: types.Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    if not raw.isdigit() or not (1 <= int(raw) <= 500000):
        await message.answer("Введите целое число звёзд от 1 до 500000.")
        return
    await ask_topup_comment(message, state, int(raw), is_edit=False)


@dp.callback_query(TopUpFSM.waiting_comment, F.data == "topup_no_comment")
async def fsm_topup_submit_empty(callback: types.CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    req = await create_topup_request_and_notify(callback.from_user.id, data["amount_rp"], "")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ В кошелёк", callback_data="wallet")]])
    await safe_edit(
        callback.message,
        f"⏳ <b>Заявка <code>{req['request_id']}</code> отправлена владельцу!</b>\n\n"
        f"Сумма: <b>{req['amount_rp']} RP</b>.\nКак только {esc(OWNER_DISPLAY)} подтвердит подарок, баланс зачислится автоматически.",
        kb,
    )
    await callback.answer("Заявка отправлена")


@dp.message(TopUpFSM.waiting_comment)
async def fsm_topup_submit_comment(message: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    comment = (message.text or "").strip()[:200]
    req = await create_topup_request_and_notify(message.from_user.id, data["amount_rp"], comment)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ В кошелёк", callback_data="wallet")]])
    await message.answer(
        f"⏳ <b>Заявка <code>{req['request_id']}</code> отправлена владельцу!</b>\n\n"
        f"Сумма: <b>{req['amount_rp']} RP</b>.\nКак только {esc(OWNER_DISPLAY)} подтвердит подарок, вам придёт уведомление.",
        parse_mode="HTML",
        reply_markup=kb,
    )


# --- ПРОЕКТЫ ---
@dp.callback_query(F.data == "projects")
async def cb_projects(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT merchant_id, title, balance_rp FROM merchants WHERE owner_tg_id = ? ORDER BY created_at DESC",
            (callback.from_user.id,),
        ).fetchall()

    buttons = [
        [InlineKeyboardButton(text=f"📦 {r['title']} ({r['balance_rp']} RP)", callback_data=f"proj:{r['merchant_id']}")]
        for r in rows
    ]
    buttons.append([InlineKeyboardButton(text="➕ Новый проект", callback_data="new_project")])
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])

    text = "📦 <b>Ваши проекты</b>\n\nВыберите проект для управления ключами, счетами и балансом:" if rows else "📦 <b>Проектов пока нет</b>\n\nСоздайте первый проект, чтобы принимать оплату по ссылкам, QR и API."
    await safe_edit(callback.message, text, InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@dp.callback_query(F.data.startswith("proj:"))
async def cb_project_detail(callback: types.CallbackQuery) -> None:
    mid = callback.data.split(":", 1)[1]
    card = build_project_card(mid, callback.from_user.id)
    if not card:
        await callback.answer("Проект не найден", show_alert=True)
        return
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer()


@dp.callback_query(F.data.startswith("withdraw_proj:"))
async def cb_withdraw_project(callback: types.CallbackQuery) -> None:
    mid = callback.data.split(":", 1)[1]
    uid = callback.from_user.id
    with get_db() as conn:
        row = conn.execute(
            "SELECT balance_rp FROM merchants WHERE merchant_id = ? AND owner_tg_id = ?",
            (mid, uid),
        ).fetchone()
        if not row or row["balance_rp"] <= 0:
            await callback.answer("На балансе проекта 0 RP", show_alert=True)
            return
        amt = row["balance_rp"]
        conn.execute("UPDATE merchants SET balance_rp = 0 WHERE merchant_id = ? AND balance_rp = ?", (mid, amt))
        conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (amt, uid))

    asyncio.create_task(sync_db_to_telegram(f"Вывод {amt} RP из проекта {mid} на кошелёк {uid}"))
    card = build_project_card(mid, uid)
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer(f"✅ Переведено +{amt} RP на ваш кошелёк!", show_alert=True)


@dp.callback_query(F.data == "new_project")
async def cb_new_project(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.set_state(CreateProjectFSM.waiting_title)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="projects")]])
    await safe_edit(callback.message, "🆕 <b>Новый проект (1/2)</b>\n\nОтправьте название вашего бота или сервиса:", kb)
    await callback.answer()


@dp.message(CreateProjectFSM.waiting_title)
async def fsm_project_title(message: types.Message, state: FSMContext) -> None:
    title = (message.text or "").strip()[:40]
    if not title:
        return
    await state.update_data(title=title)
    await state.set_state(CreateProjectFSM.waiting_webhook)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить (только ТГ-сигналы)", callback_data="skip_webhook")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="projects")],
        ]
    )
    await message.answer(
        f"🆕 <b>Проект «{esc(title)}» (2/2)</b>\n\nОтправьте HTTPS-ссылку для Webhook или нажмите «Пропустить»:",
        parse_mode="HTML",
        reply_markup=kb,
    )


async def finalize_project(owner_id: int, title: str, webhook_url: Optional[str]) -> str:
    mid = f"mrc_{secrets.token_hex(5)}"
    api_key = f"rp_live_{secrets.token_hex(16)}"
    with get_db() as conn:
        conn.execute(
            "INSERT INTO merchants (merchant_id, owner_tg_id, title, api_key, webhook_url, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (mid, owner_id, title, api_key, webhook_url, utc_now()),
        )
    asyncio.create_task(sync_db_to_telegram(f"Новый проект: {title} ({mid})"))
    return mid


@dp.callback_query(CreateProjectFSM.waiting_webhook, F.data == "skip_webhook")
async def fsm_project_skip_wh(callback: types.CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    mid = await finalize_project(callback.from_user.id, data["title"], None)
    card = build_project_card(mid, callback.from_user.id)
    await safe_edit(callback.message, f"✅ <b>Проект создан!</b>\n\n{card[0]}", card[1])
    await callback.answer()


@dp.message(CreateProjectFSM.waiting_webhook)
async def fsm_project_wh(message: types.Message, state: FSMContext) -> None:
    url = (message.text or "").strip()
    if not url.startswith(("https://", "http://")):
        await message.answer("URL должен начинаться с https://")
        return
    data = await state.get_data()
    await state.clear()
    mid = await finalize_project(message.from_user.id, data["title"], url)
    card = build_project_card(mid, message.from_user.id)
    await message.answer(f"✅ <b>Проект создан!</b>\n\n{card[0]}", parse_mode="HTML", reply_markup=card[1])


@dp.callback_query(F.data.startswith("edit_wh:"))
async def cb_edit_wh(callback: types.CallbackQuery, state: FSMContext) -> None:
    mid = callback.data.split(":", 1)[1]
    await state.set_state(EditWebhookFSM.waiting_url)
    await state.update_data(merchant_id=mid)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Отключить Webhook", callback_data=f"clear_wh:{mid}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"proj:{mid}")],
        ]
    )
    await safe_edit(callback.message, "🔗 Отправьте новый HTTPS-адрес для Webhook:", kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("clear_wh:"))
async def cb_clear_wh(callback: types.CallbackQuery, state: FSMContext) -> None:
    mid = callback.data.split(":", 1)[1]
    await state.clear()
    with get_db() as conn:
        conn.execute("UPDATE merchants SET webhook_url = NULL WHERE merchant_id = ? AND owner_tg_id = ?", (mid, callback.from_user.id))
    asyncio.create_task(sync_db_to_telegram(f"Отключён webhook ({mid})"))
    card = build_project_card(mid, callback.from_user.id)
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer("Webhook отключён")


@dp.message(EditWebhookFSM.waiting_url)
async def fsm_save_wh(message: types.Message, state: FSMContext) -> None:
    url = (message.text or "").strip()
    if not url.startswith(("https://", "http://")):
        await message.answer("URL должен начинаться с https://")
        return
    mid = (await state.get_data())["merchant_id"]
    await state.clear()
    with get_db() as conn:
        conn.execute("UPDATE merchants SET webhook_url = ? WHERE merchant_id = ? AND owner_tg_id = ?", (url, mid, message.from_user.id))
    asyncio.create_task(sync_db_to_telegram(f"Обновлён webhook ({mid})"))
    card = build_project_card(mid, message.from_user.id)
    await message.answer(card[0], parse_mode="HTML", reply_markup=card[1])


@dp.callback_query(F.data.startswith("reset_key:"))
async def cb_reset_key(callback: types.CallbackQuery) -> None:
    mid = callback.data.split(":", 1)[1]
    new_key = f"rp_live_{secrets.token_hex(16)}"
    with get_db() as conn:
        conn.execute("UPDATE merchants SET api_key = ? WHERE merchant_id = ? AND owner_tg_id = ?", (new_key, mid, callback.from_user.id))
    asyncio.create_task(sync_db_to_telegram(f"Сброшен API-ключ ({mid})"))
    card = build_project_card(mid, callback.from_user.id)
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer("🔑 Ключ обновлён", show_alert=True)


# --- БЫСТРЫЙ СЧЁТ В БОТЕ ---
@dp.callback_query(F.data == "quick_pay")
async def cb_quick_pay(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    with get_db() as conn:
        rows = conn.execute("SELECT merchant_id, title FROM merchants WHERE owner_tg_id = ?", (callback.from_user.id,)).fetchall()
    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать первый проект", callback_data="new_project")],
                [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
            ]
        )
        await safe_edit(callback.message, "⚡ Сначала создайте проект — на его баланс будут поступать оплаты.", kb)
        await callback.answer()
        return

    if len(rows) == 1:
        await start_quick_invoice(callback, state, rows[0]["merchant_id"])
        return

    btns = [[InlineKeyboardButton(text=f"📦 {r['title']}", callback_data=f"qp_select:{r['merchant_id']}")] for r in rows]
    btns.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])
    await safe_edit(callback.message, "⚡ Выберите проект для счёта:", InlineKeyboardMarkup(inline_keyboard=btns))
    await callback.answer()


@dp.callback_query(F.data.startswith("qp_select:"))
async def cb_qp_select(callback: types.CallbackQuery, state: FSMContext) -> None:
    await start_quick_invoice(callback, state, callback.data.split(":", 1)[1])


async def start_quick_invoice(callback: types.CallbackQuery, state: FSMContext, mid: str) -> None:
    await state.set_state(QuickInvoiceFSM.waiting_title)
    await state.update_data(merchant_id=mid)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")]])
    await safe_edit(callback.message, "⚡ <b>Новый счёт (1/2)</b>\n\nВведите название услуги:", kb)
    await callback.answer()


@dp.message(QuickInvoiceFSM.waiting_title)
async def fsm_qp_title(message: types.Message, state: FSMContext) -> None:
    title = (message.text or "").strip()[:48]
    if not title:
        return
    await state.update_data(title=title)
    await state.set_state(QuickInvoiceFSM.waiting_amount)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="25 RP", callback_data="qp_amt:25"),
                InlineKeyboardButton(text="50 RP", callback_data="qp_amt:50"),
                InlineKeyboardButton(text="100 RP", callback_data="qp_amt:100"),
                InlineKeyboardButton(text="250 RP", callback_data="qp_amt:250"),
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")],
        ]
    )
    await message.answer(
        f"⚡ <b>Услуга «{esc(title)}» (2/2)</b>\n\nВыберите сумму в RP или введите вручную:",
        parse_mode="HTML",
        reply_markup=kb,
    )


async def send_invoice_done(target: types.Message, mid: str, title: str, amount: int, is_edit: bool) -> None:
    order = create_order_record(mid, title, "Оплата через терминал RoboPay", amount)
    asyncio.create_task(sync_db_to_telegram(f"Создан счёт {order['order_id']} ({amount} RP)"))
    text = (
        f"✅ <b>Счёт создан</b>\n\n"
        f"• Услуга: <b>{esc(title)}</b>\n"
        f"• Сумма: <b>{amount} RP</b> (к зачислению {order['net_rp']} RP)\n\n"
        f"🔗 <b>Ссылка на оплату:</b>\n<code>{order['checkout_deeplink']}</code>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть чекаут / QR", web_app=WebAppInfo(url=order["webapp_url"]))],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
        ]
    )
    if is_edit:
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)


@dp.callback_query(QuickInvoiceFSM.waiting_amount, F.data.startswith("qp_amt:"))
async def fsm_qp_amt_btn(callback: types.CallbackQuery, state: FSMContext) -> None:
    amt = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    await state.clear()
    await send_invoice_done(callback.message, data["merchant_id"], data["title"], amt, is_edit=True)
    await callback.answer()


@dp.message(QuickInvoiceFSM.waiting_amount)
async def fsm_qp_amt_txt(message: types.Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    if not raw.isdigit() or not (1 <= int(raw) <= 500000):
        await message.answer("Введите целое число от 1 до 500000.")
        return
    data = await state.get_data()
    await state.clear()
    await send_invoice_done(message, data["merchant_id"], data["title"], int(raw), is_edit=False)


# --- API ДОКУМЕНТАЦИЯ ---
@dp.callback_query(F.data == "api_docs")
async def cb_api_docs(callback: types.CallbackQuery) -> None:
    text = (
        f"📖 <b>RoboPay API</b>\n\n"
        f"<b>Создание счёта:</b> <code>POST {PUBLIC_URL}/api/v1/invoices</code>\n"
        f"<b>Заголовок:</b> <code>X-API-Key: ваш_ключ</code>\n"
        f"<pre>{{\n"
        f'  "title": "Подписка Pro",\n'
        f'  "description": "30 дней доступа",\n'
        f'  "amount": 100,\n'
        f'  "external_order_id": "u_42"\n'
        f"}}</pre>\n"
        f"Отдайте пользователю <code>checkout_deeplink</code>. При оплате придёт сигнал в ТГ и Webhook (<code>X-RoboPay-Signature</code>, HMAC-SHA256)."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")]])
    await safe_edit(callback.message, text, kb)
    await callback.answer()


# --- АДМИН-ПАНЕЛЬ, ОДОБРЕНИЕ ЗАЯВОК И БЭКАП (СТРОГО ADMIN_ID) ---
def build_admin_panel() -> tuple[str, InlineKeyboardMarkup]:
    with get_db() as conn:
        u_cnt = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        w_sum = conn.execute("SELECT COALESCE(SUM(wallet_balance), 0) FROM users").fetchone()[0]
        p_cnt = conn.execute("SELECT COUNT(*) FROM merchants").fetchone()[0]
        pend_cnt = conn.execute("SELECT COUNT(*) FROM topup_requests WHERE status = 'pending'").fetchone()[0]
        o_fee = conn.execute("SELECT COALESCE(SUM(fee_rp), 0) FROM orders WHERE status = 'paid'").fetchone()[0]
        t_fee = conn.execute("SELECT COALESCE(SUM(fee_rp), 0) FROM transfers").fetchone()[0]

    text = (
        f"🛠 <b>Админ-панель RoboPay</b>\n\n"
        f"• Пользователей: <b>{u_cnt}</b> (на кошельках: {w_sum} RP)\n"
        f"• Проектов: <b>{p_cnt}</b>\n"
        f"• Заявок на проверку: <b>{pend_cnt}</b>\n"
        f"• Доход с комиссий: <b>{o_fee + t_fee} RP</b>\n"
        f"• Приём подарков: <a href=\"{OWNER_URL}\">{esc(OWNER_DISPLAY)}</a>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"⏳ Заявки на пополнение ({pend_cnt})", callback_data="admin_topups")],
            [InlineKeyboardButton(text="💾 Бэкап БД в канал", callback_data="admin_backup")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_panel")],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
        ]
    )
    return text, kb


@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: types.CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    text, kb = build_admin_panel()
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data == "admin_topups")
async def cb_admin_topups(callback: types.CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    with get_db() as conn:
        req = conn.execute(
            """
            SELECT r.*, u.username, u.first_name
            FROM topup_requests r LEFT JOIN users u ON r.user_tg_id = u.tg_id
            WHERE r.status = 'pending' ORDER BY r.created_at ASC LIMIT 1
            """
        ).fetchone()
    if not req:
        await callback.answer("Все заявки разобраны!", show_alert=True)
        return

    uname = f"@{req['username']}" if req["username"] else "без username"
    text = (
        f"⏳ <b>Заявка <code>{req['request_id']}</code></b>\n\n"
        f"• Клиент: <b>{esc(req['first_name'] or 'User')}</b> ({uname})\n"
        f"• ID: <code>{req['user_tg_id']}</code>\n"
        f"• Сумма: <b>{req['amount_rp']} ⭐ → {req['amount_rp']} RP</b>\n"
        f"• Комментарий: <i>{esc(req['comment'] or '—')}</i>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"✅ Одобрить (+{req['amount_rp']} RP)", callback_data=f"topup_ok:{req['request_id']}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"topup_no:{req['request_id']}"),
            ],
            [InlineKeyboardButton(text="◀️ В админ-панель", callback_data="admin_panel")],
        ]
    )
    await safe_edit(callback.message, text, kb)
    await callback.answer()


@dp.callback_query(F.data.startswith(("topup_ok:", "topup_no:")))
async def cb_review_topup(callback: types.CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    action, req_id = callback.data.split(":", 1)
    approve = action == "topup_ok"
    new_status = "approved" if approve else "rejected"

    with get_db() as conn:
        cur = conn.execute(
            "UPDATE topup_requests SET status = ?, reviewed_at = ? WHERE request_id = ? AND status = 'pending'",
            (new_status, utc_now(), req_id),
        )
        if cur.rowcount == 0:
            await callback.answer("Заявка уже обработана.", show_alert=True)
            return

        req = conn.execute("SELECT * FROM topup_requests WHERE request_id = ?", (req_id,)).fetchone()
        if approve:
            conn.execute(
                "UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?",
                (req["amount_rp"], req["user_tg_id"]),
            )

    asyncio.create_task(sync_db_to_telegram(f"Заявка {req_id}: {new_status} ({req['amount_rp']} RP)"))

    verdict_badge = f"✅ <b>ОДОБРЕНО (+{req['amount_rp']} RP)</b>" if approve else "❌ <b>ОТКЛОНЕНО</b>"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛠 В админ-панель", callback_data="admin_panel")]])
    await safe_edit(
        callback.message,
        f"{verdict_badge}\n• Заявка: <code>{req_id}</code>\n• Пользователь: <code>{req['user_tg_id']}</code>",
        kb,
    )
    await callback.answer("Готово!")

    try:
        if approve:
            await bot.send_message(
                req["user_tg_id"],
                f"✅ <b>Пополнение одобрено!</b>\n\nНа ваш кошелёк RoboPay зачислено <b>+{req['amount_rp']} RP</b> (заявка <code>{req_id}</code>).",
                parse_mode="HTML",
            )
        else:
            await bot.send_message(
                req["user_tg_id"],
                f"❌ <b>Заявка <code>{req_id}</code> на {req['amount_rp']} RP отклонена.</b>\nСвязь с владельцем: <a href=\"{OWNER_URL}\">{esc(OWNER_DISPLAY)}</a>",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
    except Exception:
        pass


@dp.callback_query(F.data == "admin_backup")
async def cb_admin_backup(callback: types.CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    msg_id = await sync_db_to_telegram("Ручной бэкап из админ-панели")
    await callback.answer(f"✅ Бэкап закреплён (#{msg_id})" if msg_id else "⚠️ Проверьте DB_CHANNEL_ID", show_alert=True)


@dp.message(Command("backup"))
async def cmd_backup(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return
    msg_id = await sync_db_to_telegram("Ручной бэкап /backup")
    await message.answer(f"✅ Бэкап закреплён (ID: <code>{msg_id}</code>)." if msg_id else "⚠️ Ошибка бэкапа.", parse_mode="HTML")


# --- ЖИВУЧИЙ POLLING-СУПЕРВИЗОР ---
async def run_bot_polling() -> None:
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Polling Error] {e} — перезапуск через 3 сек...")
            await asyncio.sleep(3)


# --- FASTAPI: REST API И MINI APP API ---
class CreateInvoiceReq(BaseModel):
    title: str = Field(..., min_length=1, max_length=48)
    description: str = Field(default="Оплата услуги через RoboPay", max_length=255)
    amount: Optional[int] = Field(default=None, ge=1, le=500000)
    amount_stars: Optional[int] = Field(default=None, ge=1, le=500000)
    external_order_id: Optional[str] = Field(default=None, max_length=128)


class WebAppInvoiceReq(BaseModel):
    merchant_id: str
    title: str = Field(..., min_length=1, max_length=48)
    amount: int = Field(..., ge=1, le=500000)


class TopUpReq(BaseModel):
    amount: int = Field(..., ge=1, le=500000)
    comment: str = Field(default="", max_length=200)


class TransferReq(BaseModel):
    to_tg_id: int = Field(..., ge=1)
    amount: int = Field(..., ge=1, le=500000)
    comment: str = Field(default="", max_length=120)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_USERNAME
    await restore_db_from_telegram()
    me = await bot.get_me()
    BOT_USERNAME = me.username
    print(f"[Startup] Бот @{BOT_USERNAME} запущен. Приём подарков: {OWNER_URL}")

    try:
        await bot.set_my_commands([BotCommand(command="start", description="Открыть меню RoboPay")])
        if PUBLIC_URL.startswith("https://"):
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="RoboPay", web_app=WebAppInfo(url=f"{PUBLIC_URL}/"))
            )
    except Exception as e:
        print(f"[Bot Setup Warning] {e}")

    polling_task = asyncio.create_task(run_bot_polling())
    yield
    polling_task.cancel()
    try:
        await polling_task
    except asyncio.CancelledError:
        pass
    await bot.session.close()


app = FastAPI(title="RoboPay Gateway", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"ok": True, "bot": BOT_USERNAME}


@app.get("/")
async def serve_miniapp():
    return FileResponse("index.html")


@app.get("/api/v1/me")
async def api_me(x_telegram_init_data: str = Header(default="")):
    tg_user = verify_telegram_init_data(x_telegram_init_data)
    uid = int(tg_user["id"])
    ensure_user(uid, tg_user.get("username"), tg_user.get("first_name"))

    with get_db() as conn:
        u = conn.execute("SELECT tg_id, username, first_name, wallet_balance FROM users WHERE tg_id = ?", (uid,)).fetchone()
        projects = [
            dict(r)
            for r in conn.execute(
                "SELECT merchant_id, title, balance_rp FROM merchants WHERE owner_tg_id = ? ORDER BY created_at DESC",
                (uid,),
            ).fetchall()
        ]
        topups = [
            dict(r)
            for r in conn.execute(
                "SELECT request_id, amount_rp, status, created_at FROM topup_requests WHERE user_tg_id = ? ORDER BY created_at DESC LIMIT 8",
                (uid,),
            ).fetchall()
        ]
        paid_orders = [
            dict(r)
            for r in conn.execute(
                """
                SELECT o.order_id AS id, o.title, -o.amount_rp AS delta, o.paid_at AS ts, 'order' AS kind
                FROM orders o WHERE o.payer_tg_id = ? AND o.status = 'paid'
                ORDER BY o.paid_at DESC LIMIT 8
                """,
                (uid,),
            ).fetchall()
        ]
        tr_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT transfer_id AS id,
                       CASE WHEN from_tg_id = ? THEN 'Перевод → ' || to_tg_id ELSE 'Перевод от ' || from_tg_id END AS title,
                       CASE WHEN from_tg_id = ? THEN -amount_rp ELSE net_rp END AS delta,
                       created_at AS ts,
                       'transfer' AS kind
                FROM transfers WHERE from_tg_id = ? OR to_tg_id = ?
                ORDER BY created_at DESC LIMIT 8
                """,
                (uid, uid, uid, uid),
            ).fetchall()
        ]

    history = sorted(paid_orders + tr_rows, key=lambda x: x["ts"] or "", reverse=True)[:10]
    return {
        "user": dict(u),
        "owner_username": OWNER_DISPLAY,
        "owner_url": OWNER_URL,
        "bot_username": BOT_USERNAME,
        "commission_rate": COMMISSION_RATE,
        "projects": projects,
        "topups": topups,
        "history": history,
    }


@app.post("/api/v1/topup")
async def api_topup(req: TopUpReq, x_telegram_init_data: str = Header(default="")):
    tg_user = verify_telegram_init_data(x_telegram_init_data)
    uid = int(tg_user["id"])
    ensure_user(uid, tg_user.get("username"), tg_user.get("first_name"))
    return await create_topup_request_and_notify(uid, req.amount, req.comment)


@app.post("/api/v1/transfer")
async def api_transfer(req: TransferReq, x_telegram_init_data: str = Header(default="")):
    tg_user = verify_telegram_init_data(x_telegram_init_data)
    from_id = int(tg_user["id"])
    ensure_user(from_id, tg_user.get("username"), tg_user.get("first_name"))

    if req.to_tg_id == from_id:
        raise HTTPException(status_code=400, detail="Нельзя перевести самому себе")

    fee_rp = int(req.amount * COMMISSION_RATE)
    net_rp = max(1, req.amount - fee_rp)
    tr_id = f"tr_{secrets.token_hex(6)}"

    with get_db() as conn:
        recipient = conn.execute("SELECT tg_id FROM users WHERE tg_id = ?", (req.to_tg_id,)).fetchone()
        if not recipient:
            raise HTTPException(status_code=404, detail="Получатель ещё не запускал RoboPay")

        cur = conn.execute(
            "UPDATE users SET wallet_balance = wallet_balance - ? WHERE tg_id = ? AND wallet_balance >= ?",
            (req.amount, from_id, req.amount),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=400, detail="Недостаточно средств на кошельке")

        conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (net_rp, req.to_tg_id))
        conn.execute(
            """
            INSERT INTO transfers (transfer_id, from_tg_id, to_tg_id, amount_rp, fee_rp, net_rp, comment, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (tr_id, from_id, req.to_tg_id, req.amount, fee_rp, net_rp, req.comment, utc_now()),
        )
        new_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (from_id,)).fetchone()["wallet_balance"]

    asyncio.create_task(sync_db_to_telegram(f"Перевод {tr_id}: {from_id} → {req.to_tg_id} ({req.amount} RP)"))
    try:
        await bot.send_message(
            req.to_tg_id,
            f"💸 <b>Входящий перевод RoboPay</b>\n\n• Зачислено: <b>+{net_rp} RP</b>\n• От: <code>{from_id}</code>"
            + (f"\n• Комментарий: <i>{esc(req.comment)}</i>" if req.comment else ""),
            parse_mode="HTML",
        )
    except Exception:
        pass

    return {"transfer_id": tr_id, "net_rp": net_rp, "fee_rp": fee_rp, "wallet_balance": new_bal}


@app.post("/api/v1/webapp/invoices")
async def api_webapp_invoice(req: WebAppInvoiceReq, x_telegram_init_data: str = Header(default="")):
    tg_user = verify_telegram_init_data(x_telegram_init_data)
    uid = int(tg_user["id"])
    with get_db() as conn:
        m = conn.execute(
            "SELECT merchant_id FROM merchants WHERE merchant_id = ? AND owner_tg_id = ?",
            (req.merchant_id, uid),
        ).fetchone()
    if not m:
        raise HTTPException(status_code=403, detail="Проект не найден")
    order = create_order_record(req.merchant_id, req.title, "Счёт RoboPay", req.amount)
    asyncio.create_task(sync_db_to_telegram(f"WebApp-счёт {order['order_id']} ({req.amount} RP)"))
    return order


@app.post("/api/v1/invoices")
async def api_external_invoice(req: CreateInvoiceReq, x_api_key: str = Header(...)):
    amount = req.amount or req.amount_stars
    if not amount:
        raise HTTPException(status_code=422, detail="Укажите поле amount")
    with get_db() as conn:
        m = conn.execute("SELECT merchant_id FROM merchants WHERE api_key = ?", (x_api_key,)).fetchone()
    if not m:
        raise HTTPException(status_code=401, detail="Invalid X-API-Key")

    order = create_order_record(m["merchant_id"], req.title, req.description, amount, req.external_order_id)
    asyncio.create_task(sync_db_to_telegram(f"API-счёт {order['order_id']} ({amount} RP)"))
    return order


@app.get("/api/v1/orders/{order_id}")
async def api_get_order(order_id: str):
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT o.order_id, o.title, o.description, o.amount_rp, o.status,
                   m.title AS merchant_title
            FROM orders o JOIN merchants m ON o.merchant_id = m.merchant_id
            WHERE o.order_id = ?
            """,
            (order_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    data = dict(row)
    data["qr_payload"] = f"https://t.me/{BOT_USERNAME}?start=pay_{order_id}"
    return data


@app.post("/api/v1/orders/{order_id}/pay")
async def api_pay_order(order_id: str, x_telegram_init_data: str = Header(default="")):
    tg_user = verify_telegram_init_data(x_telegram_init_data)
    payer_id = int(tg_user["id"])
    ensure_user(payer_id, tg_user.get("username"), tg_user.get("first_name"))
    now = utc_now()

    with get_db() as conn:
        order = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Счёт не найден")
        if order["status"] == "paid":
            raise HTTPException(status_code=409, detail="Счёт уже оплачен")

        cur_w = conn.execute(
            "UPDATE users SET wallet_balance = wallet_balance - ? WHERE tg_id = ? AND wallet_balance >= ?",
            (order["amount_rp"], payer_id, order["amount_rp"]),
        )
        if cur_w.rowcount == 0:
            raise HTTPException(status_code=402, detail="Недостаточно средств на кошельке")

        cur_o = conn.execute(
            "UPDATE orders SET status = 'paid', payer_tg_id = ?, paid_at = ? WHERE order_id = ? AND status = 'pending'",
            (payer_id, now, order_id),
        )
        if cur_o.rowcount == 0:
            raise HTTPException(status_code=409, detail="Счёт уже оплачен")

        conn.execute(
            "UPDATE merchants SET balance_rp = balance_rp + ? WHERE merchant_id = ?",
            (order["net_rp"], order["merchant_id"]),
        )
        updated_order = dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())
        merchant = dict(conn.execute("SELECT * FROM merchants WHERE merchant_id = ?", (order["merchant_id"],)).fetchone())
        new_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (payer_id,)).fetchone()["wallet_balance"]

    asyncio.create_task(dispatch_partner_notification(updated_order, merchant))
    asyncio.create_task(sync_db_to_telegram(f"Оплата {order_id} ({updated_order['amount_rp']} RP)"))
    return {"status": "paid", "order_id": order_id, "wallet_balance": new_bal}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=env_int("PORT", 8000), reload=False)

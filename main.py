import asyncio
import hashlib
import hmac
import html
import json
import math
import os
import re
import secrets
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from urllib.parse import parse_qsl

import httpx
import uvicorn
from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
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
    LabeledPrice,
)
from fastapi import FastAPI, Header, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field


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
    cleaned = re.sub(r"^https?://t\.me/", "", raw.strip(), flags=re.IGNORECASE)
    cleaned = cleaned.lstrip("@").split("/")[0].split("?")[0].strip() or "owner"
    return f"@{cleaned}", f"https://t.me/{cleaned}"


# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")
OWNER_DISPLAY, OWNER_URL = normalize_owner(os.getenv("OWNER_USERNAME", "@owner"))
COMMISSION_RATE = env_float("COMMISSION_RATE", 0.05)
WITHDRAW_FEE_RATE = env_float("WITHDRAW_FEE_RATE", 0.20)
DB_PATH = os.getenv("DB_PATH", "gateway.db").strip() or "gateway.db"
DB_CHANNEL_ID = env_int("DB_CHANNEL_ID", 0)
ADMIN_ID = env_int("ADMIN_ID", 0)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant").strip()

GIFT_TIERS = {15: 13, 25: 21, 50: 43, 100: 85}
MAX_TOPUP_STARS = 100

if not BOT_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN!")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
BOT_USERNAME = ""
CACHED_GROQ_MODEL: Optional[str] = None
db_sync_lock = asyncio.Lock()


# --- REAL-TIME WEBSOCKET ХАБ ---
class RealtimeHub:
    def __init__(self) -> None:
        self.clients: Dict[int, Set[WebSocket]] = {}

    async def connect(self, tg_id: int, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.setdefault(tg_id, set()).add(ws)

    def disconnect(self, tg_id: int, ws: WebSocket) -> None:
        conns = self.clients.get(tg_id)
        if conns:
            conns.discard(ws)
            if not conns:
                self.clients.pop(tg_id, None)

    async def push(self, tg_id: int, payload: dict) -> None:
        conns = list(self.clients.get(tg_id, set()))
        dead = []
        for ws in conns:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(tg_id, ws)


rt_hub = RealtimeHub()


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


class BroadcastFSM(StatesGroup):
    waiting_message = State()
    waiting_confirm = State()


def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID


def esc(text: Optional[str]) -> str:
    return html.escape(str(text or ""))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def calc_withdraw_cost(gift_stars: int) -> tuple[int, int, int]:
    fee_rp = math.ceil(gift_stars * WITHDRAW_FEE_RATE) if WITHDRAW_FEE_RATE > 0 else 0
    return gift_stars + fee_rp, fee_rp, GIFT_TIERS.get(gift_stars, int(gift_stars * 0.85))


async def safe_edit(message: types.Message, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup, disable_web_page_preview=True)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            try:
                await message.answer(text, parse_mode="HTML", reply_markup=reply_markup, disable_web_page_preview=True)
            except Exception:
                pass


def verify_telegram_init_data(init_data: str) -> dict:
    if not init_data:
        raise HTTPException(status_code=401, detail="Откройте приложение внутри Telegram")
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            raise HTTPException(status_code=401, detail="Нет подписи Telegram")
        data_check = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, received_hash):
            raise HTTPException(status_code=401, detail="Невалидная подпись Telegram")
        return json.loads(parsed["user"])
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Ошибка авторизации Telegram")


# --- БАЗА ДАННЫХ И ТГ-БЭКАПЫ ---
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
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
            CREATE TABLE IF NOT EXISTS withdraw_requests (
                request_id TEXT PRIMARY KEY,
                user_tg_id INTEGER NOT NULL,
                gift_stars INTEGER NOT NULL,
                receive_stars INTEGER NOT NULL,
                cost_rp INTEGER NOT NULL,
                fee_rp INTEGER NOT NULL,
                target_note TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT
            );
            CREATE TABLE IF NOT EXISTS stars_payments (
                telegram_charge_id TEXT PRIMARY KEY,
                user_tg_id INTEGER NOT NULL,
                amount_stars INTEGER NOT NULL,
                created_at TEXT NOT NULL
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
        if "wallet_balance" not in table_cols(conn, "users"):
            conn.execute("ALTER TABLE users ADD COLUMN wallet_balance INTEGER DEFAULT 0")
        m_cols = table_cols(conn, "merchants")
        if "balance_rp" not in m_cols:
            conn.execute("ALTER TABLE merchants ADD COLUMN balance_rp INTEGER DEFAULT 0")
            if "balance_stars" in m_cols:
                conn.execute("UPDATE merchants SET balance_rp = COALESCE(balance_stars, 0)")
        o_cols = table_cols(conn, "orders")
        if "amount_rp" not in o_cols or "invoice_link" in o_cols:
            a = "amount_rp" if "amount_rp" in o_cols else "COALESCE(amount_stars, 0)"
            f = "fee_rp" if "fee_rp" in o_cols else "COALESCE(fee_stars, 0)"
            n = "net_rp" if "net_rp" in o_cols else "COALESCE(net_stars, 0)"
            conn.executescript(
                f"""
                CREATE TABLE orders_new (
                    order_id TEXT PRIMARY KEY, merchant_id TEXT NOT NULL, external_order_id TEXT,
                    title TEXT NOT NULL, description TEXT, amount_rp INTEGER NOT NULL,
                    fee_rp INTEGER NOT NULL, net_rp INTEGER NOT NULL, status TEXT DEFAULT 'pending',
                    payer_tg_id INTEGER, created_at TEXT NOT NULL, paid_at TEXT
                );
                INSERT INTO orders_new SELECT order_id, merchant_id, external_order_id, title, description,
                    {a}, {f}, {n}, status, payer_tg_id, created_at, paid_at FROM orders;
                DROP TABLE orders;
                ALTER TABLE orders_new RENAME TO orders;
                """
            )


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


def get_wallet_balance(tg_id: int) -> int:
    with get_db() as conn:
        row = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (tg_id,)).fetchone()
    return row["wallet_balance"] if row else 0


async def restore_db_from_telegram() -> None:
    if not DB_CHANNEL_ID:
        init_db()
        return
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
        if chat.pinned_message and chat.pinned_message.document:
            await bot.download(chat.pinned_message.document, destination=DB_PATH)
            init_db()
            print(f"[DB Sync] Восстановлена из канала (#{chat.pinned_message.message_id}).")
            return
    except Exception as e:
        print(f"[DB Sync] Первый запуск / ошибка: {e}")
    init_db()
    await sync_db_to_telegram("Инициализация БД RoboPay")


async def sync_db_to_telegram(reason: str = "Синхронизация") -> Optional[int]:
    if not DB_CHANNEL_ID:
        return None
    async with db_sync_lock:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        snap = f"backup_{ts}.db"
        try:
            with get_db() as src, sqlite3.connect(snap) as dst:
                src.backup(dst)
            msg = await bot.send_document(
                DB_CHANNEL_ID,
                FSInputFile(snap, filename="gateway.db"),
                caption=f"🗄 <b>RoboPay DB</b>\n• {esc(reason)}\n• UTC: <code>{ts}</code>",
                parse_mode="HTML",
            )
            await bot.pin_chat_message(DB_CHANNEL_ID, msg.message_id, disable_notification=True)
            return msg.message_id
        except Exception as e:
            print(f"[DB Sync Error] {e}")
            return None
        finally:
            if os.path.exists(snap):
                os.remove(snap)


# --- АВТОПОДБОР АКТИВНОЙ МОДЕЛИ GROQ ---
async def resolve_groq_model(client: httpx.AsyncClient, exclude: Optional[str] = None) -> Optional[str]:
    global CACHED_GROQ_MODEL
    try:
        r = await client.get(
            "https://api.groq.com/openai/v1/models",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
        )
        if r.status_code != 200:
            print(f"[Groq Models Error] {r.status_code} {r.text}")
            return None
        models = [m["id"] for m in r.json().get("data", []) if isinstance(m, dict) and m.get("id")]
        skip = ("whisper", "tts", "guard", "safeguard", "embed", "moderation", "playai", "orpheus", "speech", "audio")
        chat_models = [m for m in models if m != exclude and not any(k in m.lower() for k in skip)]
        preferred = [
            "llama-3.1-8b-instant",
            "meta-llama/llama-4-scout-17b-16e-instruct",
            "meta-llama/llama-4-maverick-17b-128e-instruct",
            "openai/gpt-oss-120b",
            "openai/gpt-oss-20b",
            "qwen/qwen3-32b",
            "llama-3.3-70b-versatile",
            "gemma2-9b-it",
        ]
        for p in preferred:
            if p in chat_models:
                CACHED_GROQ_MODEL = p
                print(f"[Groq Auto-Selected Model] {p}")
                return p
        if chat_models:
            CACHED_GROQ_MODEL = chat_models[0]
            print(f"[Groq Fallback Model] {chat_models[0]}")
            return chat_models[0]
    except Exception as e:
        print(f"[Groq Models Exception] {e}")
    return None


# --- БИЗНЕС-ЛОГИКА ---
def create_order_record(merchant_id: str, title: str, description: str, amount_rp: int, external_order_id: Optional[str] = None) -> dict:
    order_id = f"ord_{secrets.token_hex(6)}"
    fee_rp = int(amount_rp * COMMISSION_RATE)
    net_rp = max(1, amount_rp - fee_rp)
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO orders (order_id, merchant_id, external_order_id, title, description, amount_rp, fee_rp, net_rp, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (order_id, merchant_id, external_order_id, title[:48], description[:255], amount_rp, fee_rp, net_rp, utc_now()),
        )
    link = f"https://t.me/{BOT_USERNAME}?start=pay_{order_id}"
    return {
        "order_id": order_id, "status": "pending", "amount": amount_rp, "amount_rp": amount_rp,
        "fee_rp": fee_rp, "net_rp": net_rp, "checkout_deeplink": link,
        "webapp_url": f"{PUBLIC_URL}/?order_id={order_id}", "qr_payload": link,
    }


async def create_topup_request(user_tg_id: int, amount_rp: int, comment: str) -> dict:
    req_id = f"top_{secrets.token_hex(5)}"
    now = utc_now()
    with get_db() as conn:
        conn.execute(
            "INSERT INTO topup_requests (request_id, user_tg_id, amount_rp, comment, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
            (req_id, user_tg_id, amount_rp, comment[:200], now),
        )
        u = conn.execute("SELECT username, first_name FROM users WHERE tg_id = ?", (user_tg_id,)).fetchone()
    asyncio.create_task(sync_db_to_telegram(f"Пополнение {req_id} (+{amount_rp} RP)"))

    if ADMIN_ID:
        uname = f"@{u['username']}" if u and u["username"] else "без username"
        fname = esc(u["first_name"] if u else "Пользователь")
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=f"✅ Одобрить (+{amount_rp} RP)", callback_data=f"topup_ok:{req_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"topup_no:{req_id}"),
        ]])
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🎁 <b>Заявка на пополнение</b> <code>{req_id}</code>\n\n"
                f"• Клиент: <a href=\"tg://user?id={user_tg_id}\">{fname}</a> ({uname}, <code>{user_tg_id}</code>)\n"
                f"• Сумма подарка: <b>{amount_rp} ⭐ → {amount_rp} RP</b>\n"
                f"• Комментарий: <i>{esc(comment or '—')}</i>",
                parse_mode="HTML", reply_markup=kb,
            )
        except Exception as e:
            print(f"[Admin Topup Notify Error] {e}")
    return {"request_id": req_id, "amount_rp": amount_rp, "status": "pending", "created_at": now}


async def create_withdraw_request(user_tg_id: int, gift_stars: int, target_note: str = "") -> dict:
    if gift_stars not in GIFT_TIERS:
        raise HTTPException(status_code=400, detail="Доступные подарки: 15, 25, 50 или 100 ⭐")
    cost_rp, fee_rp, receive_stars = calc_withdraw_cost(gift_stars)
    req_id = f"wdr_{secrets.token_hex(5)}"
    now = utc_now()

    with get_db() as conn:
        cur = conn.execute(
            "UPDATE users SET wallet_balance = wallet_balance - ? WHERE tg_id = ? AND wallet_balance >= ?",
            (cost_rp, user_tg_id, cost_rp),
        )
        if cur.rowcount == 0:
            raise HTTPException(status_code=400, detail=f"Недостаточно средств (нужно {cost_rp} RP)")
        conn.execute(
            """
            INSERT INTO withdraw_requests (request_id, user_tg_id, gift_stars, receive_stars, cost_rp, fee_rp, target_note, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (req_id, user_tg_id, gift_stars, receive_stars, cost_rp, fee_rp, target_note[:120], now),
        )
        u = conn.execute("SELECT username, first_name, wallet_balance FROM users WHERE tg_id = ?", (user_tg_id,)).fetchone()

    asyncio.create_task(sync_db_to_telegram(f"Вывод {req_id} (подарок {gift_stars}⭐, -{cost_rp} RP)"))
    asyncio.create_task(rt_hub.push(user_tg_id, {
        "type": "sync", "wallet_balance": u["wallet_balance"],
        "notify": f"Заявка на вывод {gift_stars} Stars создана", "emotion": "happy",
    }))

    if ADMIN_ID:
        uname = f"@{u['username']}" if u and u["username"] else "без username"
        fname = esc(u["first_name"] if u else "Пользователь")
        rows_kb = []
        if u and u["username"]:
            rows_kb.append([InlineKeyboardButton(text=f"🎁 Открыть @{u['username']}", url=f"https://t.me/{u['username']}")])
        rows_kb.append([
            InlineKeyboardButton(text=f"✅ Одобрить ({gift_stars} ⭐)", callback_data=f"wdr_ok:{req_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wdr_no:{req_id}"),
        ])
        try:
            await bot.send_message(
                ADMIN_ID,
                f"📤 <b>Заявка на вывод подарком</b> <code>{req_id}</code>\n\n"
                f"• Клиент: <a href=\"tg://user?id={user_tg_id}\">{fname}</a> ({uname}, ID <code>{user_tg_id}</code>)\n"
                f"• Подарок: <b>{gift_stars} ⭐</b> (~{receive_stars} ⭐ при обмене)\n"
                f"• Списано: <b>{cost_rp} RP</b> (комиссия: {fee_rp} RP)\n"
                f"• Примечание: <i>{esc(target_note or '—')}</i>",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows_kb),
            )
        except Exception as e:
            print(f"[Admin Withdraw Notify Error] {e}")

    return {
        "request_id": req_id, "gift_stars": gift_stars, "receive_stars": receive_stars,
        "cost_rp": cost_rp, "fee_rp": fee_rp, "status": "pending", "wallet_balance": u["wallet_balance"],
    }


async def dispatch_partner_notification(order: dict, merchant: dict) -> None:
    try:
        await bot.send_message(
            merchant["owner_tg_id"],
            f"⚡ <b>Оплата получена — {esc(merchant['title'])}</b>\n\n"
            f"• Услуга: <b>{esc(order['title'])}</b>\n"
            f"• Сумма: <b>{order['amount_rp']} RP</b> (зачислено <b>+{order['net_rp']} RP</b>)\n"
            f"• Заказ: <code>{order['order_id']}</code> | Покупатель: <code>{order['payer_tg_id']}</code>",
            parse_mode="HTML",
        )
    except Exception:
        pass

    asyncio.create_task(rt_hub.push(merchant["owner_tg_id"], {
        "type": "sync",
        "wallet_balance": get_wallet_balance(merchant["owner_tg_id"]),
        "notify": f"Оплата +{order['net_rp']} RP ({order['title']})",
        "emotion": "glee",
    }))

    if merchant.get("webhook_url"):
        body = json.dumps({
            "event": "payment.succeeded", "order_id": order["order_id"],
            "external_order_id": order["external_order_id"], "amount_rp": order["amount_rp"],
            "net_rp": order["net_rp"], "payer_tg_id": order["payer_tg_id"], "paid_at": order["paid_at"],
        }, ensure_ascii=False).encode()
        sig = hmac.new(merchant["api_key"].encode(), body, hashlib.sha256).hexdigest()
        headers = {"Content-Type": "application/json", "X-RoboPay-Signature": sig, "X-RoboPay-Event": "payment.succeeded"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(3):
                try:
                    r = await client.post(merchant["webhook_url"], content=body, headers=headers)
                    if 200 <= r.status_code < 300:
                        break
                except Exception:
                    await asyncio.sleep(1.5 * (attempt + 1))


# --- ПРИВЕТСТВИЕ И МЕНЮ БОТА ---
def build_main_menu(user: types.User) -> tuple[str, InlineKeyboardMarkup]:
    ensure_user(user.id, user.username, user.first_name)
    with get_db() as conn:
        u = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (user.id,)).fetchone()
        ms = conn.execute("SELECT COUNT(*) AS cnt, COALESCE(SUM(balance_rp), 0) AS bal FROM merchants WHERE owner_tg_id = ?", (user.id,)).fetchone()
    w_bal = u["wallet_balance"] if u else 0

    text = (
        f"👋 <b>Привет, {esc(user.first_name)}! Это RoboPay.</b>\n\n"
        f"<b>Для чего нужен RoboPay:</b>\n"
        f"• <b>Мгновенные оплаты по QR и ссылкам</b> — платите за услуги и переводите друзьям во внутренней валюте <b>RP</b> (1 Star = 1 RP).\n"
        f"• <b>Пополнение и вывод подарками Telegram</b> — пополняйте баланс подарками до 100 ⭐ и выводите RP обратно в звёзды.\n"
        f"• <b>Платёжный шлюз для ваших проектов</b> — подключайте своего бота или канал и получайте мгновенные сигналы об оплате.\n"
        f"• <b>Живой ИИ-робот техподдержки</b> — отвечает голосом и текстом 24/7.\n\n"
        f"👤 ID: <code>{user.id}</code>  |  💳 Кошелёк: <b>{w_bal} RP</b>  |  📦 Проекты ({ms['cnt']}): <b>{ms['bal']} RP</b>"
    )

    btns = []
    if PUBLIC_URL.startswith("https://"):
        btns.append([InlineKeyboardButton(text="Открыть терминал RoboPay", web_app=WebAppInfo(url=f"{PUBLIC_URL}/"))])
    btns.append([
        InlineKeyboardButton(text="⭐ Пополнить Stars", callback_data="stars_topup"),
        InlineKeyboardButton(text="💳 Кошелёк / Вывод", callback_data="wallet"),
        InlineKeyboardButton(text="⚡ Выставить счёт", callback_data="quick_pay"),
    ])
    row2 = [InlineKeyboardButton(text="📦 Мои проекты", callback_data="projects")]
    if PUBLIC_URL.startswith("https://"):
        row2.append(InlineKeyboardButton(text="💬 Техподдержка", web_app=WebAppInfo(url=f"{PUBLIC_URL}/?page=support")))
    else:
        row2.append(InlineKeyboardButton(text="📖 API", callback_data="api_docs"))
    btns.append(row2)
    if PUBLIC_URL.startswith("https://"):
        btns.append([InlineKeyboardButton(text="📖 Документация API", callback_data="api_docs")])
    if is_admin(user.id):
        btns.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_panel")])
    return text, InlineKeyboardMarkup(inline_keyboard=btns)


def build_project_card(mid: str, owner_id: int) -> Optional[tuple[str, InlineKeyboardMarkup]]:
    with get_db() as conn:
        m = conn.execute("SELECT * FROM merchants WHERE merchant_id = ? AND owner_tg_id = ?", (mid, owner_id)).fetchone()
        if not m:
            return None
        paid = conn.execute("SELECT COUNT(*) AS c, COALESCE(SUM(amount_rp), 0) AS v FROM orders WHERE merchant_id = ? AND status = 'paid'", (mid,)).fetchone()
    wh = f"<code>{esc(m['webhook_url'])}</code>" if m["webhook_url"] else "<i>Только ТГ-сигналы</i>"
    text = (
        f"📦 <b>{esc(m['title'])}</b>\n\n"
        f"• Баланс проекта: <b>{m['balance_rp']} RP</b>\n"
        f"• Оплат: <b>{paid['c']}</b> (оборот {paid['v']} RP) | Комиссия: <b>{int(COMMISSION_RATE * 100)}%</b>\n\n"
        f"🆔 ID: <code>{m['merchant_id']}</code>\n"
        f"🔑 API Key: <tg-spoiler><code>{m['api_key']}</code></tg-spoiler>\n"
        f"🌐 Webhook: {wh}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ Создать счёт / QR", callback_data=f"qp_select:{mid}")],
        [InlineKeyboardButton(text="💸 Перевести на кошелёк", callback_data=f"withdraw_proj:{mid}")],
        [
            InlineKeyboardButton(text="🔗 Webhook", callback_data=f"edit_wh:{mid}"),
            InlineKeyboardButton(text="🔄 Сбросить ключ", callback_data=f"reset_key:{mid}"),
        ],
        [InlineKeyboardButton(text="◀️ К проектам", callback_data="projects")],
    ])
    return text, kb


@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext) -> None:
    await state.clear()
    ensure_user(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if command.args and command.args.startswith("pay_"):
        oid = command.args[4:]
        with get_db() as conn:
            o = conn.execute("SELECT o.*, m.title AS m_title FROM orders o JOIN merchants m ON o.merchant_id = m.merchant_id WHERE o.order_id = ?", (oid,)).fetchone()
        if not o:
            await message.answer("Счёт не найден или устарел.")
            return
        if o["status"] == "paid":
            await message.answer(f"✅ Счёт <b>«{esc(o['title'])}»</b> уже оплачен.", parse_mode="HTML")
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"Оплатить {o['amount_rp']} RP", web_app=WebAppInfo(url=f"{PUBLIC_URL}/?order_id={oid}"))]])
        await message.answer(f"<b>Счёт • {esc(o['m_title'])}</b>\n\n• Услуга: <b>{esc(o['title'])}</b>\n• К оплате: <b>{o['amount_rp']} RP</b>", parse_mode="HTML", reply_markup=kb)
        return
    if command.args and command.args.startswith("uid_"):
        target_uid = command.args[4:]
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=f"Перевести на ID {target_uid}", web_app=WebAppInfo(url=f"{PUBLIC_URL}/?to_uid={target_uid}"))]])
        await message.answer(f"💸 <b>Перевод в RoboPay</b>\nПолучатель: <code>{esc(target_uid)}</code>", parse_mode="HTML", reply_markup=kb)
        return
    text, kb = build_main_menu(message.from_user)
    await message.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)


@dp.message(Command("stars"))
async def cmd_stars(msg: types.Message) -> None:
    await send_stars_topup_menu(msg)


async def send_stars_topup_menu(target) -> None:
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ 10 Stars — 10 RP", callback_data="stars_buy:10")],
        [InlineKeyboardButton(text="⭐ 25 Stars — 25 RP", callback_data="stars_buy:25")],
        [InlineKeyboardButton(text="⭐ 50 Stars — 50 RP", callback_data="stars_buy:50")],
        [InlineKeyboardButton(text="⭐ 100 Stars — 100 RP", callback_data="stars_buy:100")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
    ])
    await target.answer("Выберите сумму пополнения. Оплата проходит внутри Telegram Stars, без внешнего кошелька.", reply_markup=kb)


@dp.callback_query(F.data == "stars_topup")
async def cb_stars_topup(cb: types.CallbackQuery) -> None:
    await send_stars_topup_menu(cb.message)
    await cb.answer()


@dp.callback_query(F.data.startswith("stars_buy:"))
async def cb_stars_buy(cb: types.CallbackQuery) -> None:
    amount = int(cb.data.split(":")[1])
    if amount not in (10, 25, 50, 100):
        await cb.answer("Недопустимая сумма", show_alert=True)
        return
    payload = f"rpstars:{cb.from_user.id}:{amount}:{secrets.token_hex(6)}"
    try:
        await bot.send_invoice(
            chat_id=cb.from_user.id,
            title=f"Пополнение RoboPay на {amount} RP",
            description=f"Зачисление {amount} RP на кошелёк RoboPay",
            payload=payload,
            currency="XTR",
            prices=[LabeledPrice(label=f"{amount} Telegram Stars", amount=amount)],
            provider_token="",
        )
        await cb.answer("Счёт отправлен в личный чат с ботом")
    except Exception as exc:
        print(f"[Stars Invoice Error] {exc}")
        await cb.answer("Не удалось создать счёт. Попробуйте позже.", show_alert=True)


@dp.pre_checkout_query()
async def stars_pre_checkout(query: types.PreCheckoutQuery) -> None:
    try:
        parts = query.invoice_payload.split(":")
        valid = len(parts) == 4 and parts[0] == "rpstars" and int(parts[1]) == query.from_user.id and int(parts[2]) in (10, 25, 50, 100)
        valid = valid and query.currency == "XTR" and query.total_amount == int(parts[2])
    except Exception:
        valid = False
    await query.answer(ok=valid, error_message=None if valid else "Счёт RoboPay недействителен. Создайте новый.")


@dp.message(F.successful_payment)
async def stars_payment_success(msg: types.Message) -> None:
    sp = msg.successful_payment
    try:
        parts = sp.invoice_payload.split(":")
        amount = int(parts[2])
        if len(parts) != 4 or parts[0] != "rpstars" or int(parts[1]) != msg.from_user.id or amount not in (10, 25, 50, 100) or sp.currency != "XTR" or sp.total_amount != amount:
            await msg.answer("Платёж получен, но счёт не прошёл проверку. Обратитесь в поддержку RoboPay.")
            return
    except Exception:
        await msg.answer("Платёж получен, но данные счёта не удалось проверить. Обратитесь в поддержку RoboPay.")
        return
    ensure_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)
    with get_db() as conn:
        inserted = conn.execute(
            "INSERT OR IGNORE INTO stars_payments (telegram_charge_id, user_tg_id, amount_stars, created_at) VALUES (?, ?, ?, ?)",
            (sp.telegram_payment_charge_id, msg.from_user.id, amount, utc_now())
        ).rowcount
        if inserted:
            conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (amount, msg.from_user.id))
        bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (msg.from_user.id,)).fetchone()["wallet_balance"]
    if inserted:
        asyncio.create_task(sync_db_to_telegram(f"Пополнение Stars {amount} RP"))
        await msg.answer(f"✅ Оплата прошла! Зачислено <b>+{amount} RP</b>. Баланс: <b>{bal} RP</b>.", parse_mode="HTML")
        asyncio.create_task(rt_hub.push(msg.from_user.id, {"type":"sync","wallet_balance":bal,"notify":f"+{amount} RP за Telegram Stars","emotion":"glee"}))
    else:
        await msg.answer(f"Этот платёж уже был учтён. Баланс: {bal} RP.")


@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(cb: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    text, kb = build_main_menu(cb.from_user)
    await safe_edit(cb.message, text, kb)
    await cb.answer()


# --- КОШЕЛЁК, ПОПОЛНЕНИЕ И ВЫВОД ---
@dp.callback_query(F.data == "wallet")
async def cb_wallet(cb: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    ensure_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    bal = get_wallet_balance(cb.from_user.id)
    text = (
        f"💳 <b>Кошелёк RoboPay</b>\n\n"
        f"• Баланс: <b>{bal} RP</b>\n"
        f"• Пополнение подарком до <b>{MAX_TOPUP_STARS} ⭐</b> на <a href=\"{OWNER_URL}\">{esc(OWNER_DISPLAY)}</a>\n"
        f"• Вывод подарком с комиссией <b>{int(WITHDRAW_FEE_RATE * 100)}%</b> (15, 25, 50, 100 ⭐)"
    )
    btns = [
        [InlineKeyboardButton(text=f"🎁 Отправить подарок ({OWNER_DISPLAY})", url=OWNER_URL)],
        [
            InlineKeyboardButton(text="➕ Пополнить", callback_data="topup_start"),
            InlineKeyboardButton(text="📤 Вывести в ⭐", callback_data="withdraw_menu"),
        ],
    ]
    if PUBLIC_URL.startswith("https://"):
        btns.append([InlineKeyboardButton(text="Открыть кошелёк в Mini App", web_app=WebAppInfo(url=f"{PUBLIC_URL}/?page=wallet"))])
    btns.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])
    await safe_edit(cb.message, text, InlineKeyboardMarkup(inline_keyboard=btns))
    await cb.answer()


@dp.callback_query(F.data == "topup_start")
async def cb_topup_start(cb: types.CallbackQuery, state: FSMContext) -> None:
    await state.set_state(TopUpFSM.waiting_amount)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎁 Перейти к {OWNER_DISPLAY}", url=OWNER_URL)],
        [InlineKeyboardButton(text=f"{x} ⭐", callback_data=f"top_amt:{x}") for x in (15, 25, 50, 100)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="wallet")],
    ])
    await safe_edit(cb.message, f"🎁 <b>Пополнение подарком (до {MAX_TOPUP_STARS} ⭐)</b>\n\nОтправьте подарок на <a href=\"{OWNER_URL}\">{OWNER_URL}</a>, затем выберите номинал:", kb)
    await cb.answer()


async def ask_topup_comment(target: types.Message, state: FSMContext, amount: int, is_edit: bool) -> None:
    await state.update_data(amount_rp=amount)
    await state.set_state(TopUpFSM.waiting_comment)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Без комментария", callback_data="topup_no_comment")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="wallet")],
    ])
    text = f"🎁 Сумма: <b>{amount} ⭐ → {amount} RP</b>.\nНапишите какой подарок отправили или нажмите «Без комментария»:"
    if is_edit:
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.callback_query(TopUpFSM.waiting_amount, F.data.startswith("top_amt:"))
async def fsm_topup_btn(cb: types.CallbackQuery, state: FSMContext) -> None:
    await ask_topup_comment(cb.message, state, int(cb.data.split(":")[1]), True)
    await cb.answer()


@dp.message(TopUpFSM.waiting_amount)
async def fsm_topup_txt(msg: types.Message, state: FSMContext) -> None:
    raw = (msg.text or "").strip()
    if not raw.isdigit() or not (1 <= int(raw) <= MAX_TOPUP_STARS):
        await msg.answer(f"Введите сумму подарка от 1 до {MAX_TOPUP_STARS} ⭐.")
        return
    await ask_topup_comment(msg, state, int(raw), False)


@dp.callback_query(TopUpFSM.waiting_comment, F.data == "topup_no_comment")
async def fsm_topup_empty(cb: types.CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    req = await create_topup_request(cb.from_user.id, data["amount_rp"], "")
    await safe_edit(cb.message, f"⏳ Заявка <code>{req['request_id']}</code> на <b>{req['amount_rp']} RP</b> отправлена владельцу.", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ В кошелёк", callback_data="wallet")]]))
    await cb.answer("Заявка отправлена")


@dp.message(TopUpFSM.waiting_comment)
async def fsm_topup_comm(msg: types.Message, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    req = await create_topup_request(msg.from_user.id, data["amount_rp"], (msg.text or "").strip())
    await msg.answer(f"⏳ Заявка <code>{req['request_id']}</code> на <b>{req['amount_rp']} RP</b> отправлена владельцу.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ В кошелёк", callback_data="wallet")]]))


@dp.callback_query(F.data == "withdraw_menu")
async def cb_withdraw_menu(cb: types.CallbackQuery) -> None:
    ensure_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    bal = get_wallet_balance(cb.from_user.id)
    lines = [
        f"📤 <b>Вывод RP подарком Telegram</b>\n",
        f"• Ваш баланс: <b>{bal} RP</b>",
        f"• Комиссия вывода: <b>{int(WITHDRAW_FEE_RATE * 100)}%</b>\n",
        "Выберите подарок (рядом цена в RP и сколько ⭐ вы получите при обмене):",
    ]
    btns = []
    for g in GIFT_TIERS:
        cost, _, rec = calc_withdraw_cost(g)
        btns.append([InlineKeyboardButton(text=f"🎁 Подарок {g} ⭐ (~{rec} ⭐) — {cost} RP", callback_data=f"wdr_req:{g}")])
    btns.append([InlineKeyboardButton(text="◀️ В кошелёк", callback_data="wallet")])
    await safe_edit(cb.message, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=btns))
    await cb.answer()


@dp.callback_query(F.data.startswith("wdr_req:"))
async def cb_withdraw_create(cb: types.CallbackQuery) -> None:
    ensure_user(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    try:
        res = await create_withdraw_request(cb.from_user.id, int(cb.data.split(":")[1]))
    except HTTPException as e:
        await cb.answer(e.detail, show_alert=True)
        return
    await safe_edit(
        cb.message,
        f"⏳ <b>Заявка на вывод <code>{res['request_id']}</code> отправлена!</b>\n\n"
        f"• Подарок: <b>{res['gift_stars']} ⭐</b> (~{res['receive_stars']} ⭐ при обмене)\n"
        f"• Списано: <b>{res['cost_rp']} RP</b> (остаток: {res['wallet_balance']} RP)",
        InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ В кошелёк", callback_data="wallet")]]),
    )
    await cb.answer("Заявка создана")


# --- ПРОЕКТЫ И БЫСТРЫЙ СЧЁТ ---
@dp.callback_query(F.data == "projects")
async def cb_projects(cb: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    with get_db() as conn:
        rows = conn.execute("SELECT merchant_id, title, balance_rp FROM merchants WHERE owner_tg_id = ? ORDER BY created_at DESC", (cb.from_user.id,)).fetchall()
    btns = [[InlineKeyboardButton(text=f"📦 {r['title']} ({r['balance_rp']} RP)", callback_data=f"proj:{r['merchant_id']}")] for r in rows]
    btns.append([InlineKeyboardButton(text="➕ Новый проект", callback_data="new_project")])
    btns.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])
    await safe_edit(cb.message, "📦 <b>Ваши проекты</b>" if rows else "📦 <b>Проектов пока нет</b>", InlineKeyboardMarkup(inline_keyboard=btns))
    await cb.answer()


@dp.callback_query(F.data.startswith("proj:"))
async def cb_proj_detail(cb: types.CallbackQuery) -> None:
    card = build_project_card(cb.data.split(":")[1], cb.from_user.id)
    if not card:
        await cb.answer("Проект не найден", show_alert=True)
        return
    await safe_edit(cb.message, card[0], card[1])
    await cb.answer()


@dp.callback_query(F.data.startswith("withdraw_proj:"))
async def cb_withdraw_proj(cb: types.CallbackQuery) -> None:
    mid, uid = cb.data.split(":")[1], cb.from_user.id
    with get_db() as conn:
        r = conn.execute("SELECT balance_rp FROM merchants WHERE merchant_id = ? AND owner_tg_id = ?", (mid, uid)).fetchone()
        if not r or r["balance_rp"] <= 0:
            await cb.answer("На проекте 0 RP", show_alert=True)
            return
        amt = r["balance_rp"]
        conn.execute("UPDATE merchants SET balance_rp = 0 WHERE merchant_id = ? AND balance_rp = ?", (mid, amt))
        conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (amt, uid))
        new_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (uid,)).fetchone()["wallet_balance"]
    asyncio.create_task(sync_db_to_telegram(f"Вывод {amt} RP с проекта {mid}"))
    asyncio.create_task(rt_hub.push(uid, {"type": "sync", "wallet_balance": new_bal, "notify": f"+{amt} RP с проекта на кошелёк", "emotion": "glee"}))
    card = build_project_card(mid, uid)
    await safe_edit(cb.message, card[0], card[1])
    await cb.answer(f"+{amt} RP на кошелёк!", show_alert=True)


@dp.callback_query(F.data == "new_project")
async def cb_new_proj(cb: types.CallbackQuery, state: FSMContext) -> None:
    await state.set_state(CreateProjectFSM.waiting_title)
    await safe_edit(cb.message, "🆕 <b>Новый проект (1/2)</b>\nОтправьте название:", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="projects")]]))
    await cb.answer()


@dp.message(CreateProjectFSM.waiting_title)
async def fsm_proj_title(msg: types.Message, state: FSMContext) -> None:
    title = (msg.text or "").strip()[:40]
    if not title:
        return
    await state.update_data(title=title)
    await state.set_state(CreateProjectFSM.waiting_webhook)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏭ Пропустить (только ТГ-сигналы)", callback_data="skip_webhook")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="projects")],
    ])
    await msg.answer(f"🆕 <b>«{esc(title)}» (2/2)</b>\nОтправьте HTTPS Webhook или нажмите «Пропустить»:", parse_mode="HTML", reply_markup=kb)


async def save_project(owner_id: int, title: str, wh: Optional[str]) -> str:
    mid, key = f"mrc_{secrets.token_hex(5)}", f"rp_live_{secrets.token_hex(16)}"
    with get_db() as conn:
        conn.execute("INSERT INTO merchants (merchant_id, owner_tg_id, title, api_key, webhook_url, created_at) VALUES (?, ?, ?, ?, ?, ?)", (mid, owner_id, title, key, wh, utc_now()))
    asyncio.create_task(sync_db_to_telegram(f"Проект {title} ({mid})"))
    return mid


@dp.callback_query(CreateProjectFSM.waiting_webhook, F.data == "skip_webhook")
async def fsm_proj_skip(cb: types.CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    mid = await save_project(cb.from_user.id, data["title"], None)
    card = build_project_card(mid, cb.from_user.id)
    await safe_edit(cb.message, f"✅ Проект создан!\n\n{card[0]}", card[1])
    await cb.answer()


@dp.message(CreateProjectFSM.waiting_webhook)
async def fsm_proj_wh(msg: types.Message, state: FSMContext) -> None:
    url = (msg.text or "").strip()
    if not url.startswith(("https://", "http://")):
        await msg.answer("URL должен начинаться с https://")
        return
    data = await state.get_data()
    await state.clear()
    mid = await save_project(msg.from_user.id, data["title"], url)
    card = build_project_card(mid, msg.from_user.id)
    await msg.answer(f"✅ Проект создан!\n\n{card[0]}", parse_mode="HTML", reply_markup=card[1])


@dp.callback_query(F.data.startswith("edit_wh:"))
async def cb_edit_wh(cb: types.CallbackQuery, state: FSMContext) -> None:
    mid = cb.data.split(":")[1]
    await state.set_state(EditWebhookFSM.waiting_url)
    await state.update_data(merchant_id=mid)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Отключить Webhook", callback_data=f"clear_wh:{mid}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data=f"proj:{mid}")],
    ])
    await safe_edit(cb.message, "🔗 Отправьте новый HTTPS-адрес Webhook:", kb)
    await cb.answer()


@dp.callback_query(F.data.startswith("clear_wh:"))
async def cb_clear_wh(cb: types.CallbackQuery, state: FSMContext) -> None:
    mid = cb.data.split(":")[1]
    await state.clear()
    with get_db() as conn:
        conn.execute("UPDATE merchants SET webhook_url = NULL WHERE merchant_id = ? AND owner_tg_id = ?", (mid, cb.from_user.id))
    card = build_project_card(mid, cb.from_user.id)
    await safe_edit(cb.message, card[0], card[1])
    await cb.answer("Отключено")


@dp.message(EditWebhookFSM.waiting_url)
async def fsm_save_wh(msg: types.Message, state: FSMContext) -> None:
    url = (msg.text or "").strip()
    if not url.startswith(("https://", "http://")):
        await msg.answer("URL должен начинаться с https://")
        return
    mid = (await state.get_data())["merchant_id"]
    await state.clear()
    with get_db() as conn:
        conn.execute("UPDATE merchants SET webhook_url = ? WHERE merchant_id = ? AND owner_tg_id = ?", (url, mid, msg.from_user.id))
    card = build_project_card(mid, msg.from_user.id)
    await msg.answer(card[0], parse_mode="HTML", reply_markup=card[1])


@dp.callback_query(F.data.startswith("reset_key:"))
async def cb_reset_key(cb: types.CallbackQuery) -> None:
    mid = cb.data.split(":")[1]
    with get_db() as conn:
        conn.execute("UPDATE merchants SET api_key = ? WHERE merchant_id = ? AND owner_tg_id = ?", (f"rp_live_{secrets.token_hex(16)}", mid, cb.from_user.id))
    card = build_project_card(mid, cb.from_user.id)
    await safe_edit(cb.message, card[0], card[1])
    await cb.answer("🔑 Ключ обновлён", show_alert=True)


@dp.callback_query(F.data == "quick_pay")
async def cb_quick_pay(cb: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    with get_db() as conn:
        rows = conn.execute("SELECT merchant_id, title FROM merchants WHERE owner_tg_id = ?", (cb.from_user.id,)).fetchall()
    if not rows:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➕ Создать проект", callback_data="new_project")], [InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu")]])
        await safe_edit(cb.message, "⚡ Сначала создайте проект.", kb)
        await cb.answer()
        return
    if len(rows) == 1:
        await start_qi(cb, state, rows[0]["merchant_id"])
        return
    btns = [[InlineKeyboardButton(text=f"📦 {r['title']}", callback_data=f"qp_select:{r['merchant_id']}")] for r in rows]
    btns.append([InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu")])
    await safe_edit(cb.message, "⚡ Выберите проект:", InlineKeyboardMarkup(inline_keyboard=btns))
    await cb.answer()


@dp.callback_query(F.data.startswith("qp_select:"))
async def cb_qp_sel(cb: types.CallbackQuery, state: FSMContext) -> None:
    await start_qi(cb, state, cb.data.split(":")[1])


async def start_qi(cb: types.CallbackQuery, state: FSMContext, mid: str) -> None:
    await state.set_state(QuickInvoiceFSM.waiting_title)
    await state.update_data(merchant_id=mid)
    await safe_edit(cb.message, "⚡ <b>Новый счёт (1/2)</b>\nВведите название услуги:", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")]]))
    await cb.answer()


@dp.message(QuickInvoiceFSM.waiting_title)
async def fsm_qi_title(msg: types.Message, state: FSMContext) -> None:
    t = (msg.text or "").strip()[:48]
    if not t:
        return
    await state.update_data(title=t)
    await state.set_state(QuickInvoiceFSM.waiting_amount)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{x} RP", callback_data=f"qp_amt:{x}") for x in (25, 50, 100, 250)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")],
    ])
    await msg.answer(f"⚡ <b>«{esc(t)}» (2/2)</b>\nВыберите сумму RP или введите вручную:", parse_mode="HTML", reply_markup=kb)


async def finish_qi(target: types.Message, mid: str, title: str, amt: int, edit: bool) -> None:
    o = create_order_record(mid, title, "Оплата через RoboPay", amt)
    asyncio.create_task(sync_db_to_telegram(f"Счёт {o['order_id']} ({amt} RP)"))
    text = f"✅ <b>Счёт создан</b>\n\n• {esc(title)} — <b>{amt} RP</b>\n• Ссылка: <code>{o['checkout_deeplink']}</code>"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Открыть чекаут / QR", web_app=WebAppInfo(url=o["webapp_url"]))],
        [InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu")],
    ])
    if edit:
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)


@dp.callback_query(QuickInvoiceFSM.waiting_amount, F.data.startswith("qp_amt:"))
async def fsm_qi_btn(cb: types.CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    await finish_qi(cb.message, data["merchant_id"], data["title"], int(cb.data.split(":")[1]), True)
    await cb.answer()


@dp.message(QuickInvoiceFSM.waiting_amount)
async def fsm_qi_txt(msg: types.Message, state: FSMContext) -> None:
    raw = (msg.text or "").strip()
    if not raw.isdigit() or not (1 <= int(raw) <= 500000):
        await msg.answer("Введите число от 1 до 500000.")
        return
    data = await state.get_data()
    await state.clear()
    await finish_qi(msg, data["merchant_id"], data["title"], int(raw), False)


@dp.callback_query(F.data == "api_docs")
async def cb_api_docs(cb: types.CallbackQuery) -> None:
    text = (
        f"📖 <b>RoboPay API</b>\n\n"
        f"<code>POST {PUBLIC_URL}/api/v1/invoices</code>\n"
        f"Заголовок: <code>X-API-Key: ваш_ключ</code>\n"
        f'<pre>{{"title":"Услуга","amount":50,"external_order_id":"u_1"}}</pre>'
        f"В ответе вернётся <code>checkout_deeplink</code>. При оплате придёт сигнал в ТГ и HMAC-SHA256 Webhook."
    )
    await safe_edit(cb.message, text, InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu")]]))
    await cb.answer()


# --- АДМИН-ПАНЕЛЬ И РАССЫЛКА (СТРОГО ADMIN_ID) ---
def build_admin_panel() -> tuple[str, InlineKeyboardMarkup]:
    with get_db() as conn:
        u_cnt = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        w_sum = conn.execute("SELECT COALESCE(SUM(wallet_balance), 0) FROM users").fetchone()[0]
        p_top = conn.execute("SELECT COUNT(*) FROM topup_requests WHERE status = 'pending'").fetchone()[0]
        p_wdr = conn.execute("SELECT COUNT(*) FROM withdraw_requests WHERE status = 'pending'").fetchone()[0]
        f1 = conn.execute("SELECT COALESCE(SUM(fee_rp), 0) FROM orders WHERE status = 'paid'").fetchone()[0]
        f2 = conn.execute("SELECT COALESCE(SUM(fee_rp), 0) FROM transfers").fetchone()[0]
        f3 = conn.execute("SELECT COALESCE(SUM(fee_rp), 0) FROM withdraw_requests WHERE status = 'approved'").fetchone()[0]
    text = (
        f"🛠 <b>Админ-панель RoboPay</b>\n\n"
        f"• Пользователей: <b>{u_cnt}</b> (на кошельках: {w_sum} RP)\n"
        f"• Пополнений на проверке: <b>{p_top}</b>\n"
        f"• Выводов на проверке: <b>{p_wdr}</b>\n"
        f"• Комиссии шлюза: <b>{f1 + f2 + f3} RP</b>\n"
        f"• Приём подарков: <a href=\"{OWNER_URL}\">{esc(OWNER_DISPLAY)}</a>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📥 Пополнения ({p_top})", callback_data="admin_topups"), InlineKeyboardButton(text=f"📤 Выводы ({p_wdr})", callback_data="admin_withdraws")],
        [InlineKeyboardButton(text="📢 Сделать рассылку", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="💾 Бэкап БД в канал", callback_data="admin_backup")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_panel"), InlineKeyboardButton(text="◀️ Меню", callback_data="main_menu")],
    ])
    return text, kb


@dp.callback_query(F.data == "admin_panel")
async def cb_admin(cb: types.CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await state.clear()
    t, kb = build_admin_panel()
    await safe_edit(cb.message, t, kb)
    await cb.answer()


# --- РАССЫЛКА (КОПИРОВАНИЕ И ПЕРЕСЫЛКА ЛЮБЫХ ПОСТОВ/МЕДИА/СТИКЕРОВ/ЭМОДЗИ) ---
@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: types.CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    await state.set_state(BroadcastFSM.waiting_message)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")]])
    await safe_edit(
        cb.message,
        "📢 <b>Рассылка пользователям RoboPay</b>\n\n"
        "Отправьте сюда или <b>перешлите пост из вашего ТГ-канала</b> — поддерживаются текст, фото, видео, GIF, кружки, голосовые, стикеры, документы и премиум-эмодзи.\n\n"
        "После этого вы выберете: переслать с шапкой канала или отправить от имени бота.",
        kb,
    )
    await cb.answer()


@dp.message(BroadcastFSM.waiting_message)
async def fsm_broadcast_receive(msg: types.Message, state: FSMContext) -> None:
    if not is_admin(msg.from_user.id):
        return
    with get_db() as conn:
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    await state.update_data(from_chat_id=msg.chat.id, message_id=msg.message_id)
    await state.set_state(BroadcastFSM.waiting_confirm)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📤 Отправить копией от бота ({total_users} чел.)", callback_data="bc_run:copy")],
        [InlineKeyboardButton(text=f"↪️ Переслать с шапкой канала ({total_users} чел.)", callback_data="bc_run:forward")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_panel")],
    ])
    await msg.answer(
        f"📢 <b>Пост готов к рассылке!</b>\n\n"
        f"• Получателей в базе: <b>{total_users}</b>\n"
        f"• <b>Копией:</b> сохранит фото, видео, стикеры, премиум-эмодзи и кнопки без пометки «Переслано».\n"
        f"• <b>Переслать:</b> сохранит шапку и кликабельную ссылку на ваш ТГ-канал.\n\n"
        f"Выберите способ отправки:",
        parse_mode="HTML",
        reply_markup=kb,
    )


@dp.callback_query(BroadcastFSM.waiting_confirm, F.data.startswith("bc_run:"))
async def cb_broadcast_run(cb: types.CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    mode = cb.data.split(":")[1]
    data = await state.get_data()
    await state.clear()

    from_chat_id = data["from_chat_id"]
    message_id = data["message_id"]

    with get_db() as conn:
        user_ids = [r["tg_id"] for r in conn.execute("SELECT tg_id FROM users").fetchall()]

    await safe_edit(cb.message, f"⏳ <b>Рассылка запущена...</b> (0 / {len(user_ids)})", InlineKeyboardMarkup(inline_keyboard=[]))
    await cb.answer("Рассылка стартовала")

    sent, failed = 0, 0
    for uid in user_ids:
        try:
            if mode == "forward":
                await bot.forward_message(chat_id=uid, from_chat_id=from_chat_id, message_id=message_id)
            else:
                await bot.copy_message(chat_id=uid, from_chat_id=from_chat_id, message_id=message_id)
            sent += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 0.5)
            try:
                if mode == "forward":
                    await bot.forward_message(chat_id=uid, from_chat_id=from_chat_id, message_id=message_id)
                else:
                    await bot.copy_message(chat_id=uid, from_chat_id=from_chat_id, message_id=message_id)
                sent += 1
            except Exception:
                failed += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛠 В админ-панель", callback_data="admin_panel")]])
    await safe_edit(
        cb.message,
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"• Режим: <b>{'Пересылка с каналом' if mode == 'forward' else 'Копия от бота'}</b>\n"
        f"• Доставлено: <b>{sent}</b>\n"
        f"• Не доставлено (заблокировали бота): <b>{failed}</b>",
        kb,
    )


@dp.callback_query(F.data == "admin_topups")
async def cb_admin_topups(cb: types.CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM topup_requests WHERE status = 'pending'").fetchone()[0]
        r = conn.execute("SELECT r.*, u.username, u.first_name FROM topup_requests r LEFT JOIN users u ON r.user_tg_id = u.tg_id WHERE r.status = 'pending' ORDER BY r.created_at ASC LIMIT 1").fetchone()
    if not r:
        await cb.answer("Нет ожидающих заявок на пополнение", show_alert=True)
        return
    uname = f"@{r['username']}" if r["username"] else "без username"
    fname = esc(r["first_name"] or "Пользователь")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Одобрить (+{r['amount_rp']} RP)", callback_data=f"topup_ok:{r['request_id']}"), InlineKeyboardButton(text="❌ Отклонить", callback_data=f"topup_no:{r['request_id']}")],
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin_panel")],
    ])
    await safe_edit(
        cb.message,
        f"📥 <b>Пополнение (1 из {total}) • <code>{r['request_id']}</code></b>\n\n"
        f"• Клиент: <a href=\"tg://user?id={r['user_tg_id']}\">{fname}</a> ({uname}, <code>{r['user_tg_id']}</code>)\n"
        f"• Сумма: <b>{r['amount_rp']} ⭐ → {r['amount_rp']} RP</b>\n"
        f"• Комментарий: <i>{esc(r['comment'] or '—')}</i>",
        kb,
    )
    await cb.answer()


@dp.callback_query(F.data == "admin_withdraws")
async def cb_admin_withdraws(cb: types.CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM withdraw_requests WHERE status = 'pending'").fetchone()[0]
        r = conn.execute("SELECT r.*, u.username, u.first_name FROM withdraw_requests r LEFT JOIN users u ON r.user_tg_id = u.tg_id WHERE r.status = 'pending' ORDER BY r.created_at ASC LIMIT 1").fetchone()
    if not r:
        await cb.answer("Нет ожидающих заявок на вывод", show_alert=True)
        return
    uname = f"@{r['username']}" if r["username"] else "без username"
    fname = esc(r["first_name"] or "Пользователь")
    rows_kb = []
    if r["username"]:
        rows_kb.append([InlineKeyboardButton(text=f"🎁 Открыть @{r['username']}", url=f"https://t.me/{r['username']}")])
    rows_kb.append([
        InlineKeyboardButton(text=f"✅ Одобрить ({r['gift_stars']} ⭐)", callback_data=f"wdr_ok:{r['request_id']}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wdr_no:{r['request_id']}"),
    ])
    rows_kb.append([InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin_panel")])
    await safe_edit(
        cb.message,
        f"📤 <b>Вывод (1 из {total}) • <code>{r['request_id']}</code></b>\n\n"
        f"• Клиент: <a href=\"tg://user?id={r['user_tg_id']}\">{fname}</a> ({uname}, <code>{r['user_tg_id']}</code>)\n"
        f"• Отправить подарок: <b>{r['gift_stars']} ⭐</b> (~{r['receive_stars']} ⭐ при обмене)\n"
        f"• Списано: <b>{r['cost_rp']} RP</b> (комиссия {r['fee_rp']} RP)\n"
        f"• Примечание: <i>{esc(r['target_note'] or '—')}</i>",
        InlineKeyboardMarkup(inline_keyboard=rows_kb),
    )
    await cb.answer()


@dp.callback_query(F.data.startswith(("topup_ok:", "topup_no:")))
async def cb_review_topup(cb: types.CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    act, rid = cb.data.split(":", 1)
    ok = act == "topup_ok"
    st = "approved" if ok else "rejected"
    with get_db() as conn:
        if conn.execute("UPDATE topup_requests SET status = ?, reviewed_at = ? WHERE request_id = ? AND status = 'pending'", (st, utc_now(), rid)).rowcount == 0:
            await cb.answer("Уже обработана", show_alert=True)
            return
        r = conn.execute("SELECT * FROM topup_requests WHERE request_id = ?", (rid,)).fetchone()
        if ok:
            conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (r["amount_rp"], r["user_tg_id"]))
        new_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (r["user_tg_id"],)).fetchone()["wallet_balance"]

    asyncio.create_task(sync_db_to_telegram(f"Пополнение {rid}: {st}"))
    asyncio.create_task(rt_hub.push(r["user_tg_id"], {
        "type": "sync", "wallet_balance": new_bal,
        "notify": f"+{r['amount_rp']} RP зачислено! Успешно" if ok else f"Заявка {rid} отклонена",
        "emotion": "glee" if ok else "sadDown",
    }))
    await safe_edit(cb.message, f"{'✅ ОДОБРЕНО' if ok else '❌ ОТКЛОНЕНО'} • <code>{rid}</code> ({r['amount_rp']} RP)", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📥 Следующая заявка", callback_data="admin_topups")], [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_panel")]]))
    await cb.answer("Готово")
    try:
        msg = f"✅ <b>Пополнение одобрено!</b> Зачислено <b>+{r['amount_rp']} RP</b>." if ok else f"❌ Заявка <code>{rid}</code> на {r['amount_rp']} RP отклонена."
        await bot.send_message(r["user_tg_id"], msg, parse_mode="HTML")
    except Exception:
        pass


@dp.callback_query(F.data.startswith(("wdr_ok:", "wdr_no:")))
async def cb_review_withdraw(cb: types.CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        await cb.answer("⛔ Нет доступа", show_alert=True)
        return
    act, rid = cb.data.split(":", 1)
    ok = act == "wdr_ok"
    st = "approved" if ok else "rejected"
    with get_db() as conn:
        if conn.execute("UPDATE withdraw_requests SET status = ?, reviewed_at = ? WHERE request_id = ? AND status = 'pending'", (st, utc_now(), rid)).rowcount == 0:
            await cb.answer("Уже обработана", show_alert=True)
            return
        r = conn.execute("SELECT * FROM withdraw_requests WHERE request_id = ?", (rid,)).fetchone()
        if not ok:
            conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (r["cost_rp"], r["user_tg_id"]))
        new_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (r["user_tg_id"],)).fetchone()["wallet_balance"]

    asyncio.create_task(sync_db_to_telegram(f"Вывод {rid}: {st}"))
    asyncio.create_task(rt_hub.push(r["user_tg_id"], {
        "type": "sync", "wallet_balance": new_bal,
        "notify": f"Готово! Подарок {r['gift_stars']} Stars отправлен" if ok else f"Вывод отклонён (+{r['cost_rp']} RP возврат)",
        "emotion": "glee" if ok else "sadDown",
    }))
    await safe_edit(cb.message, f"{'✅ ВЫВОД ОДОБРЕН' if ok else '❌ ВЫВОД ОТКЛОНЁН (RP возвращены)'} • <code>{rid}</code> ({r['gift_stars']} ⭐)", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📤 Следующая заявка", callback_data="admin_withdraws")], [InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin_panel")]]))
    await cb.answer("Готово")
    try:
        if ok:
            await bot.send_message(r["user_tg_id"], f"✅ <b>Готово! Звёзды вам отправлены.</b>\n\nПодарок <b>{r['gift_stars']} ⭐</b> (~{r['receive_stars']} ⭐ при обмене) отправлен на ваш аккаунт (заявка <code>{rid}</code>).", parse_mode="HTML")
        else:
            await bot.send_message(r["user_tg_id"], f"❌ <b>Заявка на вывод <code>{rid}</code> отклонена.</b>\n<b>+{r['cost_rp']} RP</b> возвращены на ваш кошелёк.", parse_mode="HTML")
    except Exception:
        pass


@dp.callback_query(F.data == "admin_backup")
async def cb_admin_backup(cb: types.CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    mid = await sync_db_to_telegram("Ручной бэкап")
    await cb.answer(f"✅ Бэкап #{mid}" if mid else "⚠️ Ошибка", show_alert=True)


@dp.message(Command("backup"))
async def cmd_backup(msg: types.Message) -> None:
    if not is_admin(msg.from_user.id):
        return
    mid = await sync_db_to_telegram("Ручной /backup")
    await msg.answer(f"✅ Бэкап закреплён (#{mid})." if mid else "⚠️ Ошибка бэкапа.")


async def run_bot_polling() -> None:
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, handle_signals=False)
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[Polling Error] {e} — рестарт через 3с")
            await asyncio.sleep(3)


# --- FASTAPI ---
class CreateInvoiceReq(BaseModel):
    title: str = Field(..., min_length=1, max_length=48)
    description: str = Field(default="Оплата через RoboPay", max_length=255)
    amount: Optional[int] = Field(default=None, ge=1, le=500000)
    amount_stars: Optional[int] = Field(default=None, ge=1, le=500000)
    external_order_id: Optional[str] = Field(default=None, max_length=128)


class WebAppInvoiceReq(BaseModel):
    merchant_id: str
    title: str = Field(..., min_length=1, max_length=48)
    amount: int = Field(..., ge=1, le=500000)


class TopUpReq(BaseModel):
    amount: int = Field(..., ge=1, le=MAX_TOPUP_STARS)
    comment: str = Field(default="", max_length=200)


class WithdrawReq(BaseModel):
    gift_stars: int
    note: str = Field(default="", max_length=120)


class TransferReq(BaseModel):
    to_tg_id: int = Field(..., ge=1)
    amount: int = Field(..., ge=1, le=500000)
    comment: str = Field(default="", max_length=120)


class SupportMsg(BaseModel):
    role: str
    content: str


class SupportChatReq(BaseModel):
    message: str = Field(..., min_length=1, max_length=1000)
    history: List[SupportMsg] = Field(default_factory=list)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_USERNAME
    await restore_db_from_telegram()
    BOT_USERNAME = (await bot.get_me()).username
    try:
        await bot.set_my_commands([BotCommand(command="start", description="Открыть меню RoboPay")])
        if PUBLIC_URL.startswith("https://"):
            await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="RoboPay", web_app=WebAppInfo(url=f"{PUBLIC_URL}/")))
    except Exception as e:
        print(f"[Setup Warning] {e}")
    task = asyncio.create_task(run_bot_polling())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await bot.session.close()


app = FastAPI(title="RoboPay", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"ok": True, "bot": BOT_USERNAME, "groq_model": CACHED_GROQ_MODEL or GROQ_MODEL}


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


@app.get("/")
async def serve_miniapp():
    return FileResponse("index.html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, initData: str = Query(default="")):
    try:
        tg_user = verify_telegram_init_data(initData)
        uid = int(tg_user["id"])
    except Exception:
        await ws.close(code=1008)
        return
    await rt_hub.connect(uid, ws)
    try:
        while True:
            msg = await ws.receive_text()
            if msg == "ping":
                await ws.send_text("pong")
    except (WebSocketDisconnect, Exception):
        rt_hub.disconnect(uid, ws)


@app.get("/api/v1/me")
async def api_me(x_telegram_init_data: str = Header(default="")):
    tg_user = verify_telegram_init_data(x_telegram_init_data)
    uid = int(tg_user["id"])
    ensure_user(uid, tg_user.get("username"), tg_user.get("first_name"))
    with get_db() as conn:
        u = dict(conn.execute("SELECT tg_id, username, first_name, wallet_balance FROM users WHERE tg_id = ?", (uid,)).fetchone())
        projects = [dict(r) for r in conn.execute("SELECT merchant_id, title, balance_rp FROM merchants WHERE owner_tg_id = ? ORDER BY created_at DESC", (uid,)).fetchall()]
        topups = [dict(r) for r in conn.execute("SELECT request_id, amount_rp, status, created_at FROM topup_requests WHERE user_tg_id = ? ORDER BY created_at DESC LIMIT 6", (uid,)).fetchall()]
        withdrawals = [dict(r) for r in conn.execute("SELECT request_id, gift_stars, receive_stars, cost_rp, status, created_at FROM withdraw_requests WHERE user_tg_id = ? ORDER BY created_at DESC LIMIT 6", (uid,)).fetchall()]
        o_hist = [dict(r) for r in conn.execute("SELECT order_id AS id, title, -amount_rp AS delta, paid_at AS ts FROM orders WHERE payer_tg_id = ? AND status = 'paid' ORDER BY paid_at DESC LIMIT 8", (uid,)).fetchall()]
        t_hist = [dict(r) for r in conn.execute("SELECT transfer_id AS id, CASE WHEN from_tg_id = ? THEN 'Перевод → ' || to_tg_id ELSE 'От ' || from_tg_id END AS title, CASE WHEN from_tg_id = ? THEN -amount_rp ELSE net_rp END AS delta, created_at AS ts FROM transfers WHERE from_tg_id = ? OR to_tg_id = ? ORDER BY created_at DESC LIMIT 8", (uid, uid, uid, uid)).fetchall()]

    tiers = [{"gift_stars": g, "cost_rp": calc_withdraw_cost(g)[0], "fee_rp": calc_withdraw_cost(g)[1], "receive_stars": calc_withdraw_cost(g)[2]} for g in GIFT_TIERS]
    return {
        "user": u, "owner_username": OWNER_DISPLAY, "owner_url": OWNER_URL, "bot_username": BOT_USERNAME,
        "commission_rate": COMMISSION_RATE, "withdraw_fee_rate": WITHDRAW_FEE_RATE,
        "max_topup_stars": MAX_TOPUP_STARS, "gift_tiers": tiers,
        "projects": projects, "topups": topups, "withdrawals": withdrawals,
        "history": sorted(o_hist + t_hist, key=lambda x: x["ts"] or "", reverse=True)[:10],
    }


@app.post("/api/v1/support/chat")
async def api_support_chat(req: SupportChatReq, x_telegram_init_data: str = Header(default="")):
    global CACHED_GROQ_MODEL
    uid, bal = 0, 0
    if x_telegram_init_data:
        try:
            u = verify_telegram_init_data(x_telegram_init_data)
            uid = int(u["id"])
            bal = get_wallet_balance(uid)
        except Exception:
            pass

    if not GROQ_API_KEY:
        return {"reply": f"Я робот поддержки RoboPay! Добавьте GROQ_API_KEY в переменные Railway, чтобы включить ИИ. Пока можно написать владельцу {OWNER_DISPLAY}."}

    system_prompt = (
        f"Ты — дружелюбный, грамотный и спокойный русскоязычный помощник техподдержки RoboPay. "
        f"Отвечай только нормальными русскими словами и предложениями; не выводи отдельные буквы, обрывки, мусорные символы, код, JSON или markdown-разметку. "
        f"Пиши 2–4 коротких предложения, чтобы ответ удобно звучал при озвучивании. Сначала дай прямой ответ, затем один конкретный шаг. "
        f"Если вопрос непонятен — задай один уточняющий вопрос. Если не знаешь ответа или он не подтверждён фактами ниже, честно скажи об этом и предложи обратиться к владельцу {OWNER_DISPLAY}; не выдумывай функции, комиссии, статусы или инструкции. "
        f"Факты RoboPay: внутренняя валюта RP; 1 Telegram Star = 1 RP. Пополнение сейчас оформляется подарком до {MAX_TOPUP_STARS} Stars владельцу {OWNER_DISPLAY} с подтверждением заявки. "
        f"Вывод — заявка на подарок 15, 25, 50 или 100 Stars с комиссией {int(WITHDRAW_FEE_RATE * 100)}%, подтверждается владельцем. "
        f"Переводы по ID/QR и оплата счетов имеют комиссию {int(COMMISSION_RATE * 100)}%. Разработчики подключают ботов через REST API в разделе Мои проекты. "
        f"Текущий пользователь: ID {uid or 'гость'}, баланс {bal} RP."
    )
    messages = [{"role": "system", "content": system_prompt}]
    for m in req.history[-8:]:
        if m.role in ("user", "assistant"):
            messages.append({"role": m.role, "content": m.content[:600]})
    messages.append({"role": "user", "content": req.message})

    async with httpx.AsyncClient(timeout=20.0) as client:
        model = CACHED_GROQ_MODEL or GROQ_MODEL
        for attempt in range(2):
            try:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
                    json={"model": model, "messages": messages, "temperature": 0.6, "max_tokens": 320},
                )
                data = resp.json()
                if resp.status_code == 200:
                    CACHED_GROQ_MODEL = model
                    return {"reply": data["choices"][0]["message"]["content"].strip()}

                err_code = data.get("error", {}).get("code", "")
                print(f"[Groq Attempt {attempt + 1} Error] model={model} resp={data}")
                if attempt == 0 and (resp.status_code in (400, 404) or err_code == "model_not_found"):
                    fallback = await resolve_groq_model(client, exclude=model)
                    if fallback:
                        model = fallback
                        continue
                break
            except Exception as e:
                print(f"[Groq Exception] {e}")
                break

    return {"reply": f"Не удалось получить ответ от ИИ. Попробуйте ещё раз через пару секунд или напишите владельцу {OWNER_DISPLAY}."}


@app.post("/api/v1/topup")
async def api_topup(req: TopUpReq, x_telegram_init_data: str = Header(default="")):
    u = verify_telegram_init_data(x_telegram_init_data)
    ensure_user(int(u["id"]), u.get("username"), u.get("first_name"))
    return await create_topup_request(int(u["id"]), req.amount, req.comment)


@app.post("/api/v1/withdraw")
async def api_withdraw(req: WithdrawReq, x_telegram_init_data: str = Header(default="")):
    u = verify_telegram_init_data(x_telegram_init_data)
    ensure_user(int(u["id"]), u.get("username"), u.get("first_name"))
    return await create_withdraw_request(int(u["id"]), req.gift_stars, req.note)


@app.post("/api/v1/transfer")
async def api_transfer(req: TransferReq, x_telegram_init_data: str = Header(default="")):
    u = verify_telegram_init_data(x_telegram_init_data)
    from_id = int(u["id"])
    ensure_user(from_id, u.get("username"), u.get("first_name"))
    if req.to_tg_id == from_id:
        raise HTTPException(status_code=400, detail="Нельзя перевести себе")
    fee_rp = math.ceil(req.amount * COMMISSION_RATE) if COMMISSION_RATE > 0 else 0
    net_rp = max(1, req.amount - fee_rp)
    tid = f"tr_{secrets.token_hex(6)}"
    with get_db() as conn:
        if not conn.execute("SELECT tg_id FROM users WHERE tg_id = ?", (req.to_tg_id,)).fetchone():
            raise HTTPException(status_code=404, detail="Получатель ещё не запускал RoboPay")
        if conn.execute("UPDATE users SET wallet_balance = wallet_balance - ? WHERE tg_id = ? AND wallet_balance >= ?", (req.amount, from_id, req.amount)).rowcount == 0:
            raise HTTPException(status_code=400, detail="Недостаточно средств")
        conn.execute("UPDATE users SET wallet_balance = wallet_balance + ? WHERE tg_id = ?", (net_rp, req.to_tg_id))
        conn.execute("INSERT INTO transfers VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (tid, from_id, req.to_tg_id, req.amount, fee_rp, net_rp, req.comment, utc_now()))
        from_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (from_id,)).fetchone()["wallet_balance"]
        to_bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (req.to_tg_id,)).fetchone()["wallet_balance"]

    asyncio.create_task(sync_db_to_telegram(f"Перевод {tid} ({req.amount} RP)"))
    asyncio.create_task(rt_hub.push(req.to_tg_id, {"type": "sync", "wallet_balance": to_bal, "notify": f"+{net_rp} RP перевод от {from_id}", "emotion": "glee"}))
    try:
        await bot.send_message(req.to_tg_id, f"💸 <b>Входящий перевод</b>\n• Зачислено: <b>+{net_rp} RP</b> (комиссия {fee_rp} RP)\n• От: <code>{from_id}</code>", parse_mode="HTML")
    except Exception:
        pass
    return {"transfer_id": tid, "net_rp": net_rp, "fee_rp": fee_rp, "wallet_balance": from_bal}


@app.post("/api/v1/webapp/invoices")
async def api_wa_inv(req: WebAppInvoiceReq, x_telegram_init_data: str = Header(default="")):
    uid = int(verify_telegram_init_data(x_telegram_init_data)["id"])
    with get_db() as conn:
        if not conn.execute("SELECT 1 FROM merchants WHERE merchant_id = ? AND owner_tg_id = ?", (req.merchant_id, uid)).fetchone():
            raise HTTPException(status_code=403, detail="Проект не найден")
    o = create_order_record(req.merchant_id, req.title, "Счёт RoboPay", req.amount)
    asyncio.create_task(sync_db_to_telegram(f"Счёт {o['order_id']}"))
    return o


@app.post("/api/v1/invoices")
async def api_ext_inv(req: CreateInvoiceReq, x_api_key: str = Header(...)):
    amt = req.amount or req.amount_stars
    if not amt:
        raise HTTPException(status_code=422, detail="Укажите amount")
    with get_db() as conn:
        m = conn.execute("SELECT merchant_id FROM merchants WHERE api_key = ?", (x_api_key,)).fetchone()
    if not m:
        raise HTTPException(status_code=401, detail="Invalid X-API-Key")
    o = create_order_record(m["merchant_id"], req.title, req.description, amt, req.external_order_id)
    asyncio.create_task(sync_db_to_telegram(f"API-счёт {o['order_id']}"))
    return o


@app.get("/api/v1/orders/{order_id}")
async def api_get_order(order_id: str):
    with get_db() as conn:
        r = conn.execute("SELECT o.order_id, o.title, o.description, o.amount_rp, o.status, m.title AS merchant_title FROM orders o JOIN merchants m ON o.merchant_id = m.merchant_id WHERE o.order_id = ?", (order_id,)).fetchone()
    if not r:
        raise HTTPException(status_code=404, detail="Счёт не найден")
    d = dict(r)
    d["qr_payload"] = f"https://t.me/{BOT_USERNAME}?start=pay_{order_id}"
    return d


@app.post("/api/v1/orders/{order_id}/pay")
async def api_pay_order(order_id: str, x_telegram_init_data: str = Header(default="")):
    u = verify_telegram_init_data(x_telegram_init_data)
    pid = int(u["id"])
    ensure_user(pid, u.get("username"), u.get("first_name"))
    now = utc_now()
    with get_db() as conn:
        o = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        if not o:
            raise HTTPException(status_code=404, detail="Счёт не найден")
        if o["status"] == "paid":
            raise HTTPException(status_code=409, detail="Счёт уже оплачен")
        if conn.execute("UPDATE users SET wallet_balance = wallet_balance - ? WHERE tg_id = ? AND wallet_balance >= ?", (o["amount_rp"], pid, o["amount_rp"])).rowcount == 0:
            raise HTTPException(status_code=402, detail="Недостаточно средств на кошельке")
        if conn.execute("UPDATE orders SET status = 'paid', payer_tg_id = ?, paid_at = ? WHERE order_id = ? AND status = 'pending'", (pid, now, order_id)).rowcount == 0:
            raise HTTPException(status_code=409, detail="Счёт уже оплачен")
        conn.execute("UPDATE merchants SET balance_rp = balance_rp + ? WHERE merchant_id = ?", (o["net_rp"], o["merchant_id"]))
        upd = dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())
        mrc = dict(conn.execute("SELECT * FROM merchants WHERE merchant_id = ?", (o["merchant_id"],)).fetchone())
        bal = conn.execute("SELECT wallet_balance FROM users WHERE tg_id = ?", (pid,)).fetchone()["wallet_balance"]
    asyncio.create_task(dispatch_partner_notification(upd, mrc))
    asyncio.create_task(sync_db_to_telegram(f"Оплата {order_id} ({upd['amount_rp']} RP)"))
    return {"status": "paid", "order_id": order_id, "wallet_balance": bal}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=env_int("PORT", 8000), reload=False)

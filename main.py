import asyncio
import hashlib
import hmac
import html
import json
import os
import secrets
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

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
    LabeledPrice,
    MenuButtonWebApp,
    PreCheckoutQuery,
    WebAppInfo,
)
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# --- КОНФИГУРАЦИЯ (берётся из Variables в Railway) ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://your-domain.up.railway.app").rstrip("/")
WEBAPP_SHORT_NAME = os.getenv("WEBAPP_SHORT_NAME", "").strip()  # опционально, если настроен /newapp
COMMISSION_RATE = float(os.getenv("COMMISSION_RATE", "0.05"))
DB_PATH = os.getenv("DB_PATH", "gateway.db")
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID", "0"))
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
BOT_USERNAME = ""
db_sync_lock = asyncio.Lock()


# --- СОСТОЯНИЯ ДЛЯ ПОШАГОВОГО ВВОДА (FSM) ---
class CreateProjectFSM(StatesGroup):
    waiting_title = State()
    waiting_webhook = State()


class EditWebhookFSM(StatesGroup):
    waiting_url = State()


class QuickInvoiceFSM(StatesGroup):
    waiting_title = State()
    waiting_amount = State()


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def is_admin(user_id: int) -> bool:
    return ADMIN_ID != 0 and user_id == ADMIN_ID


def esc(text: Optional[str]) -> str:
    return html.escape(str(text or ""))


async def safe_edit(message: types.Message, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    try:
        await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup, disable_web_page_preview=True)
    except TelegramBadRequest:
        pass


# --- БАЗА ДАННЫХ И СИНХРОНИЗАЦИЯ С ТГ-КАНАЛОМ ---
def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS merchants (
                merchant_id TEXT PRIMARY KEY,
                owner_tg_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                api_key TEXT UNIQUE NOT NULL,
                webhook_url TEXT,
                balance_stars INTEGER DEFAULT 0,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                merchant_id TEXT NOT NULL,
                external_order_id TEXT,
                title TEXT NOT NULL,
                description TEXT,
                amount_stars INTEGER NOT NULL,
                fee_stars INTEGER NOT NULL,
                net_stars INTEGER NOT NULL,
                invoice_link TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                payer_tg_id INTEGER,
                telegram_charge_id TEXT,
                created_at TEXT NOT NULL,
                paid_at TEXT,
                FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
            );
            """
        )


def register_or_update_user(user: types.User) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO users (tg_id, username, first_name, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name
            """,
            (user.id, user.username, user.first_name, now),
        )


def create_atomic_snapshot(snapshot_path: str) -> None:
    with get_db() as src, sqlite3.connect(snapshot_path) as dst:
        src.backup(dst)


async def restore_db_from_telegram() -> None:
    if not DB_CHANNEL_ID:
        print("[DB Sync] DB_CHANNEL_ID не задан — используется локальная БД.")
        init_db()
        return

    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
        pinned = chat.pinned_message
        if pinned and pinned.document:
            await bot.download(pinned.document, destination=DB_PATH)
            print(f"[DB Sync] База восстановлена из канала (msg_id={pinned.message_id}).")
            init_db()
            return
    except Exception as e:
        print(f"[DB Sync] Не удалось скачать БД из канала (первый запуск?): {e}")

    init_db()
    await sync_db_to_telegram("Инициализация новой базы данных")


async def sync_db_to_telegram(reason: str = "Авто-синхронизация") -> Optional[int]:
    if not DB_CHANNEL_ID:
        return None

    async with db_sync_lock:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        snapshot_path = f"backup_{timestamp}.db"
        try:
            create_atomic_snapshot(snapshot_path)
            caption = (
                f"🗄 <b>RoboPay DB Snapshot</b>\n"
                f"• Событие: {esc(reason)}\n"
                f"• Время (UTC): <code>{timestamp}</code>"
            )
            msg = await bot.send_document(
                chat_id=DB_CHANNEL_ID,
                document=FSInputFile(snapshot_path, filename="gateway.db"),
                caption=caption,
                parse_mode="HTML",
            )
            await bot.pin_chat_message(
                chat_id=DB_CHANNEL_ID,
                message_id=msg.message_id,
                disable_notification=True,
            )
            return msg.message_id
        except Exception as e:
            print(f"[DB Sync Error] {e}")
            return None
        finally:
            if os.path.exists(snapshot_path):
                os.remove(snapshot_path)


# --- СОЗДАНИЕ СЧЁТА (ОБЩАЯ ЛОГИКА ДЛЯ БОТА И REST API) ---
async def create_order_record(
    merchant_id: str,
    title: str,
    description: str,
    amount_stars: int,
    external_order_id: Optional[str] = None,
) -> dict:
    order_id = f"ord_{secrets.token_hex(8)}"
    fee_stars = int(amount_stars * COMMISSION_RATE)
    net_stars = max(1, amount_stars - fee_stars)

    invoice_link = await bot.create_invoice_link(
        title=title[:32],
        description=description[:255],
        payload=order_id,
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=title[:32], amount=amount_stars)],
    )

    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO orders (
                order_id, merchant_id, external_order_id, title, description,
                amount_stars, fee_stars, net_stars, invoice_link, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order_id,
                merchant_id,
                external_order_id,
                title,
                description,
                amount_stars,
                fee_stars,
                net_stars,
                invoice_link,
                now,
            ),
        )

    bot_start_link = f"https://t.me/{BOT_USERNAME}?start=pay_{order_id}"
    short_deeplink = (
        f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp={order_id}"
        if WEBAPP_SHORT_NAME
        else bot_start_link
    )

    return {
        "order_id": order_id,
        "status": "pending",
        "amount_stars": amount_stars,
        "fee_stars": fee_stars,
        "net_stars": net_stars,
        "bot_pay_link": bot_start_link,
        "checkout_deeplink": short_deeplink,
        "webapp_url": f"{PUBLIC_URL}/?order_id={order_id}",
        "direct_invoice_link": invoice_link,
    }


# --- ВЕБХУКИ И СИГНАЛЫ ---
def sign_payload(secret_key: str, payload_bytes: bytes) -> str:
    return hmac.new(secret_key.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()


async def dispatch_partner_notification(order: dict, merchant: dict) -> None:
    signal_text = (
        f"⚡ <b>Поступила оплата — {esc(merchant['title'])}</b>\n\n"
        f"• Услуга: <b>{esc(order['title'])}</b>\n"
        f"• Оплачено: <b>{order['amount_stars']} ⭐</b>\n"
        f"• Зачислено на баланс: <b>+{order['net_stars']} ⭐</b>\n"
        f"• ID заказа: <code>{order['order_id']}</code>\n"
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
            "amount_stars": order["amount_stars"],
            "net_stars": order["net_stars"],
            "payer_tg_id": order["payer_tg_id"],
            "telegram_charge_id": order["telegram_charge_id"],
            "paid_at": order["paid_at"],
        }
        raw_body = json.dumps(event_data, ensure_ascii=False).encode("utf-8")
        signature = sign_payload(merchant["api_key"], raw_body)

        headers = {
            "Content-Type": "application/json",
            "X-StarPay-Signature": signature,
            "X-StarPay-Event": "payment.succeeded",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            for attempt in range(3):
                try:
                    resp = await client.post(merchant["webhook_url"], content=raw_body, headers=headers)
                    if 200 <= resp.status_code < 300:
                        break
                except Exception as e:
                    print(f"[Webhook Attempt {attempt + 1} Failed] {e}")
                    await asyncio.sleep(1.5 * (attempt + 1))


# --- КЛАВИАТУРЫ И ЭКРАНЫ ИНТЕРФЕЙСА ---
def build_main_menu(user: types.User) -> tuple[str, InlineKeyboardMarkup]:
    with get_db() as conn:
        stats = conn.execute(
            """
            SELECT COUNT(*) AS cnt, COALESCE(SUM(balance_stars), 0) AS total_balance
            FROM merchants WHERE owner_tg_id = ?
            """,
            (user.id,),
        ).fetchone()

    name = esc(user.first_name or "друг")
    text = (
        f"🤖 <b>Добро пожаловать в RoboPay, {name}!</b>\n\n"
        f"Ваш аккаунт автоматически привязан к шлюзу Telegram Stars.\n"
        f"Создавайте платёжные ссылки с анимированным терминалом, принимайте <b>⭐ Stars</b> "
        f"и получайте мгновенные сигналы в Telegram или по API.\n\n"
        f"👤 <b>ID аккаунта:</b> <code>{user.id}</code>\n"
        f"📦 <b>Активных проектов:</b> {stats['cnt']}\n"
        f"⭐ <b>Общий баланс:</b> <b>{stats['total_balance']} ⭐</b>"
    )

    buttons = [
        [
            InlineKeyboardButton(
                text="🤖 Открыть терминал RoboPay",
                web_app=WebAppInfo(url=f"{PUBLIC_URL}/"),
            )
        ],
        [
            InlineKeyboardButton(text="⚡ Выставить счёт", callback_data="quick_pay"),
            InlineKeyboardButton(text="📦 Мои проекты", callback_data="projects"),
        ],
        [
            InlineKeyboardButton(text="💰 Баланс и история", callback_data="balance"),
            InlineKeyboardButton(text="📖 Подключить API", callback_data="api_docs"),
        ],
    ]

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
        paid_stats = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(amount_stars), 0) AS vol FROM orders WHERE merchant_id = ? AND status = 'paid'",
            (merchant_id,),
        ).fetchone()

    webhook_display = f"<code>{esc(m['webhook_url'])}</code>" if m["webhook_url"] else "<i>Только Telegram-сигналы</i>"
    text = (
        f"📦 <b>Проект: {esc(m['title'])}</b>\n\n"
        f"• Баланс: <b>{m['balance_stars']} ⭐</b>\n"
        f"• Оплаченных счетов: <b>{paid_stats['cnt']}</b> (оборот: {paid_stats['vol']} ⭐)\n"
        f"• Комиссия шлюза: <b>{int(COMMISSION_RATE * 100)}%</b>\n\n"
        f"🆔 <b>Merchant ID:</b> <code>{m['merchant_id']}</code>\n"
        f"🔑 <b>API Key:</b> <tg-spoiler><code>{m['api_key']}</code></tg-spoiler>\n"
        f"🌐 <b>Webhook:</b> {webhook_display}"
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚡ Создать счёт в этом проекте", callback_data=f"qp_select:{merchant_id}")],
            [
                InlineKeyboardButton(text="🔗 Изменить Webhook", callback_data=f"edit_wh:{merchant_id}"),
                InlineKeyboardButton(text="🔄 Сбросить ключ", callback_data=f"reset_key:{merchant_id}"),
            ],
            [InlineKeyboardButton(text="◀️ К списку проектов", callback_data="projects")],
        ]
    )
    return text, kb


# --- ОБРАБОТЧИКИ КОМАНД И КНОПОК ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext) -> None:
    await state.clear()
    register_or_update_user(message.from_user)

    # Обработка платёжной ссылки формата https://t.me/bot?start=pay_ord_...
    if command.args and command.args.startswith("pay_"):
        order_id = command.args[4:]
        with get_db() as conn:
            order = conn.execute(
                """
                SELECT o.*, m.title AS merchant_title
                FROM orders o
                JOIN merchants m ON o.merchant_id = m.merchant_id
                WHERE o.order_id = ?
                """,
                (order_id,),
            ).fetchone()

        if not order:
            await message.answer("⚠️ Счёт не найден или срок его действия истёк.")
            return

        if order["status"] == "paid":
            await message.answer(f"✅ Счёт <b>«{esc(order['title'])}»</b> уже успешно оплачен.", parse_mode="HTML")
            return

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"🤖 Оплатить {order['amount_stars']} ⭐ через RoboPay",
                        web_app=WebAppInfo(url=f"{PUBLIC_URL}/?order_id={order_id}"),
                    )
                ]
            ]
        )
        await message.answer(
            f"💳 <b>Счёт на оплату от {esc(order['merchant_title'])}</b>\n\n"
            f"• Услуга: <b>{esc(order['title'])}</b>\n"
            f"• Сумма: <b>{order['amount_stars']} ⭐</b>\n\n"
            f"Нажмите кнопку ниже, чтобы открыть терминал RoboPay 👇",
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


# --- БАЛАНС И ИСТОРИЯ ---
@dp.callback_query(F.data == "balance")
async def cb_balance(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    with get_db() as conn:
        merchants = conn.execute(
            "SELECT title, balance_stars FROM merchants WHERE owner_tg_id = ? ORDER BY balance_stars DESC",
            (user_id,),
        ).fetchall()
        recent = conn.execute(
            """
            SELECT o.title, o.amount_stars, o.net_stars, o.paid_at, m.title AS m_title
            FROM orders o
            JOIN merchants m ON o.merchant_id = m.merchant_id
            WHERE m.owner_tg_id = ? AND o.status = 'paid'
            ORDER BY o.paid_at DESC LIMIT 5
            """,
            (user_id,),
        ).fetchall()

    total = sum(m["balance_stars"] for m in merchants)
    lines = [f"💰 <b>Ваш общий баланс: {total} ⭐</b>\n"]

    if merchants:
        lines.append("<b>По проектам:</b>")
        for m in merchants:
            lines.append(f"• {esc(m['title'])}: <b>{m['balance_stars']} ⭐</b>")
    else:
        lines.append("<i>У вас пока нет созданных проектов.</i>")

    if recent:
        lines.append("\n<b>Последние поступления:</b>")
        for r in recent:
            lines.append(f"• +{r['net_stars']} ⭐ — {esc(r['title'])} ({esc(r['m_title'])})")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📦 Перейти к проектам", callback_data="projects")],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
        ]
    )
    await safe_edit(callback.message, "\n".join(lines), kb)
    await callback.answer()


# --- УПРАВЛЕНИЕ ПРОЕКТАМИ ---
@dp.callback_query(F.data == "projects")
async def cb_projects(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT merchant_id, title, balance_stars FROM merchants WHERE owner_tg_id = ? ORDER BY created_at DESC",
            (callback.from_user.id,),
        ).fetchall()

    buttons = []
    for r in rows:
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"📦 {r['title']} ({r['balance_stars']} ⭐)",
                    callback_data=f"proj:{r['merchant_id']}",
                )
            ]
        )

    buttons.append([InlineKeyboardButton(text="➕ Создать новый проект", callback_data="new_project")])
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])

    text = (
        "📦 <b>Ваши проекты</b>\n\n"
        "Выберите проект для просмотра API-ключа, настройки вебхука и создания счетов — или добавьте новый."
        if rows
        else "📦 <b>У вас пока нет проектов</b>\n\nСоздайте первый проект за 10 секунд, чтобы принимать Stars по ссылкам и через API."
    )
    await safe_edit(callback.message, text, InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@dp.callback_query(F.data.startswith("proj:"))
async def cb_project_detail(callback: types.CallbackQuery) -> None:
    merchant_id = callback.data.split(":", 1)[1]
    card = build_project_card(merchant_id, callback.from_user.id)
    if not card:
        await callback.answer("Проект не найден", show_alert=True)
        return
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer()


@dp.callback_query(F.data == "new_project")
async def cb_new_project_start(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.set_state(CreateProjectFSM.waiting_title)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="projects")]])
    await safe_edit(
        callback.message,
        "🆕 <b>Создание проекта — шаг 1 из 2</b>\n\nОтправьте название вашего бота, канала или сервиса (до 40 символов):",
        kb,
    )
    await callback.answer()


@dp.message(CreateProjectFSM.waiting_title)
async def fsm_project_title(message: types.Message, state: FSMContext) -> None:
    title = (message.text or "").strip()[:40]
    if not title:
        await message.answer("Введите текстовое название проекта.")
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
        f"🆕 <b>Проект «{esc(title)}» — шаг 2 из 2</b>\n\n"
        "Если вы подключаете свой сервер, отправьте HTTPS-ссылку для Webhook.\n"
        "Если сервера нет — нажмите <b>«Пропустить»</b>, бот будет присылать уведомления прямо сюда.",
        parse_mode="HTML",
        reply_markup=kb,
    )


async def finalize_project_creation(owner_id: int, title: str, webhook_url: Optional[str]) -> str:
    merchant_id = f"mrc_{secrets.token_hex(6)}"
    api_key = f"sp_live_{secrets.token_hex(16)}"
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO merchants (merchant_id, owner_tg_id, title, api_key, webhook_url, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (merchant_id, owner_id, title, api_key, webhook_url, now),
        )
    asyncio.create_task(sync_db_to_telegram(f"Новый проект: {title} ({merchant_id})"))
    return merchant_id


@dp.callback_query(CreateProjectFSM.waiting_webhook, F.data == "skip_webhook")
async def fsm_project_skip_webhook(callback: types.CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    await state.clear()
    merchant_id = await finalize_project_creation(callback.from_user.id, data["title"], None)
    card = build_project_card(merchant_id, callback.from_user.id)
    await safe_edit(callback.message, f"✅ <b>Проект создан!</b>\n\n{card[0]}", card[1])
    await callback.answer("Проект создан!")


@dp.message(CreateProjectFSM.waiting_webhook)
async def fsm_project_webhook(message: types.Message, state: FSMContext) -> None:
    url = (message.text or "").strip()
    if not url.startswith(("https://", "http://")):
        await message.answer("⚠️ Ссылка должна начинаться с `https://`. Отправьте корректный URL или нажмите «Пропустить» выше.")
        return

    data = await state.get_data()
    await state.clear()
    merchant_id = await finalize_project_creation(message.from_user.id, data["title"], url)
    card = build_project_card(merchant_id, message.from_user.id)
    await message.answer(f"✅ <b>Проект создан!</b>\n\n{card[0]}", parse_mode="HTML", reply_markup=card[1])


# --- РЕДАКТИРОВАНИЕ WEBHOOK И СБРОС КЛЮЧА ---
@dp.callback_query(F.data.startswith("edit_wh:"))
async def cb_edit_webhook(callback: types.CallbackQuery, state: FSMContext) -> None:
    merchant_id = callback.data.split(":", 1)[1]
    await state.set_state(EditWebhookFSM.waiting_url)
    await state.update_data(merchant_id=merchant_id)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Отключить Webhook", callback_data=f"clear_wh:{merchant_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"proj:{merchant_id}")],
        ]
    )
    await safe_edit(callback.message, "🔗 Отправьте новый HTTPS-адрес для Webhook:", kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("clear_wh:"))
async def cb_clear_webhook(callback: types.CallbackQuery, state: FSMContext) -> None:
    merchant_id = callback.data.split(":", 1)[1]
    await state.clear()
    with get_db() as conn:
        conn.execute(
            "UPDATE merchants SET webhook_url = NULL WHERE merchant_id = ? AND owner_tg_id = ?",
            (merchant_id, callback.from_user.id),
        )
    asyncio.create_task(sync_db_to_telegram(f"Отключён webhook ({merchant_id})"))
    card = build_project_card(merchant_id, callback.from_user.id)
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer("Webhook отключён")


@dp.message(EditWebhookFSM.waiting_url)
async def fsm_save_webhook(message: types.Message, state: FSMContext) -> None:
    url = (message.text or "").strip()
    if not url.startswith(("https://", "http://")):
        await message.answer("⚠️ Адрес должен начинаться с `https://`.")
        return
    data = await state.get_data()
    merchant_id = data["merchant_id"]
    await state.clear()
    with get_db() as conn:
        conn.execute(
            "UPDATE merchants SET webhook_url = ? WHERE merchant_id = ? AND owner_tg_id = ?",
            (url, merchant_id, message.from_user.id),
        )
    asyncio.create_task(sync_db_to_telegram(f"Обновлён webhook ({merchant_id})"))
    card = build_project_card(merchant_id, message.from_user.id)
    await message.answer(f"✅ Webhook обновлён!\n\n{card[0]}", parse_mode="HTML", reply_markup=card[1])


@dp.callback_query(F.data.startswith("reset_key:"))
async def cb_reset_key(callback: types.CallbackQuery) -> None:
    merchant_id = callback.data.split(":", 1)[1]
    new_key = f"sp_live_{secrets.token_hex(16)}"
    with get_db() as conn:
        conn.execute(
            "UPDATE merchants SET api_key = ? WHERE merchant_id = ? AND owner_tg_id = ?",
            (new_key, merchant_id, callback.from_user.id),
        )
    asyncio.create_task(sync_db_to_telegram(f"Перевыпущен API-ключ ({merchant_id})"))
    card = build_project_card(merchant_id, callback.from_user.id)
    await safe_edit(callback.message, card[0], card[1])
    await callback.answer("🔑 Новый API-ключ сгенерирован!", show_alert=True)


# --- БЫСТРОЕ ВЫСТАВЛЕНИЕ СЧЁТА ПО КНОПКАМ ---
@dp.callback_query(F.data == "quick_pay")
async def cb_quick_pay_menu(callback: types.CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    with get_db() as conn:
        rows = conn.execute(
            "SELECT merchant_id, title FROM merchants WHERE owner_tg_id = ? ORDER BY created_at DESC",
            (callback.from_user.id,),
        ).fetchall()

    if not rows:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать первый проект", callback_data="new_project")],
                [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
            ]
        )
        await safe_edit(
            callback.message,
            "⚡ <b>Чтобы выставить счёт, сначала создайте проект</b> — на его баланс будут зачисляться звёзды.",
            kb,
        )
        await callback.answer()
        return

    if len(rows) == 1:
        await start_quick_invoice_flow(callback, state, rows[0]["merchant_id"])
        return

    buttons = [
        [InlineKeyboardButton(text=f"📦 {r['title']}", callback_data=f"qp_select:{r['merchant_id']}")]
        for r in rows
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")])
    await safe_edit(
        callback.message,
        "⚡ <b>Выставление счёта</b>\n\nВыберите проект, от имени которого будет создан счёт:",
        InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("qp_select:"))
async def cb_quick_pay_select(callback: types.CallbackQuery, state: FSMContext) -> None:
    merchant_id = callback.data.split(":", 1)[1]
    await start_quick_invoice_flow(callback, state, merchant_id)


async def start_quick_invoice_flow(callback: types.CallbackQuery, state: FSMContext, merchant_id: str) -> None:
    await state.set_state(QuickInvoiceFSM.waiting_title)
    await state.update_data(merchant_id=merchant_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")]])
    await safe_edit(
        callback.message,
        "⚡ <b>Новый счёт — шаг 1 из 2</b>\n\nНапишите название услуги или товара (например: <i>VIP-доступ на месяц</i>):",
        kb,
    )
    await callback.answer()


@dp.message(QuickInvoiceFSM.waiting_title)
async def fsm_quick_pay_title(message: types.Message, state: FSMContext) -> None:
    title = (message.text or "").strip()[:32]
    if not title:
        await message.answer("Введите название услуги текстом.")
        return

    await state.update_data(title=title)
    await state.set_state(QuickInvoiceFSM.waiting_amount)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="15 ⭐", callback_data="qp_amt:15"),
                InlineKeyboardButton(text="50 ⭐", callback_data="qp_amt:50"),
                InlineKeyboardButton(text="100 ⭐", callback_data="qp_amt:100"),
                InlineKeyboardButton(text="250 ⭐", callback_data="qp_amt:250"),
            ],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="main_menu")],
        ]
    )
    await message.answer(
        f"⚡ <b>Услуга: «{esc(title)}» — шаг 2 из 2</b>\n\nВыберите сумму в Stars кнопкой или введите любое число вручную:",
        parse_mode="HTML",
        reply_markup=kb,
    )


async def send_created_invoice_card(target_msg: types.Message, merchant_id: str, title: str, amount: int, is_edit: bool) -> None:
    order = await create_order_record(
        merchant_id=merchant_id,
        title=title,
        description="Мгновенная оплата через терминал RoboPay",
        amount_stars=amount,
    )
    asyncio.create_task(sync_db_to_telegram(f"Создан счёт {order['order_id']} ({amount} ⭐)"))

    text = (
        f"✅ <b>Счёт успешно создан!</b>\n\n"
        f"• Услуга: <b>{esc(title)}</b>\n"
        f"• К оплате: <b>{amount} ⭐</b> (к зачислению: {order['net_stars']} ⭐)\n"
        f"• ID счёта: <code>{order['order_id']}</code>\n\n"
        f"🔗 <b>Ссылка для клиента (скопируйте и отправьте):</b>\n"
        f"<code>{order['checkout_deeplink']}</code>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤖 Проверить анимацию оплаты",
                    web_app=WebAppInfo(url=order["webapp_url"]),
                )
            ],
            [InlineKeyboardButton(text="⚡ Создать ещё один счёт", callback_data=f"qp_select:{merchant_id}")],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
        ]
    )
    if is_edit:
        await safe_edit(target_msg, text, kb)
    else:
        await target_msg.answer(text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)


@dp.callback_query(QuickInvoiceFSM.waiting_amount, F.data.startswith("qp_amt:"))
async def fsm_quick_pay_amount_btn(callback: types.CallbackQuery, state: FSMContext) -> None:
    amount = int(callback.data.split(":", 1)[1])
    data = await state.get_data()
    await state.clear()
    await callback.answer("Создаю счёт...")
    await send_created_invoice_card(callback.message, data["merchant_id"], data["title"], amount, is_edit=True)


@dp.message(QuickInvoiceFSM.waiting_amount)
async def fsm_quick_pay_amount_text(message: types.Message, state: FSMContext) -> None:
    raw = (message.text or "").strip()
    if not raw.isdigit() or not (1 <= int(raw) <= 100000):
        await message.answer("⚠️ Введите целое число звёзд от 1 до 100000.")
        return

    amount = int(raw)
    data = await state.get_data()
    await state.clear()
    await send_created_invoice_card(message, data["merchant_id"], data["title"], amount, is_edit=False)


# --- ДОКУМЕНТАЦИЯ API ---
@dp.callback_query(F.data == "api_docs")
async def cb_api_docs(callback: types.CallbackQuery) -> None:
    text = (
        f"📖 <b>Интеграция RoboPay API</b>\n\n"
        f"<b>1. Создание счёта (POST):</b>\n"
        f"<code>{PUBLIC_URL}/api/v1/invoices</code>\n\n"
        f"<b>Заголовок:</b> <code>X-API-Key: ваш_ключ_из_проекта</code>\n"
        f"<b>JSON-тело:</b>\n"
        f"<pre>{{\n"
        f'  "title": "VIP доступ",\n'
        f'  "description": "Подписка 30 дней",\n'
        f'  "amount_stars": 100,\n'
        f'  "external_order_id": "user_42"\n'
        f"}}</pre>\n\n"
        f"В ответе придёт поле <code>checkout_deeplink</code> — отдайте эту ссылку пользователю.\n"
        f"После оплаты на ваш Webhook придёт событие <code>payment.succeeded</code> с подписью HMAC-SHA256 в заголовке <code>X-StarPay-Signature</code>."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📦 Мои проекты и ключи", callback_data="projects")],
            [InlineKeyboardButton(text="◀️ Главное меню", callback_data="main_menu")],
        ]
    )
    await safe_edit(callback.message, text, kb)
    await callback.answer()


# --- АДМИН-ПАНЕЛЬ И БЭКАП (СТРОГО ТОЛЬКО ДЛЯ ADMIN_ID) ---
def build_admin_panel() -> tuple[str, InlineKeyboardMarkup]:
    with get_db() as conn:
        users_cnt = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        proj_cnt = conn.execute("SELECT COUNT(*) FROM merchants").fetchone()[0]
        paid = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(amount_stars), 0) AS vol, COALESCE(SUM(fee_stars), 0) AS fee FROM orders WHERE status = 'paid'"
        ).fetchone()

    text = (
        f"🛠 <b>Админ-панель RoboPay</b>\n\n"
        f"• Пользователей в базе: <b>{users_cnt}</b>\n"
        f"• Подключённых проектов: <b>{proj_cnt}</b>\n"
        f"• Успешных оплат: <b>{paid['cnt']}</b>\n"
        f"• Общий оборот: <b>{paid['vol']} ⭐</b>\n"
        f"• Заработанная комиссия: <b>{paid['fee']} ⭐</b>\n"
        f"• Канал БД: <code>{DB_CHANNEL_ID or 'не задан'}</code>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💾 Сделать бэкап БД в канал", callback_data="admin_backup")],
            [InlineKeyboardButton(text="🔄 Обновить статистику", callback_data="admin_panel")],
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


@dp.callback_query(F.data == "admin_backup")
async def cb_admin_backup(callback: types.CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    msg_id = await sync_db_to_telegram(f"Ручной бэкап из админ-панели ({callback.from_user.id})")
    if msg_id:
        await callback.answer(f"✅ Бэкап закреплён в канале (msg #{msg_id})", show_alert=True)
    else:
        await callback.answer("⚠️ Ошибка: проверьте DB_CHANNEL_ID и права бота", show_alert=True)


@dp.message(Command("backup"))
async def cmd_manual_backup(message: types.Message) -> None:
    if not is_admin(message.from_user.id):
        return  # Для всех кроме админа команда полностью игнорируется

    msg_id = await sync_db_to_telegram(f"Ручной бэкап командой /backup ({message.from_user.id})")
    if msg_id:
        await message.answer(f"✅ Слепок БД отправлен и закреплён в канале (ID: <code>{msg_id}</code>).", parse_mode="HTML")
    else:
        await message.answer("⚠️ Не удалось отправить бэкап. Проверьте <code>DB_CHANNEL_ID</code> и права бота в канале.", parse_mode="HTML")


# --- ОБРАБОТКА ПЛАТЕЖЕЙ TELEGRAM STARS (XTR) ---
@dp.pre_checkout_query()
async def process_pre_checkout(query: PreCheckoutQuery) -> None:
    order_id = query.invoice_payload
    with get_db() as conn:
        order = conn.execute("SELECT status FROM orders WHERE order_id = ?", (order_id,)).fetchone()

    if not order:
        await query.answer(ok=False, error_message="Счёт не найден в системе шлюза.")
        return
    if order["status"] == "paid":
        await query.answer(ok=False, error_message="Этот счёт уже был оплачен.")
        return

    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def process_successful_payment(message: types.Message) -> None:
    payment = message.successful_payment
    order_id = payment.invoice_payload
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        order_row = conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        if not order_row or order_row["status"] == "paid":
            return

        conn.execute(
            """
            UPDATE orders
            SET status = 'paid', payer_tg_id = ?, telegram_charge_id = ?, paid_at = ?
            WHERE order_id = ?
            """,
            (message.from_user.id, payment.telegram_payment_charge_id, now, order_id),
        )
        conn.execute(
            "UPDATE merchants SET balance_stars = balance_stars + ? WHERE merchant_id = ?",
            (order_row["net_stars"], order_row["merchant_id"]),
        )
        updated_order = dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())
        merchant = dict(
            conn.execute("SELECT * FROM merchants WHERE merchant_id = ?", (order_row["merchant_id"],)).fetchone()
        )

    asyncio.create_task(dispatch_partner_notification(updated_order, merchant))
    asyncio.create_task(sync_db_to_telegram(f"Оплата {order_id} (+{updated_order['amount_stars']} ⭐)"))


# --- FASTAPI REST API И АВТОНАСТРОЙКА БОТА ПРИ СТАРТЕ ---
class CreateInvoiceRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=32)
    description: str = Field(default="Оплата цифровой услуги", max_length=255)
    amount_stars: int = Field(..., ge=1, le=100000)
    external_order_id: Optional[str] = Field(default=None, max_length=128)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_USERNAME
    await restore_db_from_telegram()
    me = await bot.get_me()
    BOT_USERNAME = me.username

    # Автоматически настраиваем список команд и кнопку Mini App слева от поля ввода
    try:
        await bot.set_my_commands([BotCommand(command="start", description="Открыть главное меню RoboPay")])
        if PUBLIC_URL.startswith("https://"):
            await bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(
                    text="🤖 RoboPay",
                    web_app=WebAppInfo(url=f"{PUBLIC_URL}/"),
                )
            )
            print("[Bot Setup] Кнопка Mini App автоматически привязана к меню бота.")
    except Exception as e:
        print(f"[Bot Setup Warning] {e}")

    polling_task = asyncio.create_task(dp.start_polling(bot))
    yield
    polling_task.cancel()
    await bot.session.close()


app = FastAPI(title="RoboPay Stars Gateway", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def serve_miniapp():
    return FileResponse("index.html")


@app.post("/api/v1/invoices")
async def api_create_invoice(req: CreateInvoiceRequest, x_api_key: str = Header(...)):
    with get_db() as conn:
        merchant = conn.execute("SELECT * FROM merchants WHERE api_key = ?", (x_api_key,)).fetchone()

    if not merchant:
        raise HTTPException(status_code=401, detail="Invalid X-API-Key")

    order = await create_order_record(
        merchant_id=merchant["merchant_id"],
        title=req.title,
        description=req.description,
        amount_stars=req.amount_stars,
        external_order_id=req.external_order_id,
    )
    asyncio.create_task(sync_db_to_telegram(f"API-счёт {order['order_id']} ({req.amount_stars} ⭐)"))
    return order


@app.get("/api/v1/orders/{order_id}")
async def api_get_order(order_id: str):
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT o.order_id, o.title, o.description, o.amount_stars, o.status, o.invoice_link,
                   m.title AS merchant_title
            FROM orders o
            JOIN merchants m ON o.merchant_id = m.merchant_id
            WHERE o.order_id = ?
            """,
            (order_id,),
        ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Order not found")

    return dict(row)


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)

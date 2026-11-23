import asyncio
import hashlib
import hmac
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
from aiogram.filters import Command
from aiogram.types import (
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    PreCheckoutQuery,
    WebAppInfo,
)
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
PUBLIC_URL = os.getenv("PUBLIC_URL", "https://your-domain.up.railway.app").rstrip("/")
WEBAPP_SHORT_NAME = os.getenv("WEBAPP_SHORT_NAME", "pay")
COMMISSION_RATE = float(os.getenv("COMMISSION_RATE", "0.05"))
DB_PATH = os.getenv("DB_PATH", "gateway.db")

# ID приватного Telegram-канала для хранения БД и бэкапов (например, -1001234567890)
DB_CHANNEL_ID = int(os.getenv("DB_CHANNEL_ID", "0"))
# Твой личный Telegram ID для доступа к команде /backup
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
BOT_USERNAME = ""
db_sync_lock = asyncio.Lock()


# --- БАЗА ДАННЫХ И АТОМАРНЫЕ БЭКАПЫ ---
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


def create_atomic_snapshot(snapshot_path: str) -> None:
    """Создаёт консистентную копию живой SQLite-базы без риска повреждения WAL."""
    with get_db() as src, sqlite3.connect(snapshot_path) as dst:
        src.backup(dst)


async def restore_db_from_telegram() -> None:
    """При запуске сервера скачивает актуальную БД из закреплённого сообщения ТГ-канала."""
    if not DB_CHANNEL_ID:
        print("[DB Sync] DB_CHANNEL_ID не задан — работаем с локальной БД.")
        init_db()
        return

    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
        pinned = chat.pinned_message
        if pinned and pinned.document:
            await bot.download(pinned.document, destination=DB_PATH)
            print(f"[DB Sync] База успешно восстановлена из канала (msg_id={pinned.message_id}).")
            init_db()  # На случай добавления новых таблиц в будущих обновлениях
            return
    except Exception as e:
        print(f"[DB Sync] Не удалось скачать БД из канала (возможно, первый запуск): {e}")

    # Если канал пуст — создаём новую базу и сразу закрепляем стартовый файл
    init_db()
    await sync_db_to_telegram("Инициализация новой базы данных")


async def sync_db_to_telegram(reason: str = "Авто-синхронизация") -> Optional[int]:
    """Отправляет свежий снимок БД в ТГ-канал и закрепляет его как актуальный."""
    if not DB_CHANNEL_ID:
        return None

    async with db_sync_lock:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        snapshot_path = f"backup_{timestamp}.db"
        try:
            create_atomic_snapshot(snapshot_path)
            caption = (
                f"🗄 <b>RoboPay DB Snapshot</b>\n"
                f"• Событие: {reason}\n"
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


# --- ВЕБХУКИ И СИГНАЛЫ ---
def sign_payload(secret_key: str, payload_bytes: bytes) -> str:
    return hmac.new(secret_key.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()


async def dispatch_partner_notification(order: dict, merchant: dict) -> None:
    signal_text = (
        f"⚡ <b>Оплата получена ({merchant['title']})</b>\n\n"
        f"• Услуга: <b>{order['title']}</b>\n"
        f"• Сумма: <b>{order['amount_stars']} ⭐</b> (зачислено: <b>{order['net_stars']} ⭐</b>)\n"
        f"• ID заказа: <code>{order['order_id']}</code>\n"
        f"• Внешний ID: <code>{order['external_order_id'] or '—'}</code>\n"
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


# --- КОМАНДЫ БОТА ---
@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🤖 Открыть терминал RoboPay",
                    web_app=WebAppInfo(url=f"{PUBLIC_URL}/"),
                )
            ]
        ]
    )
    await message.answer(
        "<b>RoboPay — платёжный шлюз для Telegram Stars (XTR)</b>\n\n"
        "• <code>/new Название | https://ваш-вебхук</code> — создать проект и получить API-ключ\n"
        "• <code>/balance</code> — проверить баланс и статистику\n"
        "• <code>/backup</code> — принудительно сохранить слепок БД в канал",
        parse_mode="HTML",
        reply_markup=kb,
    )


@dp.message(Command("new"))
async def cmd_new_project(message: types.Message) -> None:
    args = message.text.replace("/new", "", 1).strip()
    if not args:
        await message.answer(
            "Укажите название проекта (и опционально URL вебхука через <code>|</code>):\n"
            "<code>/new MyShopBot | https://example.com/webhook</code>",
            parse_mode="HTML",
        )
        return

    parts = [p.strip() for p in args.split("|", 1)]
    title = parts[0][:64]
    webhook_url = parts[1] if len(parts) > 1 and parts[1].startswith("http") else None

    merchant_id = f"mrc_{secrets.token_hex(6)}"
    api_key = f"sp_live_{secrets.token_hex(16)}"
    now = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO merchants (merchant_id, owner_tg_id, title, api_key, webhook_url, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (merchant_id, message.from_user.id, title, api_key, webhook_url, now),
        )

    # Синхронизируем БД в Telegram-канал после создания нового проекта
    asyncio.create_task(sync_db_to_telegram(f"Новый проект: {title} ({merchant_id})"))

    await message.answer(
        f"✅ <b>Проект «{title}» зарегистрирован!</b>\n\n"
        f"• Merchant ID: <code>{merchant_id}</code>\n"
        f"• API Key: <code>{api_key}</code>\n"
        f"• Webhook: <code>{webhook_url or 'Только Telegram-сигналы'}</code>\n"
        f"• Комиссия шлюза: <b>{int(COMMISSION_RATE * 100)}%</b>",
        parse_mode="HTML",
    )


@dp.message(Command("balance"))
async def cmd_balance(message: types.Message) -> None:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT title, merchant_id, balance_stars FROM merchants WHERE owner_tg_id = ?",
            (message.from_user.id,),
        ).fetchall()

    if not rows:
        await message.answer("У вас пока нет проектов. Создайте первый командой <code>/new Название</code>", parse_mode="HTML")
        return

    lines = ["💎 <b>Ваши проекты и балансы:</b>\n"]
    for r in rows:
        lines.append(f"• <b>{r['title']}</b> (<code>{r['merchant_id']}</code>): <b>{r['balance_stars']} ⭐</b>")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("backup"))
async def cmd_manual_backup(message: types.Message) -> None:
    if ADMIN_ID and message.from_user.id != ADMIN_ID:
        return

    msg_id = await sync_db_to_telegram(f"Ручной бэкап от админа ({message.from_user.id})")
    if msg_id:
        await message.answer(f"✅ Слепок БД отправлен и закреплён в канале (ID сообщения: <code>{msg_id}</code>).", parse_mode="HTML")
    else:
        await message.answer("⚠️ Не удалось отправить бэкап. Проверьте <code>DB_CHANNEL_ID</code> и права бота в канале.", parse_mode="HTML")


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
    # Сразу фиксируем оплату в облачном ТГ-канале
    asyncio.create_task(sync_db_to_telegram(f"Оплата {order_id} (+{updated_order['amount_stars']} ⭐)"))


# --- FASTAPI REST API ---
class CreateInvoiceRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=64)
    description: str = Field(default="Оплата цифровой услуги", max_length=255)
    amount_stars: int = Field(..., ge=1, le=100000)
    external_order_id: Optional[str] = Field(default=None, max_length=128)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global BOT_USERNAME
    await restore_db_from_telegram()
    me = await bot.get_me()
    BOT_USERNAME = me.username
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
async def create_invoice(req: CreateInvoiceRequest, x_api_key: str = Header(...)):
    with get_db() as conn:
        merchant = conn.execute("SELECT * FROM merchants WHERE api_key = ?", (x_api_key,)).fetchone()

    if not merchant:
        raise HTTPException(status_code=401, detail="Invalid X-API-Key")

    order_id = f"ord_{secrets.token_hex(8)}"
    fee_stars = int(req.amount_stars * COMMISSION_RATE)
    net_stars = max(1, req.amount_stars - fee_stars)

    invoice_link = await bot.create_invoice_link(
        title=req.title,
        description=req.description,
        payload=order_id,
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=req.title, amount=req.amount_stars)],
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
                merchant["merchant_id"],
                req.external_order_id,
                req.title,
                req.description,
                req.amount_stars,
                fee_stars,
                net_stars,
                invoice_link,
                now,
            ),
        )

    return {
        "order_id": order_id,
        "status": "pending",
        "amount_stars": req.amount_stars,
        "checkout_deeplink": f"https://t.me/{BOT_USERNAME}/{WEBAPP_SHORT_NAME}?startapp={order_id}",
        "web_preview_url": f"{PUBLIC_URL}/?order_id={order_id}",
        "direct_invoice_link": invoice_link,
    }


@app.get("/api/v1/orders/{order_id}")
async def get_order(order_id: str):
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

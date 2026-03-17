import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

import aiohttp
from aiohttp import web
from telegram import Bot, BotCommand, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

TELEGRAM_BOT_TOKEN = "8694705597:AAEGcuZyxKl4WZAS2ark0vBE7zWL3pxYjNs"

POLL_INTERVAL          = 4
SPREAD_THRESHOLD       = 1.0
SPREAD_DEPTH_MIN       = 1.5
MIN_VOLUME_24H         = 500_000
MIN_LIQUIDITY          = 50_000
MAX_LEVERAGE           = 100
SIGNAL_COOLDOWN        = 600
SUBSCRIBERS_FILE       = "subscribers.json"
MAX_FUNDING_RATE       = 0.003
MAX_SPOT_FUTURES_DIFF  = 0.30
SPREAD_STABLE_WINDOW   = 12
SPREAD_STABLE_COUNT    = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

MEXC_TICKER_ALL  = "https://contract.mexc.com/api/v1/contract/ticker"
MEXC_DETAIL      = "https://contract.mexc.com/api/v1/contract/detail"
MEXC_DEPTH       = "https://contract.mexc.com/api/v1/contract/depth/"
MEXC_FUNDING     = "https://contract.mexc.com/api/v1/contract/funding_rate/"

subscribers:     set[int]         = set()
last_signal:     dict[str, float] = {}
last_spread:     dict[str, float] = {}
current_signals: dict[str, dict]  = {}
contract_info:   dict[str, dict]  = {}
spread_history:  dict[str, list]  = {}


def load_subscribers():
    global subscribers
    if Path(SUBSCRIBERS_FILE).exists():
        with open(SUBSCRIBERS_FILE) as f:
            subscribers = set(json.load(f))
        log.info(f"Loaded {len(subscribers)} subscribers")


def save_subscribers():
    with open(SUBSCRIBERS_FILE, "w") as f:
        json.dump(list(subscribers), f)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in subscribers:
        subscribers.add(chat_id)
        save_subscribers()
        log.info(f"New subscriber: {chat_id}")
        for info in list(current_signals.values()):
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=build_message(info),
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                )
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning(f"Failed to send catchup signal to {chat_id}: {e}")


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in subscribers:
        subscribers.discard(chat_id)
        save_subscribers()
        log.info(f"Unsubscribed: {chat_id}")


def calc_spread(fair_price: float, futures_price: float) -> float:
    if fair_price == 0:
        return 0.0
    return (futures_price - fair_price) / fair_price * 100


def spread_emoji(spread: float) -> str:
    return "🟢" if spread > 0 else "🔴"


def mexc_futures_link(symbol: str) -> str:
    return f"https://futures.mexc.com/exchange/{symbol}"


def format_price(p: float) -> str:
    if p >= 1:
        return f"{p:.4f}"
    elif p >= 0.001:
        return f"{p:.6f}"
    return f"{p:.8f}"


def format_size(vol: float) -> str:
    if vol >= 1_000_000:
        return f"{vol/1_000_000:.1f}M$"
    elif vol >= 1_000:
        return f"{vol/1_000:.1f}k$"
    return f"{vol:.0f}$"


async def fetch_json(session: aiohttp.ClientSession, url: str) -> Optional[dict | list]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                return await r.json()
    except Exception as e:
        log.warning(f"Fetch error {url}: {e}")
    return None


async def load_contract_details(session: aiohttp.ClientSession):
    global contract_info
    data = await fetch_json(session, MEXC_DETAIL)
    if not data or not data.get("success"):
        return
    for c in data.get("data", []):
        sym = c.get("symbol", "")
        contract_info[sym] = {
            "max_leverage":  c.get("maxLeverage", MAX_LEVERAGE),
            "max_vol":       c.get("maxVol", 0),
            "contract_size": c.get("contractSize", 1),
        }
    log.info(f"Loaded {len(contract_info)} contracts")


async def fetch_all_tickers(session: aiohttp.ClientSession) -> list[dict]:
    data = await fetch_json(session, MEXC_TICKER_ALL)
    if not data or not data.get("success"):
        return []
    return data.get("data", [])


async def check_orderbook_depth(session: aiohttp.ClientSession, symbol: str, min_size_usd: float = 10_000) -> bool:
    data = await fetch_json(session, MEXC_DEPTH + symbol)
    if not data or not data.get("success"):
        return False

    asks = data.get("data", {}).get("asks", [])
    bids = data.get("data", {}).get("bids", [])

    if not asks or not bids:
        return False

    ask_depth_usd = sum(float(row[0]) * float(row[1]) for row in asks[:5])
    bid_depth_usd = sum(float(row[0]) * float(row[1]) for row in bids[:5])

    return ask_depth_usd >= min_size_usd and bid_depth_usd >= min_size_usd


async def check_funding_rate(session: aiohttp.ClientSession, symbol: str) -> bool:
    data = await fetch_json(session, MEXC_FUNDING + symbol)
    if not data or not data.get("success"):
        return True

    funding = float(data.get("data", {}).get("fundingRate", 0) or 0)

    if abs(funding) > MAX_FUNDING_RATE:
        log.debug(f"[{symbol}] Аномальний funding: {funding:.4%} — пропускаємо")
        return False

    return True


def check_fair_futures_diff(fair_price: float, futures_price: float) -> bool:
    if futures_price == 0:
        return False
    diff = abs(fair_price - futures_price) / futures_price
    if diff > MAX_SPOT_FUTURES_DIFF:
        return False
    return True


def update_spread_history(symbol: str, spread: float) -> bool:
    now = time.time()

    if symbol not in spread_history:
        spread_history[symbol] = []

    spread_history[symbol].append((now, spread))

    cutoff = now - SPREAD_STABLE_WINDOW - 5
    spread_history[symbol] = [
        (t, s) for t, s in spread_history[symbol] if t >= cutoff
    ]

    recent = [(t, s) for t, s in spread_history[symbol] if t >= now - SPREAD_STABLE_WINDOW]
    if len(recent) < SPREAD_STABLE_COUNT:
        return False

    signs = [1 if s > 0 else -1 for _, s in recent]
    if len(set(signs)) > 1:
        return False

    return all(abs(s) >= SPREAD_THRESHOLD for _, s in recent)


def passes_filters(symbol: str, ticker: dict) -> tuple[bool, dict]:
    futures_price = float(ticker.get("lastPrice",  0) or 0)
    fair_price    = float(ticker.get("fairPrice",  0) or 0)
    volume24h_usd = float(ticker.get("volume24",   0) or 0)
    open_interest = float(ticker.get("holdVol",    0) or 0)

    if futures_price == 0 or fair_price == 0:
        return False, {}

    spread = calc_spread(fair_price, futures_price)
    if abs(spread) < SPREAD_THRESHOLD:
        return False, {}

    if volume24h_usd < MIN_VOLUME_24H:
        return False, {}

    cinfo       = contract_info.get(symbol, {})
    contract_sz = cinfo.get("contract_size", 1)
    oi_usd      = open_interest * futures_price * contract_sz
    if oi_usd < MIN_LIQUIDITY:
        return False, {}

    max_lev     = min(cinfo.get("max_leverage", MAX_LEVERAGE), MAX_LEVERAGE)
    raw_max_vol = cinfo.get("max_vol", 0)
    max_pos_usd = raw_max_vol * contract_sz * futures_price if raw_max_vol else 0

    return True, {
        "symbol":        symbol,
        "futures_price": futures_price,
        "fair_price":    fair_price,
        "spread":        spread,
        "volume24h_usd": volume24h_usd,
        "oi_usd":        oi_usd,
        "max_leverage":  max_lev,
        "max_pos_usd":   max_pos_usd,
    }


def build_message(info: dict) -> str:
    sym         = info["symbol"]
    sym_display = sym.replace("_", "")
    spread      = info["spread"]
    link        = mexc_futures_link(sym)
    circle      = spread_emoji(spread)

    fp  = format_price(info["futures_price"])
    sp  = format_price(info["fair_price"])
    lev = f'{int(info["max_leverage"])}x'
    siz = format_size(info["max_pos_usd"])
    spr = f'{spread:.2f}%'

    header = f'🚨 <a href="{link}"><b>{sym_display}</b></a> {circle} <b>{spread:.2f}%</b>'
    body = (
        f"Фьюч. ціна:  {fp}\n"
        f"Справедлива: {sp}\n"
        f"\n"
        f"Спред:       {spr}\n"
        f"Макс плечо:  {lev}\n"
        f"Макс сайз:   {siz}"
    )

    return f"{header}\n\n<code>{body}</code>"


async def broadcast(bot: Bot, info: dict):
    sym = info["symbol"]
    now = time.time()

    prev_time   = last_signal.get(sym, 0)
    prev_spread = last_spread.get(sym, 0)
    time_ok     = (now - prev_time) >= SIGNAL_COOLDOWN
    spread_grew = abs(info["spread"]) >= abs(prev_spread) + 1.0
    if not time_ok and not spread_grew:
        return
    if not subscribers:
        return

    msg = build_message(info)
    last_signal[sym]     = now
    last_spread[sym]     = info["spread"]
    current_signals[sym] = info

    dead = set()
    for chat_id in list(subscribers):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ("blocked", "chat not found", "deactivated", "forbidden")):
                log.info(f"Removing dead subscriber: {chat_id}")
                dead.add(chat_id)
            else:
                log.warning(f"Send error to {chat_id}: {e}")

    if dead:
        subscribers.difference_update(dead)
        save_subscribers()

    log.info(f"[{sym}] spread={info['spread']:.2f}% → sent to {len(subscribers)} users")


async def parser_loop(bot: Bot):
    async with aiohttp.ClientSession() as session:
        await load_contract_details(session)
        last_detail_load = time.time()

        while True:
            loop_start = time.time()

            if loop_start - last_detail_load > 3600:
                await load_contract_details(session)
                last_detail_load = loop_start

            tickers = await fetch_all_tickers(session)

            for ticker in tickers:
                symbol = ticker.get("symbol", "")
                fp     = float(ticker.get("lastPrice", 0) or 0)
                vol    = float(ticker.get("volume24",  0) or 0)

                if fp == 0 or vol < MIN_VOLUME_24H:
                    continue

                ok, info = passes_filters(symbol, ticker)
                if not ok:
                    continue

                if not check_fair_futures_diff(info["fair_price"], info["futures_price"]):
                    continue

                if not update_spread_history(symbol, info["spread"]):
                    continue

                if not await check_funding_rate(session, symbol):
                    continue

                if abs(info["spread"]) < SPREAD_DEPTH_MIN:
                    continue

                if not await check_orderbook_depth(session, symbol):
                    continue

                await broadcast(bot, info)

            elapsed = time.time() - loop_start
            await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))


async def health_check(request):
    return web.Response(text="OK — bot is alive")


async def start_web_server():
    app_web = web.Application()
    app_web.router.add_get("/", health_check)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    log.info("Web server started on port 8080")


async def main():
    load_subscribers()

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("stop",  cmd_stop))

    await app.initialize()
    await app.bot.delete_webhook(drop_pending_updates=True)
    await asyncio.sleep(3)

    await app.start()
    await app.updater.start_polling(
        drop_pending_updates=True,
        allowed_updates=["message"],
    )

    await start_web_server()
    log.info("Bot is running. Waiting for /start from users...")

    try:
        await parser_loop(app.bot)
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 Binance Spot Testnet Trading Bot - BTC/USDT (100% paper money)
================================================================================
A small, safe, ready-to-run trading bot for the Binance SPOT TESTNET.

STRATEGY
--------
Trend-following on 5m candles:
    * BUY  when EMA_fast (8) crosses ABOVE EMA_slow (21)  -> golden cross
    * SELL when EMA_fast crosses BELOW EMA_slow (21)      -> death cross
    * HOLD otherwise
    * Signals are read from *completed* candles only (SIGNAL_ON_CLOSED_CANDLE),
      so a crossover that appears mid-candle - and disappears before that
      candle closes ("repainting") - can no longer trigger an order.
ADX(14) trend filter: a new BUY (entry) is only allowed when ADX > 25, i.e. we
only trade a market that is genuinely trending and skip choppy side-ways moves
and fakeouts. The threshold is configurable via ADX_THRESHOLD.
RSI(14) is also computed and printed so you can keep an eye on
overbought / oversold conditions.

RISK MANAGEMENT (automatic TP / SL / trailing stop)
---------------------------------------------------
Every open position is protected by hard price limits applied to the entry:
    * Take-Profit  +2.5%  -> close the position when price rises to that level
    * Stop-Loss    -1.0%  -> close the position when price falls to that level
    * Trailing stop (TRAILING_STOP_PCT, e.g. 1.0%) -> while the price rises the
      stop is dragged up behind it, and the position closes when the price
      drops that far from its post-entry peak, locking in profit instead of
      handing the whole win back.
TP/SL/trailing are checked against the live price on every poll (not just once
per candle), and the opposite EMA cross still closes the position too. All
values are configurable via .env.
The bot prints the *net* reward/risk after fees and estimated slippage - a
+2.5% / -1.0% bracket is only ~+2.2% / -1.3% after two 0.1% taker fees, i.e.
an R:R of ~1.7, so one win pays for roughly one and a half losses.

DCA - AVERAGING DOWN INSTEAD OF AN IMMEDIATE STOP-OUT (optional)
----------------------------------------------------------------
With DCA_ENABLED=true a position that drops DCA_DROP_PERCENT (default 1.5%)
below its *current average* entry buys DCA_SIZE_QUOTE more of the same coin
instead of being stopped out on the spot. That lowers the average entry (and
with it the TP/SL levels) and gives the trade room to recover. Guard rails:
    * at most DCA_MAX_ENTRIES adds (default 2) and never twice on one candle,
    * never add once the price has already broken the hard stop-loss (the
      position is then closed as usual - no averaging into a broken trade),
    * never add past DCA_MAX_POSITION_QUOTE (per-coin exposure cap) or past the
      free USDT balance,
    * every add is lot/minNotional checked exactly like the first entry, so a
      small account can never send a dust order.
Take-profit and trailing-stop exits always win over DCA: profit is taken, and
losses are only averaged while the trade is still inside its stop.

RSI FILTER (optional, off by default)
-------------------------------------
Besides the ADX trend filter an entry can additionally be blocked by
RSI_MAX_ENTRY (skip a BUY when RSI is above it, e.g. 75 = do not buy into a
blow-off top) and RSI_MIN_ENTRY. 0 disables the check. RSI is always logged and
reported to Telegram.

COIN LIST / MULTI-COIN SCANNING
-------------------------------
SCANNER_SYMBOLS (e.g. "BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT" - plain
"BTCUSDT,BTCUSDT" style names are accepted too) makes the bot poll that list
one coin after another, every SCAN_POLL_SECONDS, and open a position in the
first coin that shows an active BUY setup (EMA golden cross + ADX trend filter
+ RSI filter), each with its own proportional budget and its own TP/SL/
trailing stop. Empty SCANNER_SYMBOLS = discover every liquid /USDT pair.

POSITION SIZING (a 50 USDT test balance)
----------------------------------------
The defaults are tuned for a 50.00 USDT testnet balance: ORDER_SIZE_QUOTE=10
spends 10 USDT per entry (five slots), the multi-coin scanner splits the
balance into PORTFOLIO_PARTS proportional slots that are floored to the
exchange minimum, and USE_ALL_BALANCE_PCT=80 keeps a ~10 USDT reserve for DCA
adds and fees.
A market order is only sent when the *rounded* order really passes the
exchange's MIN_NOTIONAL filter: the bot floors the quantity to the lot step,
bumps it up by one step when that flooring would fall below minNotional (only
when the balance can pay for it) and otherwise SKIPS the trade with an exact
"you need X USDT free" message instead of sending an order Binance rejects.
Extra risk gates protect a small account: a daily loss limit
(MAX_DAILY_LOSS_PCT), a pause after consecutive stop-outs
(MAX_CONSECUTIVE_LOSSES / LOSS_COOLDOWN_CANDLES) and a warning when the
stop-loss exit itself would drop below minNotional (dust risk). Exits are
NEVER blocked by any of these gates.

SAFETY
------
* The exchange is FORCED into sandbox/testnet mode on purpose. Every request
  goes to  https://testnet.binance.vision/  - real funds are never touched.
* DEMO_MODE=true runs a purely local simulation of the exact same loop so you
  can watch the live log output before you even have testnet API keys.
TELEGRAM NOTIFICATIONS (optional)
---------------------------------
Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to the .env file (setup guide in
README.md) and the bot will push a message when it starts, on every BUY/SELL
order and on critical errors / network problems. Trade messages are
self-contained: they carry the trade number, entry/exit price, quantity,
notional, the *reason* for the order (EMA death cross or which hard TP/SL/
trailing level was hit), the TP/SL levels, ADX/RSI at entry, holding time,
P/L in USDT + %, and the running session statistics, so the chat doubles as a
trade journal. A DCA add gets its own card: trigger price, amount added, the
old -> new average entry, the recomputed TP/SL, the remaining DCA budget and
the current unrealised P/L. Every message goes through an automatic retry
mechanism (a few quick attempts, then a background queue keeps re-sending),
so a temporary internet outage - connection timeout, DNS/NameResolutionError
- can neither crash the bot, nor block the trading loop, nor lose a message.

HOW TO GET TESTNET API KEYS
---------------------------
1. Log in (or create an account) on Binance and open the Spot Testnet:
       https://testnet.binance.vision/
2. Click "Generate HMAC SHA256 key" -> copy the API key and secret.
3. Click "Request testnet funds" to add fake BTC/USDT to your test wallet.
4. Copy ".env.example" to ".env", then fill in BINANCE_TESTNET_API_KEY and
   BINANCE_TESTNET_API_SECRET.  NEVER put real mainnet keys in this file.
5. Install the dependencies and run:
       pip install -r requirements.txt
       python trading_bot.py
   ...or set DEMO_MODE=true in the .env file to simulate without keys.
================================================================================
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import ccxt
import pandas as pd
import requests
from dotenv import dotenv_values, load_dotenv

# ------------------------------------------------------------------------------
# .env loading - the .env file is the SINGLE SOURCE OF TRUTH
# ------------------------------------------------------------------------------
# python-dotenv normally IGNORES variables that already exist in the process
# environment, so a stale DEMO_MODE=true, RUN_FOR_SECONDS or a dummy token left
# over in a terminal silently beat the real .env values - the classic reason a
# production bot drifts into test mode or stops itself. _load_env_file() makes
# .env win every time and records the conflicting keys, which main() reports
# at startup.
ENV_CONFLICTS: list = []   # keys whose shell value lost to .env (filled at import)


def _load_env_file() -> None:
    """Force-load .env over inherited shell variables; record the conflicts."""
    try:
        file_values = {key: val for key, val in (dotenv_values() or {}).items()
                       if val is not None}
    except Exception:  # missing/unreadable .env -> plain environment still works
        file_values = {}
    for key, file_val in file_values.items():
        env_val = os.environ.get(key)
        if env_val is not None and env_val.strip() != str(file_val).strip():
            ENV_CONFLICTS.append(key)
    load_dotenv(override=True)


def log_env_conflicts(log) -> None:
    """Report (and forget) the keys whose stale shell values lost to .env."""
    if ENV_CONFLICTS:
        log.warning(
            "[config] .env overrode stale shell variable(s): %s. .env is the "
            "single source of truth - close old terminals if this surprises you.",
            ", ".join(ENV_CONFLICTS),
        )
        ENV_CONFLICTS.clear()


_load_env_file()


# ------------------------------------------------------------------------------
# Configuration (every value can be overridden in the .env file)
# ------------------------------------------------------------------------------
def _parse_floats(raw: str) -> tuple:
    """Parse a comma-separated number list, e.g. "2,4" -> (2.0, 4.0)."""
    values = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                values.append(float(part))
            except ValueError:
                pass
    return tuple(values)


def _mask_secret(value: str) -> str:
    """Short preview of a secret (e.g. a token) that never shows it in full."""
    if not value:
        return "(empty)"
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def load_config() -> dict:
    """Collect all tunable settings from environment variables / .env file."""
    return {
        # --- Binance Spot Testnet credentials -------------------------------
        "api_key": os.getenv("BINANCE_TESTNET_API_KEY", "").strip(),
        "api_secret": os.getenv("BINANCE_TESTNET_API_SECRET", "").strip(),
        # --- Market & strategy ----------------------------------------------
        "symbol": os.getenv("SYMBOL", "BTC/USDT").strip(),
        # 5m frames are used: 5-minute candles (see README).
        "timeframe": os.getenv("TIMEFRAME", "5m").strip(),
        # Exponential Moving Average crossover (fast / slow), more responsive
        # than the old SMA(7,25). BUY on fast crossing above slow, SELL on the
        # opposite.
        "ema_fast": int(os.getenv("EMA_FAST_PERIOD", "8")),
        "ema_slow": int(os.getenv("EMA_SLOW_PERIOD", "21")),
        # RSI lookback period (printed for information, not used for signals).
        "rsi_period": int(os.getenv("RSI_PERIOD", "14")),
        # ADX (Average Directional Index) trend filter. New BUY entries are only
        # allowed when the market is trending (ADX > ADX_THRESHOLD). 25 instead
        # of the classic 20 keeps the bot out of weak trends and fakeouts.
        "adx_period": int(os.getenv("ADX_PERIOD", "14")),
        "adx_threshold": float(os.getenv("ADX_THRESHOLD", "25")),
        # Optional RSI entry filter on top of the ADX gate (0 = disabled).
        # RSI_MAX_ENTRY=75 refuses to chase a blow-off top; RSI_MIN_ENTRY skips
        # entries while the market is still deeply oversold. RSI is always
        # computed and reported - these only control whether it can block a BUY.
        "rsi_max_entry": float(os.getenv("RSI_MAX_ENTRY", "0")),
        "rsi_min_entry": float(os.getenv("RSI_MIN_ENTRY", "0")),
        # Risk management: hard Take-Profit / Stop-Loss percentages applied to
        # the entry price. A wider target (+2.5%) than stop (-1.0%) means one
        # winner covers roughly one and a half losers - the classic way to stay
        # profitable with a mediocre win rate.
        "take_profit_pct": float(os.getenv("TAKE_PROFIT_PCT", "2.5")),
        "stop_loss_pct": float(os.getenv("STOP_LOSS_PCT", "1.0")),
        # Estimated market-order slippage per side (%), used for the net
        # reward/risk report and the dust-risk warning. 0.05 = 5 bps.
        "slippage_pct": float(os.getenv("SLIPPAGE_PCT", "0.05")),
        # Extra headroom on top of minNotional (%). 0 = allow an order exactly
        # on the exchange minimum (needed for a 10 USDT account); raise it when
        # the account is bigger to avoid price-move rejections.
        "min_notional_buffer_pct": float(os.getenv("MIN_NOTIONAL_BUFFER_PCT", "0.0")),
        # When true, an entry is refused if the *stop-loss exit* would fall
        # below minNotional (Binance would reject that sell as dust). Default
        # false: the bot only warns, so a small account still trades.
        "require_exit_viable": os.getenv("REQUIRE_EXIT_VIABLE", "false").strip().lower() == "true",
        # Signal only from COMPLETED candles (no repainting / fakeout entries).
        "signal_on_closed_candle": os.getenv("SIGNAL_ON_CLOSED_CANDLE", "true").strip().lower() != "false",
        # --- Account protection (small-balance risk gates) -------------------
        # Stop opening new positions after this much realised loss in one UTC
        # day, in % of the day's starting equity (0 = disabled). Exits stay on.
        "max_daily_loss_pct": max(0.0, float(os.getenv("MAX_DAILY_LOSS_PCT", "3.0"))),
        # Pause new entries after N losing trades in a row (0 = disabled).
        "max_consecutive_losses": max(0, int(os.getenv("MAX_CONSECUTIVE_LOSSES", "3"))),
        # ...and stay flat for this many candles after a STOP-LOSS exit so the
        # bot does not immediately re-enter the same chop (0 = disabled).
        "loss_cooldown_candles": max(0, int(os.getenv("LOSS_COOLDOWN_CANDLES", "2"))),
        # Sell only the quantity the bot actually bought (keeps a pre-existing
        # testnet/BTC balance untouched). false = legacy "sell all free base".
        "sell_only_tracked_qty": os.getenv("SELL_ONLY_TRACKED_QTY", "true").strip().lower() != "false",
        # Quote amount spent on each market BUY. On the 50 USDT test balance 10
        # leaves five slots plus room for DCA adds; the multi-coin scanner
        # derives its own per-coin budget from PORTFOLIO_PARTS instead.
        "order_size_quote": float(os.getenv("ORDER_SIZE_QUOTE", "10")),  # USDT per BUY
        # Spot trading fee (0.1% taker on Binance) simulated in DEMO mode so a
        # 10 USDT balance behaves realistically (fees shrink the position).
        "fee_rate": float(os.getenv("FEE_RATE", "0.001")),
        # Starting balance in DEMO / mock mode. 50.00 USDT mirrors the test
        # balance this bot is tuned for: five 10 USDT slots, each comfortably
        # above Binance's minNotional (~5..10 USDT depending on the pair).
        "initial_balance_usdt": float(os.getenv("INITIAL_BALANCE_USDT", "50.0")),
        "candle_limit": int(os.getenv("CANDLE_LIMIT", "150")),
        # --- Telegram notifications (optional, see README.md) ----------------
        "telegram_bot_token": os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
        "telegram_chat_id": os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        # Telegram robustness: quick inline retries, then a background queue
        # keeps re-sending failed messages while the trading loop carries on.
        "telegram_max_attempts": int(os.getenv("TELEGRAM_MAX_ATTEMPTS", "3")),
        "telegram_retry_delays": _parse_floats(os.getenv("TELEGRAM_RETRY_DELAYS", "2,4")),
        "telegram_queue_retry_seconds": float(os.getenv("TELEGRAM_QUEUE_RETRY_SECONDS", "30")),
        "telegram_queue_max_age": float(os.getenv("TELEGRAM_QUEUE_MAX_AGE", "1800")),
        # --- Loop behaviour --------------------------------------------------
        "poll_seconds": int(os.getenv("POLL_SECONDS", "30")),
        "demo_mode": os.getenv("DEMO_MODE", "false").strip().lower() == "true",
        # TEST-ONLY auto-stop (seconds). Honoured in DEMO_MODE only; ignored in
        # production so a 24/7 bot can never shut itself down (0 = off).
        "run_for_seconds": max(0, int(os.getenv("RUN_FOR_SECONDS", "0"))),
        "log_level": os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        # --- Multi-coin scanner (async, dynamic symbol discovery) --------------
        # Master switch: when true the bot becomes a portfolio scanner instead of
        # trading a single SYMBOL. It discovers every active spot pair quoted in
        # SCAN_QUOTE (default USDT) on the exchange, filters/caps the universe and
        # trades each pair with a *proportional* slice of the free USDT balance.
        "scan_enabled": os.getenv("SCAN_ENABLED", "false").strip().lower() == "true",
        # Quote currency all discovered pairs share (e.g. USDT).
        "scan_quote": os.getenv("SCAN_QUOTE", "USDT").strip().upper() or "USDT",
        # How many parts the free balance is split into - the reference video
        # divides the balance into 21 slots. Each part becomes the per-pair budget
        # (auto-shrunk so a part can still clear minNotional on a small account).
        "portfolio_parts": max(1, int(os.getenv("PORTFOLIO_PARTS", "21"))),
        # Absolute floor (USDT) per budget slot so a $10 account still respects
        # Binance's minNotional instead of trying to trade a few cents per pair.
        "portfolio_floor_usdt": float(os.getenv("PORTFOLIO_FLOOR_USDT", "0")),
        # Only scan spot pairs that traded at least this much quote in 24h -- a
        # cheap "high-volume" filter that also skips illiquid garbage pairs.
        "scan_min_24h_quote": float(os.getenv("SCAN_MIN_24H_QUOTE", "100000")),
        # Hard cap on the number of pairs scanned/traded per cycle (0 = no cap).
        "scan_max_symbols": int(os.getenv("SCAN_MAX_SYMBOLS", "30")),
        # Comma-separated symbols never to touch (e.g. "SHIBUSDT,DOGEUSDT").
        "scan_exclude": [s.strip().upper() for s in
                         os.getenv("SCAN_EXCLUDE", "").split(",") if s.strip()],
        # Optional explicit universe: "SYM1/USDT,SYM2/USDT". When set it wins over
        # the automatic high-volume discovery (empty = discover dynamically).
        "scanner_symbols": os.getenv("SCANNER_SYMBOLS", "").strip(),
        # Trailing stop: while the price rises the hard stop-loss is dragged up
        # behind it, locking in profit, and the position closes when price drops
        # this many % from its post-entry peak. 0 = classic fixed stop only.
        "trailing_stop_pct": float(os.getenv("TRAILING_STOP_PCT", "0")),
        # The trailing stop only *arms* once the post-entry peak climbed this
        # many % above the (current average) entry, so on a straight decline the
        # DCA / fixed stop-loss rules handle the trade instead of a 1% "trail"
        # exit. No effect while TRAILING_STOP_PCT=0.
        "trailing_arm_pct": max(0.0, float(os.getenv("TRAILING_ARM_PCT", "0.5"))),
        # --- DCA / averaging down ------------------------------------------
        # Instead of being stopped out on the spot, a position that falls
        # DCA_DROP_PERCENT below its current average entry buys
        # DCA_SIZE_QUOTE more of the same coin (0 -> ORDER_SIZE_QUOTE), which
        # lowers the average entry and the TP/SL levels attached to it.
        "dca_enabled": os.getenv("DCA_ENABLED", "false").strip().lower() == "true",
        "dca_drop_pct": max(0.0, float(os.getenv("DCA_DROP_PERCENT", "1.5"))),
        # Quote amount per DCA add. 0 = reuse ORDER_SIZE_QUOTE.
        "dca_size_quote": max(0.0, float(os.getenv("DCA_SIZE_QUOTE", "0"))),
        # How many adds a single position may receive (0 = DCA disabled).
        "dca_max_entries": max(0, int(os.getenv("DCA_MAX_ENTRIES", "2"))),
        # Hard cap on the total quote amount a single position may hold after
        # averaging down (0 = only limited by the free balance).
        "dca_max_position_quote": max(0.0, float(os.getenv("DCA_MAX_POSITION_QUOTE", "0"))),
        # Binance BNB-fee-discount (0..25%). Holding BNB cuts the taker fee by up
        # to 25%; this is reflected in the effective fee used for sizing & R:R.
        "bnb_fee_discount_pct": float(os.getenv("BNB_FEE_DISCOUNT_PCT", "0")),
        # Max share of the free USDT balance that may be deployed across the whole
        # portfolio at once (%), leaving a reserve for fees and future entries.
        "use_all_balance_pct": float(os.getenv("USE_ALL_BALANCE_PCT", "100")),
        # Poll interval (seconds) between scanner cycles (independent of POLL_SECONDS).
        "scan_poll_seconds": int(os.getenv("SCAN_POLL_SECONDS", "60")),
    }


def setup_logging(level: str) -> None:
    """Human-friendly terminal logging."""
    try:  # keep emoji log lines intact even when stdout is redirected to a file
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # cosmetic only - never block startup over it
        pass
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


# ------------------------------------------------------------------------------
# Telegram notifications (optional - a Telegram failure can never crash the bot)
# ------------------------------------------------------------------------------
class TelegramNotifier:
    """
    Sends messages to a Telegram chat through the official Bot API, with a
    built-in retry mechanism so a flaky internet connection can neither crash
    the bot, nor block the trading loop, nor silently lose a notification:

        1. `send()` tries up to `max_attempts` times, waiting
           `retry_delays[i]` seconds between attempts (quick inline retries
           that absorb short hiccups such as a DNS failure or a dropped
           connection).
        2. If every inline attempt fails (connection timeout,
           NameResolutionError, network fully down, ...), the message is put
           into a background retry queue and a daemon thread keeps re-sending
           it every `queue_retry_seconds` - while the trading loop carries on
           immediately, never waiting for the network to come back.
        3. The queue is mirrored to a small disk spool file
           (`telegram_retry_spool.jsonl` next to this script) and re-loaded at
           startup, so even a crash or restart in the middle of an outage
           cannot lose a message. Queued messages are NEVER dropped: they are
           retried until Telegram accepts them. Delivery is at-least-once - a
           message may very rarely be duplicated (if the process dies right
           after a successful send), but it is never lost.

    SETUP (full guide in README.md):
        1. In Telegram, talk to @BotFather -> /newbot -> copy the bot token.
        2. IMPORTANT: open your new bot's chat and press START (or send it any
           message) - Telegram forbids bots from messaging you first.
        3. Put the token and your chat id into the .env file:
               TELEGRAM_BOT_TOKEN=123456789:AA...
               TELEGRAM_CHAT_ID=123456789
           (the retry behaviour is tunable - see .env.example)

    SAFETY: every HTTP attempt is wrapped in its own try/except and the
    background worker is exception-proof as well - an unexpected bug is
    logged and the queue is simply retried on the next cycle. A missing
    token simply disables the notifier, and a Telegram outage only produces
    log lines - the trading loop is never affected and can NEVER crash
    because of it.
    """

    API_URL = "https://api.telegram.org/bot{token}/sendMessage"
    ME_URL = "https://api.telegram.org/bot{token}/getMe"

    # Telegram answered but permanently refused the message (bad token, the
    # chat never pressed START, wrong chat id, text too long, ...). Retrying
    # the same payload can never succeed, so these are never retried.
    NO_RETRY_HTTP = frozenset({400, 401, 403, 404})

    def __init__(self, token: str, chat_id: str, log, max_attempts: int = 3,
                 retry_delays=(2.0, 4.0), queue_retry_seconds: float = 30.0,
                 queue_max_age: float = 1800.0, background_retry: bool = True,
                 spool_path=None) -> None:
        self.log = log
        self.enabled = bool(token and chat_id)
        self.token = token
        self.chat_id = chat_id
        self.timeout = 10          # seconds per HTTP request
        self.max_attempts = max(1, int(max_attempts))
        safe_delays = []
        for d in retry_delays:
            try:
                d = float(d)
            except (TypeError, ValueError):
                continue
            if d > 0:
                safe_delays.append(d)
        self.retry_delays = tuple(safe_delays) or (2.0,)
        self.queue_retry_seconds = max(1.0, float(queue_retry_seconds))
        # KEPT for compatibility: queued messages are never dropped, so this
        # age only triggers a one-time "older than configured" warning.
        self.queue_max_age = max(0.0, float(queue_max_age))

        # Disk spool: every message that cannot be sent right now is appended
        # to this file (default: next to this script) and re-loaded at
        # startup, so even a crash or restart during an outage loses nothing.
        self.spool_path = spool_path or os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "telegram_retry_spool.jsonl",
        )

        # Background retry queue: messages that could not be sent right now
        # wait here and are re-sent by the daemon thread below, completely
        # detached from (and invisible to) the trading loop.
        self._queue = []                  # FIFO - oldest message first
        self._queue_lock = threading.Lock()
        self._wakeup = threading.Event()  # a new enqueue interrupts the sleep
        if self.enabled:
            with self._queue_lock:
                recovered = self._spool_recover_locked()
            if background_retry:
                threading.Thread(
                    target=self._retry_worker, name="telegram-retry", daemon=True,
                ).start()
            if recovered:
                log.info(
                    "[telegram] recovered %d unsent message(s) from the "
                    "previous session (disk spool) - they will be delivered "
                    "as soon as Telegram is reachable.", recovered,
                )
            log.info(
                "Telegram notifications: ENABLED (chat_id=%s, token=%s, up to %d "
                "quick attempt(s) per send%s).",
                chat_id, _mask_secret(token), self.max_attempts,
                ", background retry queue active" if background_retry else "",
            )
        else:
            missing = [name for name, val in
                       (("TELEGRAM_BOT_TOKEN", token), ("TELEGRAM_CHAT_ID", chat_id))
                       if not val]
            log.warning(
                "Telegram notifications: DISABLED - %s missing/empty. No messages "
                "will be sent (fill it in .env and restart the bot to enable).",
                " and ".join(missing) or "settings",
            )

    def pending_count(self) -> int:
        """Number of messages currently waiting in the background retry queue."""
        with self._queue_lock:
            return len(self._queue)

    def verify(self) -> bool:
        """
        One-time startup check: ask Telegram who this token belongs to (getMe).

        Catches a wrong/revoked TELEGRAM_BOT_TOKEN right at startup with a
        clear ERROR log, instead of failing quietly on the first order.
        Never raises; returns True only when Telegram confirms the token.
        """
        if not self.enabled:
            return False
        try:
            resp = requests.get(self.ME_URL.format(token=self.token), timeout=5)
            if resp.status_code == 200 and bool(resp.json().get("ok")):
                username = (resp.json().get("result") or {}).get("username")
                self.log.info(
                    "[telegram] token VERIFIED with Telegram (bot @%s) - messages "
                    "will be delivered to chat_id=%s.",
                    username or "unknown", self.chat_id,
                )
                return True
            if resp.status_code in (401, 404):
                self.log.error(
                    "[telegram] TELEGRAM_BOT_TOKEN is INVALID (HTTP %s) - NO "
                    "messages will be delivered! Re-create the token with "
                    "@BotFather, update .env and restart the bot.",
                    resp.status_code,
                )
            else:
                self.log.warning(
                    "[telegram] token check got an unexpected answer (HTTP %s): %s",
                    resp.status_code, str(getattr(resp, "text", ""))[:120],
                )
        except (requests.Timeout, requests.ConnectionError) as err:
            self.log.warning(
                "[telegram] token check skipped (network problem: %s) - delivery "
                "will be retried on the first message.", err,
            )
        except Exception as err:  # a broken check must never crash the bot
            self.log.warning("[telegram] token check failed unexpectedly: %s", err)
        return False

    # -- public API ---------------------------------------------------------
    def send(self, text: str) -> bool:
        """
        Send one message with automatic retries. Telegram/network problems
        NEVER propagate and the caller is never blocked for long (only the
        few quick inline attempts; a Ctrl+C still passes through, but only
        after the message has been parked safely on disk).

        Returns True once the message reached Telegram, False when it was
        permanently rejected by Telegram or handed over to the background
        retry queue - where it is kept (memory + disk) and retried until it
        is finally delivered, even across a bot restart.
        """
        if not self.enabled:
            return False  # not configured -> silently do nothing

        queued = False
        try:
            last_err = "unknown error"
            for attempt in range(1, self.max_attempts + 1):
                sent, retryable, err = self._attempt(text)
                if sent:
                    first_line = text.splitlines()[0][:60] if text else ""
                    self.log.info("[telegram] delivered: %s", first_line)
                    if attempt > 1:
                        self.log.info(
                            "[telegram] (it needed %d of %d quick attempts.)",
                            attempt, self.max_attempts,
                        )
                    return True
                if not retryable:
                    # permanent rejection - retrying cannot help (the log
                    # line in _attempt names the exact .env value to fix)
                    return False
                last_err = err
                if attempt < self.max_attempts:
                    delay = self.retry_delays[min(attempt - 1, len(self.retry_delays) - 1)]
                    self.log.warning(
                        "[telegram] attempt %d/%d failed (%s) - retrying in %.0fs ...",
                        attempt, self.max_attempts, err, delay,
                    )
                    time.sleep(delay)

            # Every inline attempt failed (timeout / NameResolutionError /
            # network down ...). Park the message in the background queue
            # (memory + disk) and return at once, so the trading loop is not
            # delayed by the outage any further. The background worker keeps
            # retrying until Telegram accepts the message.
            self._enqueue(text)
            queued = True
            self.log.error(
                "[telegram] giving up after %d attempt(s) (%s) - the message "
                "is safely queued (memory + disk) and WILL be re-sent in the "
                "background every %.0fs until it is delivered; trading "
                "continues unaffected.",
                self.max_attempts, last_err, self.queue_retry_seconds,
            )
            return False
        except BaseException:
            # Even a Ctrl+C in the middle of the retry loop must not lose the
            # message: park it on disk first, then let the interrupt through.
            if not queued:
                self._enqueue(text)
            raise

    # -- internals ------------------------------------------------------------
    def _attempt(self, text: str) -> tuple:
        """
        One HTTP attempt. Returns (sent, retryable, error_description).

        Catches EVERY exception, so it can never raise into the trading code:
        timeouts, DNS failures, connection resets, rate limits and even
        unexpected bugs all come back as plain booleans plus a description.
        """
        try:
            resp = requests.post(
                self.API_URL.format(token=self.token),
                json={"chat_id": self.chat_id, "text": text},
                timeout=self.timeout,
            )
            if resp.status_code == 200 and bool(resp.json().get("ok")):
                return True, False, ""
            if resp.status_code in self.NO_RETRY_HTTP:
                # Telegram answered but rejected the message permanently.
                # 401/404 -> wrong bot token; 400/403 -> wrong chat id, the
                # START button was never pressed, or the bot was blocked.
                hint = ("check TELEGRAM_BOT_TOKEN in .env"
                        if resp.status_code in (401, 404) else
                        "check TELEGRAM_CHAT_ID in .env (chat not found / START "
                        "not pressed / bot blocked)")
                self.log.warning(
                    "[telegram] message rejected (HTTP %s) - not retrying (%s): %s",
                    resp.status_code, hint, str(getattr(resp, "text", ""))[:200],
                )
                return False, False, f"HTTP {resp.status_code}"
            # Temporary problems: 5xx server errors and 429 rate limiting are
            # both worth another try.
            err = f"HTTP {resp.status_code}: {str(getattr(resp, 'text', ''))[:200]}"
            if resp.status_code == 429:
                err += " (rate limited - Telegram asks us to slow down)"
            return False, True, err
        except (requests.Timeout, requests.ConnectionError) as err:
            # The classic temporary network problems: connection timeout,
            # DNS/NameResolutionError, connection reset, ... -> retry.
            return False, True, f"{type(err).__name__}: {err}"
        except requests.RequestException as err:
            # Any other requests-level problem is worth another try too.
            return False, True, f"{type(err).__name__}: {err}"
        except Exception as err:  # last resort - still never crash the bot
            return False, True, f"unexpected {type(err).__name__}: {err}"

    def _enqueue(self, text: str) -> None:
        """
        Park a failed message for background delivery: pushed into the
        in-memory queue AND appended to the disk spool in one atomic step
        (under the queue lock), so no crash window can lose it.
        """
        item = {"text": text, "queued_at": time.time(), "retries": 0}
        with self._queue_lock:
            self._queue.append(item)
            self._spool_append_locked(item)
        self._wakeup.set()  # let the worker start retrying right away

    def _spool_append_locked(self, item: dict) -> None:
        """Append one queued message to the spool file. Caller holds the lock."""
        try:
            with open(self.spool_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        except Exception as err:
            self.log.error(
                "[telegram] could not persist a queued message to %s: %s "
                "(it is still kept in memory and will be retried)",
                self.spool_path, err,
            )

    def _retry_worker(self) -> None:
        """
        Background daemon: retries queued messages every `queue_retry_seconds`
        until Telegram accepts them. Nothing is ever dropped: network outages,
        timeouts, 5xx answers and rate limits are all retried indefinitely, the
        whole cycle is exception-proof (an unexpected bug is logged and the
        queue is simply retried on the next cycle), and the disk spool is
        re-synced after every cycle so a crash or restart during an outage
        loses nothing.
        """
        while True:
            self._wakeup.wait(self.queue_retry_seconds)
            self._wakeup.clear()
            if not self._queue:
                continue
            try:
                self._run_retry_cycle()
            except Exception as err:  # a bug must never stop the retries
                self.log.error(
                    "[telegram] background retry cycle aborted by an unexpected "
                    "error (%s) - the queue is untouched and will be retried in "
                    "the next cycle.", err,
                )

    def _run_retry_cycle(self) -> None:
        """One background pass: every queued message is tried exactly once."""
        still_failing = []
        while True:  # every queued item is tried at most once per cycle
            with self._queue_lock:
                item = self._queue.pop(0) if self._queue else None
            if item is None:
                break
            sent, retryable, err = self._attempt(item["text"])
            if sent:
                self.log.info(
                    "[telegram] QUEUED message delivered after %d background "
                    "retry(ies): %s",
                    item["retries"],
                    item["text"].splitlines()[0][:60] if item["text"] else "",
                )
                continue
            if not retryable:
                # Telegram permanently rejected it - the log line from
                # _attempt names the exact .env value to fix. Retrying the
                # same payload can never succeed, so it leaves the queue.
                self.log.error(
                    "[telegram] queued message permanently rejected - removed "
                    "from the queue (the hint above names the fix): %s",
                    item["text"].splitlines()[0][:60] if item["text"] else "",
                )
                continue
            item["retries"] += 1
            age = time.time() - item["queued_at"]
            if (self.queue_max_age and age >= self.queue_max_age
                    and not item.get("age_warned")):
                item["age_warned"] = True
                self.log.error(
                    "[telegram] a queued message is older than %.0fs "
                    "(TELEGRAM_QUEUE_MAX_AGE) - it is KEPT and will keep "
                    "retrying: background messages are never dropped.",
                    self.queue_max_age,
                )
            still_failing.append(item)
            self.log.warning(
                "[telegram] background retry %d failed (%s) - trying again "
                "in %.0fs; trading loop unaffected.",
                item["retries"], err, self.queue_retry_seconds,
            )
        with self._queue_lock:
            self._queue.extend(still_failing)
            self._spool_rewrite_locked()  # delivered ones leave the disk too

    def _spool_rewrite_locked(self) -> None:
        """
        Replace the spool file with the current queue contents (atomic write).
        Caller holds _queue_lock. Called after every background cycle, so
        delivered messages disappear from disk and only undelivered ones stay.
        """
        try:
            tmp = self.spool_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                for queued_item in self._queue:
                    fh.write(json.dumps(queued_item, ensure_ascii=False) + "\n")
            os.replace(tmp, self.spool_path)
        except Exception as err:
            self.log.error(
                "[telegram] could not update the spool file %s: %s "
                "(the in-memory queue is unaffected and keeps retrying)",
                self.spool_path, err,
            )

    def _spool_recover_locked(self) -> int:
        """
        Load messages saved by a previous session into the queue. Caller holds
        _queue_lock. Returns how many messages were recovered.
        """
        try:
            with open(self.spool_path, "r", encoding="utf-8") as fh:
                lines = fh.readlines()
        except FileNotFoundError:
            return 0
        except Exception as err:
            self.log.error(
                "[telegram] could not read the spool file %s: %s",
                self.spool_path, err,
            )
            return 0
        parsed = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except ValueError:
                self.log.error(
                    "[telegram] spool line is corrupt (torn write?) - skipped: "
                    "%.80s", line,
                )
                continue
            if not isinstance(entry, dict) or "text" not in entry:
                self.log.error(
                    "[telegram] spool line has a bad shape - skipped: %.80s", line,
                )
                continue
            entry.setdefault("queued_at", time.time())
            entry.setdefault("retries", 0)
            parsed.append(entry)
        try:  # rewrite the file clean - drops only corrupt/partial lines
            tmp = self.spool_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                for entry in parsed:
                    fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
            os.replace(tmp, self.spool_path)
        except Exception as err:
            self.log.error("[telegram] could not clean the spool file: %s", err)
        self._queue.extend(parsed)
        return len(parsed)


# ------------------------------------------------------------------------------
# Technical indicators
# ------------------------------------------------------------------------------
def _wilder_smooth(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing (as used for RSI / ATR / DX) over `period` values."""
    return series.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()


def _compute_adx(df: pd.DataFrame, period: int) -> pd.Series:
    """
    Average Directional Index (ADX) with period `period` using Wilder's method.

    ADX quantifies how strongly a market is trending (0 = flat / range-bound,
    100 = strongly trending). We use it as an entry filter: only BUY when the
    market is actually trending (ADX > ADX_THRESHOLD). A value of NaN on the
    first candles simply means there is not enough history yet.
    """
    period = max(2, int(period))

    # True Range.
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    # Directional movement.
    up_move = df["high"].diff()     # current high - previous high
    down_move = -df["low"].diff()   # previous low - current low
    plus_dm = pd.Series(up_move.where((up_move > down_move) & (up_move > 0.0), 0.0),
                        index=df.index)
    minus_dm = pd.Series(down_move.where((down_move > up_move) & (down_move > 0.0), 0.0),
                         index=df.index)

    # Smoothed +DM / -DM and ATR -> Directional Indicators.
    atr = _wilder_smooth(tr, period)
    plus_di = 100.0 * _wilder_smooth(plus_dm, period) / atr
    minus_di = 100.0 * _wilder_smooth(minus_dm, period) / atr

    # DX, then ADX = smoothed average of DX.
    di_sum = plus_di + minus_di
    dx = (100.0 * (plus_di - minus_di).abs() / di_sum).where(di_sum > 0.0)
    return _wilder_smooth(dx, period)


def compute_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Add EMA fast/slow, Wilder's RSI and ADX columns to the candle DataFrame."""
    df = df.copy()
    # Exponential Moving Averages of the closing price (more responsive to the
    # latest move than the old simple SMA(7,25)).
    df["ema_fast"] = df["close"].ewm(span=cfg["ema_fast"], adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=cfg["ema_slow"], adjust=False).mean()

    # RSI (14) using Wilder's smoothing.
    delta = df["close"].diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = _wilder_smooth(gain, cfg["rsi_period"])
    avg_loss = _wilder_smooth(loss, cfg["rsi_period"])
    rs = avg_gain / avg_loss
    df["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    # ADX trend-strength filter.
    df["adx"] = _compute_adx(df, cfg["adx_period"])
    return df


def round_trip_cost(fee_rate: float, slippage_pct: float) -> float:
    """
    Total round-trip trading cost as a fraction of the position (buy + sell).

    Two taker fees (entry and exit) plus an estimated market-order slippage
    per side. 0.1% fee + 0.05% slippage -> 0.003 = 0.3% - i.e. a trade has to
    move ~0.3% just to break even.
    """
    fee = max(0.0, float(fee_rate or 0.0))
    slip = max(0.0, float(slippage_pct or 0.0)) / 100.0
    return 2.0 * fee + 2.0 * slip


def reward_risk_after_costs(tp_pct: float, sl_pct: float, fee_rate: float,
                            slippage_pct: float) -> dict:
    """
    Turn the raw TP/SL brackets into the numbers a trader actually cares about.

    Returns a dict with the net win / loss percentages (after costs), the
    reward:risk ratio and the break-even win rate the strategy must beat.
    Pure function - used for the startup report, the log line, the Telegram
    trade cards and the smoke test.
    """
    cost = round_trip_cost(fee_rate, slippage_pct)
    net_win = max(0.0, float(tp_pct)) / 100.0 - cost
    net_loss = max(0.0, float(sl_pct)) / 100.0 + cost
    rr = (net_win / net_loss) if net_loss > 0 else float("inf")
    break_even = (100.0 / (1.0 + rr)) if rr > 0 and math.isfinite(rr) else 100.0
    return {
        "cost_pct": cost * 100.0,
        "net_win_pct": net_win * 100.0,
        "net_loss_pct": net_loss * 100.0,
        "rr": rr,
        "break_even_win_rate": break_even,
    }


def rsi_entry_block(rsi_now, rsi_max: float = 0.0, rsi_min: float = 0.0) -> str:
    """
    Explain why RSI forbids a new entry, or "" when RSI is fine (or disabled).

    `rsi_max` refuses to chase a blow-off top (RSI above it), `rsi_min` refuses
    to enter while the market is still deeply oversold. 0 turns a bound off, so
    the default configuration behaves exactly like the RSI-less strategy.
    """
    try:
        if rsi_now is None or pd.isna(rsi_now):
            return ""
        value = float(rsi_now)
    except (TypeError, ValueError):
        return ""
    if rsi_max and rsi_max > 0 and value > float(rsi_max):
        return (f"RSI={value:.1f} > RSI_MAX_ENTRY={float(rsi_max):g} "
                f"(too overbought to start a trade)")
    if rsi_min and rsi_min > 0 and value < float(rsi_min):
        return (f"RSI={value:.1f} < RSI_MIN_ENTRY={float(rsi_min):g} "
                f"(too oversold to start a trade)")
    return ""


def current_signal(df: pd.DataFrame, adx_threshold: float = 25.0,
                   closed_only: bool = False, rsi_max: float = 0.0,
                   rsi_min: float = 0.0) -> tuple:
    """
    Detect an EMA crossover using the last two *completed* candles.

    A BUY (new entry) is only allowed when the market is trending: ADX must be
    above `adx_threshold` (> 25 by default) so we skip choppy / side-ways moves
    and fakeout crosses, and the optional RSI bounds (`rsi_max` / `rsi_min`,
    0 = off) must not be violated either. A SELL (position exit) is never gated
    by ADX or RSI - protecting the position is always allowed, regardless of
    trend strength.

    `closed_only=True` ignores the newest candle (which is usually still
    forming while the bot polls) and reads the two last completed candles
    instead, so a signal can no longer appear and vanish inside one candle.

    Returns a tuple (signal, rsi_now, adx_now, reason):
        signal = "BUY", "SELL" or "HOLD"
    """
    if len(df) < 2:
        return "HOLD", float("nan"), float("nan"), "not enough candles yet"

    view = df.iloc[:-1] if closed_only and len(df) > 2 else df
    if len(view) < 2:
        return "HOLD", float("nan"), float("nan"), "not enough candles yet"

    last_fast, last_slow = view["ema_fast"].iloc[-1], view["ema_slow"].iloc[-1]
    prev_fast, prev_slow = view["ema_fast"].iloc[-2], view["ema_slow"].iloc[-2]
    rsi_now = view["rsi"].iloc[-1]
    adx_now = view["adx"].iloc[-1]

    # Indicators may still be NaN on the first candles of a session.
    if any(pd.isna(v) for v in (last_fast, last_slow, prev_fast, prev_slow)):
        return "HOLD", rsi_now, adx_now, "indicators warming up"

    if prev_fast <= prev_slow and last_fast > last_slow:
        # Golden cross. Only a trending market qualifies as an entry.
        if pd.isna(adx_now) or adx_now <= adx_threshold:
            return ("HOLD", rsi_now, adx_now,
                    f"golden cross but ADX={adx_now:.1f} <= {adx_threshold:.0f} "
                    f"(no trend - entry filtered out)")
        blocked = rsi_entry_block(rsi_now, rsi_max, rsi_min)
        if blocked:
            return "HOLD", rsi_now, adx_now, f"golden cross but {blocked}"
        return "BUY", rsi_now, adx_now, "EMA fast crossed ABOVE EMA slow (ADX trend OK)"
    if prev_fast >= prev_slow and last_fast < last_slow:
        return "SELL", rsi_now, adx_now, "EMA fast crossed BELOW EMA slow"
    return "HOLD", rsi_now, adx_now, "no crossover"


# ------------------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------------------
def floor_to_step(value: float, step: float) -> float:
    """Round *down* a quantity to the exchange's lot step size."""
    if not step or step <= 0:
        step = 1e-8
    return math.floor(value / step) * step


def symbol_rules(exchange, symbol: str):
    """
    Extract minQty / minNotional / stepSize for a symbol from ccxt market data.
    Used both for correct order sizing and for healthy log messages.
    """
    market = exchange.market(symbol)
    limits = market.get("limits") or {}
    min_qty = float((limits.get("amount") or {}).get("min") or 0.0)
    min_cost = float((limits.get("cost") or {}).get("min") or 0.0)
    prec = (market.get("precision") or {}).get("amount", 8)
    try:
        step = float(prec)
        if step >= 1:  # precision expressed as "number of decimals"
            step = 10.0 ** (-int(round(step)))
    except (TypeError, ValueError):
        step = 1e-8
    if not step or step <= 0:
        step = 1e-8
    return min_qty, min_cost, step


def min_viable_balance(min_cost: float, step: float, price: float,
                       fee_rate: float = 0.0, sl_pct: float = 0.0,
                       slippage_pct: float = 0.0) -> float:
    """
    Smallest free quote balance that still allows a *safe* trade on this pair.

    Both the entry AND the stop-loss exit have to stay above the exchange's
    minNotional: the position loses (fees + the stop distance) before it is
    closed, and the lot step means the entry can be one whole step smaller than
    the requested budget. For BTC/USDT with minNotional 10 USDT, 0.1% fees and
    a -1.0% stop this is ~10.12 USDT - i.e. a 10.00 USDT balance is *below*
    the line, exactly what a small account must know before it trades.
    """
    if not min_cost or min_cost <= 0:
        return 0.0
    exit_factor = ((1.0 - max(0.0, fee_rate))
                   * (1.0 - (max(0.0, sl_pct) + max(0.0, slippage_pct)) / 100.0))
    need = min_cost / exit_factor if exit_factor > 0 else float("inf")
    return max(need, min_cost) + max(0.0, step) * max(0.0, price)


def run_deadline(cfg: dict, log) -> float:
    """
    Shared RUN_FOR_SECONDS handling for every loop (single bot + scanner).

    RUN_FOR_SECONDS is a TEST-ONLY convenience: in DEMO_MODE the loop may
    stop itself after N seconds. In production mode the limit is IGNORED -
    the bot runs 24/7 and only the operator can stop it (Ctrl+C).

    Returns a Unix-timestamp deadline the loop must check, or None when the
    loop must not stop on its own.
    """
    seconds = int(cfg.get("run_for_seconds", 0) or 0)
    if cfg.get("demo_mode") and seconds > 0:
        # TEST-ONLY timer: a local DEMO run may stop itself.
        log.info("TEST RUN: will stop automatically after %ds (RUN_FOR_SECONDS).",
                 seconds)
        return time.time() + seconds
    if seconds > 0:
        # PRODUCTION GUARANTEE: the timer is fully ignored here - there is no
        # code path that can stop the loop in production mode. Only Ctrl+C or
        # killing the process can end it.
        log.warning("RUN_FOR_SECONDS=%d is IGNORED in production mode - the bot "
                    "runs 24/7 until you press Ctrl+C.", seconds)
    return None


def plan_market_buy(price: float, quote_free: float, *, order_size_quote: float,
                    min_qty: float, step: float, min_cost: float,
                    buffer_pct: float = 0.0, fee_rate: float = 0.0,
                    tp_pct: float = 0.0, sl_pct: float = 0.0,
                    slippage_pct: float = 0.0, require_exit_viable: bool = False,
                    quote: str = "USDT", base: str = "BASE") -> dict:
    """
    Turn the configured budget into the exact market-BUY that Binance accepts.

    Pure function (no exchange access) so the sizing rules stay unit-testable.
    Returned dict:

        ok            - True when the order may be sent
        reason        - human readable why not (empty when ok)
        hint          - what to change (top up, lower ORDER_SIZE_QUOTE, ...)
        spend         - quote amount the bot intends to spend
        qty           - lot-rounded quantity to send
        notional      - qty * price, i.e. what MIN_NOTIONAL is checked against
        bumped        - True when the quantity had to be raised by one lot step
                        because flooring would have broken minNotional
        sl_exit_notional / tp_exit_notional - estimated value of the exit order
        exit_notional_ok - False when the stop-loss exit would fall below
                        minNotional (Binance rejects such "dust" sells)
        min_balance   - the free balance this pair really needs
    """
    plan = {
        "ok": False, "reason": "", "hint": "", "warn": "",
        "spend": 0.0, "qty": 0.0, "notional": 0.0, "bumped": False,
        "sl_exit_notional": 0.0, "tp_exit_notional": 0.0,
        "exit_notional_ok": True,
        "min_balance": min_viable_balance(min_cost, step, price, fee_rate, sl_pct,
                                          slippage_pct),
    }
    if price <= 0:
        plan["reason"] = "no valid price available"
        return plan

    spend = min(max(0.0, float(quote_free)), max(0.0, float(order_size_quote)))
    plan["spend"] = spend
    required = max(0.0, float(min_cost)) * (1.0 + max(0.0, buffer_pct) / 100.0)

    if spend <= 0:
        plan["reason"] = "free balance is 0"
        plan["hint"] = (f"deposit funds or lower ORDER_SIZE_QUOTE "
                        f"(currently {order_size_quote:g} {quote})")
        return plan

    qty = floor_to_step(spend / price, step)
    # Lot flooring can push a "just big enough" order below minNotional
    # (10 USDT / 60,000 -> 9.9996 USDT). Buy the next lot up when the free
    # balance can pay for it: that is the smallest order Binance accepts, so a
    # small account wastes nothing on a bigger position than it wants.
    if required > 0 and qty > 0 and qty * price < required:
        next_qty = qty + step
        if next_qty * price <= quote_free + 1e-9:
            qty, plan["bumped"] = next_qty, True

    if qty <= 0 or qty < min_qty:
        plan["qty"] = qty
        plan["reason"] = (f"quantity {qty:.8f} {base} is below the exchange "
                          f"minimum of {min_qty:.8f} {base}")
        plan["hint"] = (f"free balance {quote_free:.4f} {quote} at price "
                        f"{price:,.2f} is too small for this pair")
        return plan

    plan["qty"] = qty
    notional = qty * price
    plan["notional"] = notional

    if required > 0 and notional < required:
        plan["reason"] = (f"order value {notional:,.4f} {quote} is below "
                          f"minNotional {min_cost:,.2f} {quote}")
        plan["hint"] = (f"need about {plan['min_balance']:,.2f} {quote} free "
                        f"(have {quote_free:,.4f}) - top up the balance or trade "
                        f"a pair with a lower minNotional")
        return plan
    # Estimated proceeds of both exits. The taker fee is charged on the
    # received base at entry and on the proceeds at exit, and a market order
    # can slip a few basis points against us in both directions.
    exit_qty = floor_to_step(qty * (1.0 - max(0.0, fee_rate)), step)
    slip = max(0.0, slippage_pct) / 100.0
    plan["sl_exit_notional"] = (exit_qty * price
                                * (1.0 - max(0.0, sl_pct) / 100.0) * (1.0 - slip))
    plan["tp_exit_notional"] = (exit_qty * price
                                * (1.0 + max(0.0, tp_pct) / 100.0) * (1.0 - slip))
    if min_cost and plan["sl_exit_notional"] < min_cost:
        plan["exit_notional_ok"] = False
        if require_exit_viable:
            plan["reason"] = (f"stop-loss exit value {plan['sl_exit_notional']:,.4f} "
                              f"{quote} would fall below minNotional {min_cost:,.2f} "
                              f"{quote} - the exchange would reject that sell")
            plan["hint"] = (f"need about {plan['min_balance']:,.2f} {quote} free to "
                            f"trade this bracket safely, or set "
                            f"REQUIRE_EXIT_VIABLE=false to accept the dust risk")
            return plan
        plan["warn"] = (f"stop-loss exit would be only "
                        f"{plan['sl_exit_notional']:,.4f} {quote} "
                        f"(< minNotional {min_cost:,.2f}) - increase the balance or "
                        f"expect unsellable dust on a losing trade")

    plan["ok"] = True
    return plan


# ------------------------------------------------------------------------------
# Multi-coin / proportional sizing helpers
# ------------------------------------------------------------------------------
def effective_fee_rate(base_fee_rate: float, bnb_discount_pct: float = 0.0) -> float:
    """
    Binance BNB-fee-discount: holding BNB cuts the taker fee by up to 25%.

    `bnb_discount_pct` is the discount in % (0..25). The result is the fee
    fraction actually charged, used by the scanner for sizing and R:R so the
    numbers match an account that pays with BNB.
    """
    base = max(0.0, float(base_fee_rate or 0.0))
    discount = max(0.0, min(25.0, float(bnb_discount_pct or 0.0))) / 100.0
    return base * (1.0 - discount)


def compute_proportional_budget(total_balance_usdt: float, parts: int = 21,
                                floor_usdt: float = 0.0, min_cost: float = 0.0,
                                buffer_pct: float = 0.0) -> dict:
    """
    Split the free USDT balance into `parts` equal position budgets.

    This is the "divide the account into 21 parts" rule from the reference
    video, made safe for small accounts ($10 .. $50+). If one full part would
    fall below the pair's tradeable floor (the largest of `floor_usdt` and
    minNotional `min_cost * (1 + buffer)`), the number of parts is auto-shrunk
    so every part still clears that floor - a $10 account therefore trades one
    properly-sized slot instead of 21 dust orders Binance would reject.

    Returns a dict: per_part, parts (requested), usable_parts, active_budget
    (usable_parts * per_part, i.e. the USDT actually deployable), floor.
    """
    total = max(0.0, float(total_balance_usdt or 0.0))
    requested = max(1, int(parts or 1))
    floor = max(0.0, float(floor_usdt or 0.0))
    if min_cost and min_cost > 0:
        floor = max(floor, float(min_cost) * (1.0 + max(0.0, buffer_pct) / 100.0))
    if total <= 0:
        return {"per_part": 0.0, "parts": requested, "usable_parts": 0,
                "active_budget": 0.0, "floor": floor}

    usable = requested
    if floor > 0 and (total / usable) < floor:
        # Auto-shrink to the largest count whose slot still clears the floor.
        usable = max(1, int(total // floor)) if floor > 0 else requested
        usable = min(usable, requested)
    per_part = total / usable
    if floor > 0 and per_part < floor:   # e.g. floor > the whole balance
        per_part, usable = floor, 1
    return {"per_part": per_part, "parts": requested, "usable_parts": usable,
            "active_budget": per_part * usable, "floor": floor}


def trailing_stop_level(entry_price: float, highest_price: float,
                        trail_pct: float) -> float:
    """
    Current trailing-stop price: `trail_pct` below the post-entry peak
    (>= entry). 0 when the peak is unknown.
    """
    trail = max(0.0, float(trail_pct or 0.0))
    if not entry_price or entry_price <= 0:
        return 0.0
    peak = max(float(entry_price), float(highest_price or entry_price))
    if trail <= 0:
        return 0.0
    return peak * (1.0 - trail / 100.0)


def trailing_stop_armed(entry_price: float, highest_price: float,
                        arm_pct: float = 0.5) -> bool:
    """
    True once the trailing stop may fire at all.

    A trailing stop is a *profit-locking* rule, so on a straight decline a
    trailing stop tighter than the DCA/stop-loss grid would otherwise close the
    trade before the DCA trigger or the hard stop is ever reached. The stop
    therefore only arms after the post-entry peak has climbed `arm_pct` above
    the entry (default 0.5%). With TRAILING_STOP_PCT=0 nothing trails anyway.
    """
    if not entry_price or entry_price <= 0:
        return False
    peak = float(highest_price or entry_price)
    return peak >= float(entry_price) * (1.0 + max(0.0, float(arm_pct or 0.0)) / 100.0)


def trailing_stop_hit(entry_price: float, highest_price: float,
                      current_price: float, trail_pct: float,
                      arm_pct: float = 0.5) -> bool:
    """
    True once current_price has dropped `trail_pct` below its post-entry peak.

    A trailing stop is a *profit-locking* risk rule: while the price rises the
    tight stop-loss is dragged up behind it, so a pullback that reaches the
    running peak minus trail_pct closes the position instead of giving back the
    whole win. It only counts once it is armed (see trailing_stop_armed), i.e.
    after the price really went up, so it never pre-empts the DCA or the fixed
    stop-loss on the way down. Returns False when there is nothing to trail.
    """
    trail = max(0.0, float(trail_pct or 0.0))
    if trail <= 0:
        return False
    if not current_price or current_price <= 0 or not entry_price or entry_price <= 0:
        return False
    if not trailing_stop_armed(entry_price, highest_price, arm_pct):
        return False
    level = trailing_stop_level(entry_price, highest_price, trail_pct)
    return level > 0 and current_price <= level


def dca_trigger_level(avg_entry: float, dca_drop_pct: float) -> float:
    """
    Price at which the next DCA (averaging-down) add is triggered.

    The drop is measured against the *current average entry*, so every add
    moves the next trigger down by the same percentage - a natural grid that
    always requires a further `dca_drop_pct` decline before more money is
    committed. Returns 0.0 when the level cannot be computed (0 = disabled).
    """
    drop = max(0.0, float(dca_drop_pct or 0.0))
    if not avg_entry or avg_entry <= 0 or drop <= 0:
        return 0.0
    return float(avg_entry) * (1.0 - drop / 100.0)


def plan_dca(price: float, *, avg_entry: float, qty: float, quote_free: float,
             dca_drop_pct: float = 1.5, dca_size_quote: float = 10.0,
             dca_count: int = 0, dca_max_entries: int = 2,
             min_qty: float = 1e-8, step: float = 1e-8, min_cost: float = 0.0,
             buffer_pct: float = 0.0, fee_rate: float = 0.0, tp_pct: float = 0.0,
             sl_pct: float = 0.0, slippage_pct: float = 0.05,
             position_cost: float = 0.0, max_position_quote: float = 0.0,
             quote: str = "USDT", base: str = "BASE") -> dict:
    """
    Decide whether the open position should be averaged down right now.

    Pure function (no exchange access), so the DCA rules stay unit-testable and
    the single-symbol bot and the multi-coin scanner obey exactly the same
    maths. It is only ever *asked* while a position is open, and it answers one
    question: "should I buy MORE at this price instead of being stopped out?"

    An add is offered when ALL of the following hold:
        * DCA is enabled and this position still has entries left,
        * price <= average entry * (1 - dca_drop_pct/100),
        * the hard stop-loss has NOT been broken yet (averaging into a trade
          that already gave up its stop is how accounts die),
        * the per-position cap (DCA_MAX_POSITION_QUOTE) and the free balance
          still allow at least the exchange minimum notional.

    Returned dict:
        ok            - True when the add may be sent
        added         - alias of ok (reads better in log lines)
        trigger       - True when the price really is at/below the DCA level
        reason/hint   - human readable why not (empty when ok)
        level         - the trigger price for the *next* add
        drop_pct      - current price distance below the average entry (%)
        spend/qty/notional - the add (lot rounded, minNotional checked)
        new_qty/new_entry/new_cost - the position after the add
        new_tp/new_sl - TP/SL recomputed from the new average entry
        dca_count/max_entries - how many adds will have been used
    """
    max_entries = max(0, int(dca_max_entries or 0))
    size = max(0.0, float(dca_size_quote or 0.0))
    drop = max(0.0, float(dca_drop_pct or 0.0))
    old_qty = max(0.0, float(qty or 0.0))
    old_entry = float(avg_entry or 0.0)
    old_cost = max(0.0, float(position_cost or 0.0)) or old_qty * old_entry
    count = max(0, int(dca_count or 0))
    plan = {
        "ok": False, "added": False, "trigger": False, "reason": "", "hint": "",
        "warn": "", "level": dca_trigger_level(old_entry, drop), "drop_pct": 0.0,
        "spend": 0.0, "qty": 0.0, "notional": 0.0,
        "new_qty": old_qty, "new_entry": old_entry, "new_cost": old_cost,
        "new_tp": 0.0, "new_sl": 0.0,
        "dca_count": count, "max_entries": max_entries,
    }
    if max_entries <= 0 or size <= 0 or drop <= 0:
        plan["reason"] = "DCA is disabled"
        return plan
    if old_entry <= 0 or old_qty <= 0:
        plan["reason"] = "no tracked position to average down"
        return plan
    if not price or price <= 0:
        plan["reason"] = "no valid price available"
        return plan

    level = plan["level"]
    plan["trigger"] = price <= level
    if not plan["trigger"]:
        plan["reason"] = (f"price {price:,.2f} is above the DCA level "
                          f"{level:,.2f} (-{drop:g}% of {old_entry:,.2f})")
        return plan

    plan["drop_pct"] = (old_entry / price - 1.0) * 100.0
    if count >= max_entries:
        plan["reason"] = f"all {max_entries} DCA entr(ies) already used"
        return plan
    # Never add money to a trade that has already broken its hard stop: that
    # position is being closed instead of averaged down.
    if sl_pct and sl_pct > 0:
        stop = old_entry * (1.0 - float(sl_pct) / 100.0)
        if price <= stop:
            plan["reason"] = (f"price {price:,.2f} already broke the -"
                              f"{float(sl_pct):g}% stop-loss {stop:,.2f} - no "
                              f"averaging into a broken trade")
            return plan

    budget = size
    if max_position_quote and max_position_quote > 0:
        room = float(max_position_quote) - old_cost
        if room <= 0:
            plan["reason"] = (f"position already holds {old_cost:,.2f} {quote} "
                              f"(DCA_MAX_POSITION_QUOTE="
                              f"{float(max_position_quote):,.2f})")
            return plan
        budget = min(budget, room)
    free = max(0.0, float(quote_free or 0.0))
    if free <= 0:
        plan["reason"] = "free balance is 0"
        plan["hint"] = "wait for an exit, or lower DCA_SIZE_QUOTE"
        return plan
    plan["spend"] = min(budget, free)

    # Size the add with exactly the same rules as a first entry: lot step,
    # minNotional bump, minQty / minNotional refusal with an exact message.
    # `free` (the real balance) is passed as quote_free on purpose: the
    # minNotional bump must be allowed to buy the *next lot* up within the free
    # balance, otherwise an add that sits exactly on minNotional (10 USDT) is
    # refused by the very rounding it was meant to fix.
    buy = plan_market_buy(
        price, free, order_size_quote=plan["spend"], min_qty=min_qty,
        step=step, min_cost=min_cost, buffer_pct=buffer_pct, fee_rate=fee_rate,
        tp_pct=tp_pct, sl_pct=sl_pct, slippage_pct=slippage_pct,
        require_exit_viable=False, quote=quote, base=base)
    if not buy["ok"]:
        plan["reason"] = f"DCA add refused: {buy['reason']}"
        plan["hint"] = buy["hint"]
        return plan

    plan.update({
        "ok": True, "added": True, "warn": buy["warn"], "qty": buy["qty"],
        "notional": buy["notional"], "dca_count": count + 1,
    })
    plan["new_qty"] = old_qty + plan["qty"]
    plan["new_cost"] = old_cost + plan["notional"]
    if plan["new_qty"] > 0:
        plan["new_entry"] = plan["new_cost"] / plan["new_qty"]
    if tp_pct:
        plan["new_tp"] = plan["new_entry"] * (1.0 + float(tp_pct) / 100.0)
    if sl_pct:
        plan["new_sl"] = plan["new_entry"] * (1.0 - float(sl_pct) / 100.0)
    return plan


def normalize_symbol(raw: str, quote: str = "USDT") -> str:
    """
    Turn a user-typed pair into ccxt unified notation ("BTC/USDT").

    Accepts "BTC/USDT", "btc/usdt", "BTCUSDT" and "BTC-USDT" - the classic
    Binance name - so a SCANNER_SYMBOLS list like ["BTCUSDT","ETHUSDT"] works
    out of the box. Anything that is not built from the quote currency is
    returned unchanged (the caller then simply won't find it in the markets).
    """
    if not raw:
        return ""
    text = str(raw).strip().upper().replace("-", "/").replace("_", "/")
    quote = (quote or "USDT").upper()
    if "/" in text:
        return text
    if quote and text.endswith(quote) and len(text) > len(quote):
        return f"{text[:-len(quote)]}/{quote}"
    return text


def discover_spot_usdt_symbols(markets, quote: str = "USDT",
                               min_24h_quote: float = 100000.0,
                               exclude=(), max_symbols: int = 0) -> list:
    """
    Discover the dynamic scan universe from ccxt `load_markets()` output.

    `markets` is a dict {symbol: market} (as returned by ccxt.load_markets) so
    this stays a pure, unit-testable function - pass the real dict in live mode
    or a hand-built one in the smoke test.

    Returns spot pairs quoted in `quote`, sorted by 24h quote volume descending,
    optionally filtered to pairs traded at least `min_24h_quote` in 24h and the
    `exclude` set, and capped to `max_symbols` (0 = no cap).
    """
    q = (quote or "USDT").upper()
    suffix = "/" + q
    rows = []
    for symbol, market in (markets or {}).items():
        if not isinstance(symbol, str) or not symbol.endswith(suffix):
            continue
        mtype = (market or {}).get("type")
        if mtype not in (None, "spot", ""):
            continue
        up = symbol.upper()
        if up in {e.upper() for e in exclude}:
            continue
        info = (market or {}).get("info") or {}
        stats = (market or {}).get("stats") or {}
        # ccxt exposes 24h volume in several shapes; prefer the quote volume.
        volume24 = float(stats.get("quoteVolume") or stats.get("quoteVolume24h")
                         or info.get("quoteVolume") or 0.0)
        rows.append((volume24, up))
    rows.sort(key=lambda r: r[0], reverse=True)
    if min_24h_quote and min_24h_quote > 0:
        rows = [r for r in rows if r[0] >= float(min_24h_quote)]
    symbols = [r[1] for r in rows]
    if max_symbols and max_symbols > 0:
        symbols = symbols[:max_symbols]
    return symbols


# ------------------------------------------------------------------------------
# The bot
# ------------------------------------------------------------------------------
class BinanceTestnetBot:
    """Runs the strategy once per poll interval, with full error handling."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.log = logging.getLogger("bot")
        self.exchange = None
        self.paper_usdt = 0.0            # demo-mode simulated balances
        self.paper_btc = 0.0
        self.demo_df = None
        self.min_qty = 1e-8
        self.min_cost = 0.0
        self.step = 1e-8
        # Fee used for estimating costs (and for the paper fills in DEMO mode).
        # _init_live() replaces it with the pair's real taker fee when ccxt
        # reports one, so the net R:R report matches the account.
        self.fee_rate = max(0.0, float(cfg.get("fee_rate", 0.001) or 0.0))
        # BNB-fee-discount (0..25%): holding BNB cuts the taker fee, which shrinks
        # the effective per-trade cost used for sizing and the R:R report.
        self.bnb_discount_pct = max(0.0, min(25.0,
                                             float(cfg.get("bnb_fee_discount_pct", 0.0) or 0.0)))
        self.fee_rate = effective_fee_rate(self.fee_rate, self.bnb_discount_pct)
        # Trailing-stop distance (%). 0 = classic fixed stop-loss only.
        self.trailing_stop_pct = max(0.0, float(cfg.get("trailing_stop_pct", 0.0) or 0.0))
        # The trail only arms after the peak rose this % above the entry.
        self.trailing_arm_pct = max(0.0, float(cfg.get("trailing_arm_pct", 0.5) or 0.0))
        # DCA / averaging down: a position that falls dca_drop_pct below its
        # current average entry buys dca_size_quote more instead of being
        # stopped out immediately (see plan_dca for the guard rails).
        self.dca_enabled = bool(cfg.get("dca_enabled", False))
        self.dca_drop_pct = max(0.0, float(cfg.get("dca_drop_pct", 1.5) or 0.0))
        self.dca_size_quote = max(0.0, float(cfg.get("dca_size_quote", 0.0) or 0.0))
        self.dca_max_entries = max(0, int(cfg.get("dca_max_entries", 0) or 0))
        self.dca_max_position_quote = max(
            0.0, float(cfg.get("dca_max_position_quote", 0.0) or 0.0))
        # Optional RSI entry filter on top of the ADX gate (0 = off).
        self.rsi_max_entry = max(0.0, float(cfg.get("rsi_max_entry", 0.0) or 0.0))
        self.rsi_min_entry = max(0.0, float(cfg.get("rsi_min_entry", 0.0) or 0.0))
        self.last_signal_candle = None   # avoid re-trading the same candle
        self.failures = 0                # for exponential backoff
        self.avg_entry_price = None      # last BUY fill price (for Telegram P/L)
        self._last_error_notify = (None, 0.0)  # last (kind, time) sent to Telegram
        # --- trade tracking (drives the log / Telegram trade journal) ---------
        self.position = None             # open position: id, entry, qty, tp, sl...
        self.closed_trades = []          # every closed trade (for the statistics)
        self.trade_counter = 0           # ids: #1, #2, ... for tracking
        self.consecutive_losses = 0
        self.last_loss_candle = None     # cooldown anchor after a STOP-LOSS
        self.day_key = None              # UTC day of the daily-loss guard
        self.day_start_equity = None
        self.day_pnl = 0.0
        self._day_limit_notified = False
        self._sizing_reported = False    # startup sizing/R:R report logged once
        # Optional Telegram push notifications (no-op when not configured).
        # Failed sends are retried automatically: a few quick inline attempts,
        # then a background queue keeps re-sending without blocking the loop.
        self.tg = TelegramNotifier(
            cfg["telegram_bot_token"], cfg["telegram_chat_id"], self.log,
            max_attempts=cfg["telegram_max_attempts"],
            retry_delays=cfg["telegram_retry_delays"],
            queue_retry_seconds=cfg["telegram_queue_retry_seconds"],
            queue_max_age=cfg["telegram_queue_max_age"],
        )

        if cfg["demo_mode"]:
            self._init_demo()
        else:
            self._init_live()

    # -- initialisation -------------------------------------------------------
    def _init_live(self):
        """Connect to the Binance SPOT TESTNET via ccxt."""
        if not self.cfg["api_key"] or not self.cfg["api_secret"]:
            raise RuntimeError(
                "Missing testnet API keys. Either add BINANCE_TESTNET_API_KEY / "
                "BINANCE_TESTNET_API_SECRET to your .env file, or set DEMO_MODE=true."
            )

        self.log.info("Creating ccxt Binance exchange object ...")
        self.exchange = ccxt.binance({
            "apiKey": self.cfg["api_key"],
            "secret": self.cfg["api_secret"],
            "enableRateLimit": True,     # ccxt paces requests for us
            "timeout": 20000,            # ms until a request is considered lost
            "options": {
                "defaultType": "spot",        # we only trade spot pairs
                "adjustForTimeDifference": True,  # avoids timestamp/recvWindow errors
                "fetchMarkets": ["spot"],     # don't query dead/hanging futures testnet endpoints
            },
        })

        # THE SAFETY NET: force every single request onto the Spot Testnet
        # (https://testnet.binance.vision). Real money is never involved.
        self.exchange.set_sandbox_mode(True)

        self.log.info("Loading market rules for %s from the testnet ...", self.cfg["symbol"])
        markets = self.exchange.load_markets()
        if self.cfg["symbol"] not in markets:
            raise RuntimeError(
                f"{self.cfg['symbol']} is not available on the Binance Spot Testnet."
            )
        self.min_qty, self.min_cost, self.step = symbol_rules(self.exchange, self.cfg["symbol"])
        # Prefer the pair's real taker fee (ccxt exposes it as a fraction) so the
        # net reward/risk report is the one that applies to this account.
        try:
            taker = (self.exchange.market(self.cfg["symbol"]) or {}).get("taker")
            if taker:
                self.fee_rate = effective_fee_rate(float(taker), self.bnb_discount_pct)
        except (TypeError, ValueError):
            pass
        self.log.info(
            "Symbol rules -> minQty=%.8f  stepSize=%.8f  minNotional=%.2f %s  taker=%.3f%%",
            self.min_qty, self.step, self.min_cost, self.cfg["symbol"].split("/")[1],
            self.fee_rate * 100.0,
        )

    def _init_demo(self):
        """Local simulation with synthetic price data - no exchange connection."""
        self.log.warning(
            "DEMO_MODE=true -> simulating everything locally, no exchange connection."
        )
        self.min_qty = 0.0001      # testnet-like BTCUSDT lot rules
        self.min_cost = 10.0       # Binance BTCUSDT minNotional (~10 USDT)
        self.step = 1e-8
        # Simulated Spot fee (0.1% taker) applied to paper fills so the balance
        # behaves like the real 10 USDT testnet account (including residual
        # dust after fees).
        self.fee_rate = max(0.0, float(self.cfg["fee_rate"]))
        self.paper_usdt = max(0.0, float(self.cfg["initial_balance_usdt"]))
        self.paper_btc = 0.0
        self.log.info(
            "Demo balance: %.2f USDT (order size %.2f USDT, minNotional %.2f, "
            "fee %.2f%%)",
            self.paper_usdt, self.cfg["order_size_quote"], self.min_cost,
            self.fee_rate * 100.0,
        )

        # Build a synthetic random-walk candle history so indicators have data.
        rows = []
        price = 60_000.0
        now = int(time.time() * 1000)
        ms = self._timeframe_ms()
        for i in range(self.cfg["candle_limit"]):
            ts = now - (self.cfg["candle_limit"] - i) * ms
            open_ = price
            price = max(100.0, open_ * (1.0 + random.gauss(0.0, 0.002)))
            high = max(open_, price) * (1.0 + random.random() * 0.001)
            low = min(open_, price) * (1.0 - random.random() * 0.001)
            vol = random.uniform(0.5, 20.0)
            rows.append([ts, open_, high, low, price, vol])
        self.demo_df = pd.DataFrame(
            rows, columns=["ts", "open", "high", "low", "close", "volume"]
        )

    # -- sizing & pro metrics --------------------------------------------------
    def _net_rr(self) -> dict:
        """Net reward/risk of the configured TP/SL bracket (after costs)."""
        return reward_risk_after_costs(
            self.cfg["take_profit_pct"], self.cfg["stop_loss_pct"],
            self.fee_rate, self.cfg["slippage_pct"],
        )

    def _sellable_qty(self, base_free: float) -> float:
        """Quantity that can actually be sold: floored to the exchange lot step."""
        return floor_to_step(max(0.0, float(base_free)), self.step)

    def _log_sizing_report(self, price: float, quote_free: float,
                           base_free: float = 0.0) -> None:
        """
        One-time startup report: what this pair needs from this account.

        A small balance fails in two ways that are easy to miss in a log - the
        entry is rejected by the MIN_NOTIONAL filter, or the stop-loss exit is
        rejected as unsellable dust. Both are computed up front from the live
        symbol rules and printed once, together with the *net* reward/risk of
        the configured TP/SL.
        """
        _, quote = self.cfg["symbol"].split("/")
        rr = self._net_rr()
        need = min_viable_balance(self.min_cost, self.step, price, self.fee_rate,
                                  self.cfg["stop_loss_pct"], self.cfg["slippage_pct"])
        equity = quote_free + base_free * price
        self.log.info(
            "Sizing check: budget %g %s | minNotional %.2f %s | safe minimum "
            "account %.2f %s | free %.4f %s (equity %.2f)",
            self.cfg["order_size_quote"], quote, self.min_cost, quote,
            need, quote, quote_free, quote, equity,
        )
        self.log.info(
            "Net reward/risk after costs: win +%.2f%% / loss -%.2f%% -> R:R %.2f "
            "(break-even win rate %.1f%%) | TP %g%% | SL %g%% | %.2f%% per round trip",
            rr["net_win_pct"], rr["net_loss_pct"], rr["rr"], rr["break_even_win_rate"],
            self.cfg["take_profit_pct"], self.cfg["stop_loss_pct"], rr["cost_pct"],
        )
        if rr["net_win_pct"] <= 0:
            self.log.warning(
                "TAKE_PROFIT_PCT=%g%% does not even cover the trading costs "
                "(%.2f%% per round trip) - every winner would be a loser. "
                "Raise TAKE_PROFIT_PCT or lower the fee/slippage assumptions.",
                self.cfg["take_profit_pct"], rr["cost_pct"],
            )
        if self.dca_enabled and self.dca_max_entries > 0 and self.dca_drop_pct > 0:
            self.log.info(
                "DCA on: up to %d add(s) of %.4f %s at -%g%% below the average "
                "entry | per-coin cap %s | first trigger from %.2f is %.2f",
                self.dca_max_entries, self.dca_size(), quote, self.dca_drop_pct,
                (f"{self.dca_max_position_quote:,.2f} {quote}"
                 if self.dca_max_position_quote > 0
                 else "none (free balance only)"),
                price, dca_trigger_level(price, self.dca_drop_pct))
        else:
            self.log.info("DCA off (set DCA_ENABLED=true to average down "
                          "instead of being stopped out immediately).")
        if self.min_cost and quote_free < need:
            self.log.warning(
                "Free balance %.4f %s is below the ~%.2f %s this pair needs for a "
                "%.2f %s order with a -%g%% stop (minNotional %.2f + fees/lot step). "
                "Entries will be SKIPPED until the balance is topped up; exits keep "
                "working. Smaller pairs with a lower minNotional are an alternative.",
                quote_free, quote, need, quote, self.cfg["order_size_quote"], quote,
                self.cfg["stop_loss_pct"], self.min_cost,
            )

    # -- trade tracking & account protection ----------------------------------
    def _trade_stats(self) -> dict:
        """Win/loss statistics of every trade closed in this session."""
        wins = [t for t in self.closed_trades if t["pnl_quote"] > 0]
        losses = [t for t in self.closed_trades if t["pnl_quote"] <= 0]
        total = sum(t["pnl_quote"] for t in self.closed_trades)
        n = len(self.closed_trades)
        return {
            "trades": n,
            "wins": len(wins),
            "losses": len(losses),
            "pnl_quote": total,
            "win_rate": (100.0 * len(wins) / n) if n else 0.0,
        }

    def _stats_line(self, quote: str) -> str:
        """One-line journal footer used in the Telegram messages."""
        s = self._trade_stats()
        if not s["trades"]:
            return f"Session: no closed trades yet | today {self.day_pnl:+.4f} {quote}"
        return (f"Session: {s['trades']} closed ({s['wins']}W/{s['losses']}L, "
                f"{s['win_rate']:.0f}% win) | session P/L {s['pnl_quote']:+.4f} {quote} "
                f"| today {self.day_pnl:+.4f} {quote}")

    def _roll_day_if_needed(self, price: float, quote_free: float,
                           base_free: float, quote: str) -> bool:
        """
        Reset the daily counters at the UTC day change. Returns True when a new
        day started (so the caller can report it once).
        """
        key = time.strftime("%Y-%m-%d", time.gmtime())
        if key == self.day_key:
            return False
        equity = quote_free + base_free * price
        self.day_key = key
        self.day_start_equity = equity if equity > 0 else None
        self.day_pnl = 0.0
        self.consecutive_losses = 0      # a fresh day lifts the losing-streak pause
        self.last_loss_candle = None
        self._day_limit_notified = False
        self.log.info(
            "[risk] new UTC day %s - daily counters reset (account equity %.4f %s)",
            key, equity, quote,
        )
        return True

    def _entry_gate(self, candle_ts: int) -> tuple:
        """
        Account-protection gates checked before every NEW entry - never before
        an exit (closing a position is always allowed). Returns (allowed, reason).

            1. daily loss limit  - stop digging after MAX_DAILY_LOSS_PCT of the
               day's starting equity is gone,
            2. losing streak     - pause after MAX_CONSECUTIVE_LOSSES in a row,
            3. stop-loss cooldown- stay flat for LOSS_COOLDOWN_CANDLES candles
               after a stop-out instead of re-entering the same chop.
        """
        cfg = self.cfg
        if cfg["max_daily_loss_pct"] > 0 and self.day_start_equity:
            limit = self.day_start_equity * cfg["max_daily_loss_pct"] / 100.0
            if self.day_pnl <= -limit:
                return False, (f"daily loss limit hit ({self.day_pnl:+.4f} <= -{limit:.4f} "
                               f"= -{cfg['max_daily_loss_pct']:g}% of the day's equity)")
        if cfg["max_consecutive_losses"] and self.consecutive_losses >= cfg["max_consecutive_losses"]:
            return False, (f"{self.consecutive_losses} losing trades in a row "
                           f"(MAX_CONSECUTIVE_LOSSES={cfg['max_consecutive_losses']})")
        if cfg["loss_cooldown_candles"] and self.last_loss_candle is not None:
            span = cfg["loss_cooldown_candles"] * self._timeframe_ms()
            if candle_ts < int(self.last_loss_candle) + span:
                left = int(round((int(self.last_loss_candle) + span - candle_ts)
                                 / max(1, self._timeframe_ms())))
                return False, (f"cooldown after a stop-loss ({left} candle(s) to go)")
        return True, "ok"

    # -- data access ----------------------------------------------------------
    def _timeframe_ms(self) -> int:
        """Convert the candle timeframe string to milliseconds."""
        unit = self.cfg["timeframe"][-1]
        mult = int(self.cfg["timeframe"][:-1] or "1")
        if unit in ("s", "m", "h", "d", "w"):
            secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]
            return mult * secs * 1000
        return 60000  # default: 1 minute

    def get_market_snapshot(self):
        """Return (current_price, candles_df). Live or simulated."""
        if self.exchange is None:  # demo path
            df = self.demo_df.copy()
            last = float(df["close"].iloc[-1])
            open_ = last
            close = max(100.0, open_ * (1.0 + random.gauss(0.0, 0.0015)))
            high = max(open_, close) * (1.0 + random.random() * 0.0008)
            low = min(open_, close) * (1.0 - random.random() * 0.0008)
            vol = random.uniform(0.5, 20.0)
            ts = int(df["ts"].iloc[-1]) + self._timeframe_ms()
            new_row = pd.DataFrame(
                [[ts, open_, high, low, close, vol]], columns=df.columns
            )
            df = pd.concat([df, new_row], ignore_index=True)
            df = df.iloc[-self.cfg["candle_limit"]:].reset_index(drop=True)
            df["ts"] = df["ts"].astype("int64")
            self.demo_df = df
            return close, df

        # Live testnet path: freshest price + the last N candles.
        ticker = self.exchange.fetch_ticker(self.cfg["symbol"])
        price = float(ticker.get("last") or ticker.get("close") or 0.0)
        ohlcv = self.exchange.fetch_ohlcv(
            self.cfg["symbol"], timeframe=self.cfg["timeframe"],
            limit=self.cfg["candle_limit"],
        )
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = df[col].astype("float64")
        df["ts"] = df["ts"].astype("int64")
        return price, df

    def get_balances(self) -> dict:
        """Return {base: {free}, quote: {free}} for the traded pair."""
        base, quote = self.cfg["symbol"].split("/")
        if self.exchange is None:  # demo
            return {
                base: {"free": self.paper_btc},
                quote: {"free": self.paper_usdt},
            }
        bal = self.exchange.fetch_balance()
        base_free = float((bal.get(base, {}) or {}).get("free") or 0.0)
        quote_free = float((bal.get(quote, {}) or {}).get("free") or 0.0)
        return {base: {"free": base_free}, quote: {"free": quote_free}}

    # -- Telegram notifications -----------------------------------------------
    def notify_startup(self) -> None:
        """Push a startup summary (mode, symbol, strategy, balance) to Telegram."""
        if not self.tg.enabled:
            return
        cfg = self.cfg
        base, quote = cfg["symbol"].split("/")
        mode = ("LOCAL DEMO SIMULATION" if cfg["demo_mode"]
                else "LIVE TESTNET (paper money @ testnet.binance.vision)")
        try:
            bal = self.get_balances()
            balance_txt = "{:.4f} {} | {:.8f} {}".format(
                bal[quote]["free"], quote, bal[base]["free"], base,
            )
        except Exception as err:  # balance fetch failed - still send the message
            self.log.warning(
                "Balance fetch for the startup Telegram message failed: %s", err,
            )
            balance_txt = "unavailable"
        rr = self._net_rr()
        trail_txt = (f"Trailing stop: -{self.trailing_stop_pct:g}% from the peak "
                     f"(arms above +{self.trailing_arm_pct:g}%)"
                     if self.trailing_stop_pct > 0 else "Trailing stop: off")
        dca_txt = ("DCA: off"
                   if not (self.dca_enabled and self.dca_max_entries > 0)
                   else f"DCA: up to {self.dca_max_entries} add(s) of "
                        f"{self.dca_size():,.2f} {quote} at "
                        f"-{self.dca_drop_pct:g}% (per-coin cap "
                        f"{self.dca_max_position_quote:,.2f} {quote})")
        rsi_gate = (f"RSI filter: {self.rsi_min_entry:g} < RSI < "
                    f"{self.rsi_max_entry:g}"
                    if (self.rsi_min_entry > 0 or self.rsi_max_entry > 0)
                    else "RSI filter: off")
        self.tg.send(
            "🤖 Trading bot started\n"
            f"Mode: {mode}\n"
            f"Symbol: {cfg['symbol']} ({cfg['timeframe']} candles"
            f"{', closed candles only' if cfg['signal_on_closed_candle'] else ''})\n"
            f"Strategy: EMA({cfg['ema_fast']}, {cfg['ema_slow']}) crossover "
            f"-> ADX({cfg['adx_period']}) trend filter "
            f"(BUY only when ADX > {cfg['adx_threshold']:g})\n"
            f"Risk: Take-Profit +{cfg['take_profit_pct']:g}% / "
            f"Stop-Loss -{cfg['stop_loss_pct']:g}%\n"
            f"{trail_txt}\n"
            f"{dca_txt}\n"
            f"{rsi_gate}\n"
            f"Net after costs: win +{rr['net_win_pct']:.2f}% / loss "
            f"-{rr['net_loss_pct']:.2f}% -> R:R {rr['rr']:.2f}\n"
            f"Risk gates: daily loss {cfg['max_daily_loss_pct']:g}%, "
            f"{cfg['max_consecutive_losses']} losses in a row, "
            f"{cfg['loss_cooldown_candles']} candle(s) cooldown after a stop-out\n"
            f"Order size: {cfg['order_size_quote']:g} {quote} per BUY\n"
            f"Balance: {balance_txt}"
        )

    def _close_position_pl(self, sell_price: float, qty: float, quote: str,
                           reason: str = "") -> str:
        """
        Record the closed trade and return the human-readable P/L line.

        Everything the trade journal needs is captured here: the P/L in quote
        currency and %, the reason, the holding time, the ADX/RSI at entry and
        the running session statistics. The text is also what the Telegram SELL
        message shows.
        """
        position, self.position = self.position, None
        entry, self.avg_entry_price = self.avg_entry_price, None
        if not entry or entry <= 0 or qty <= 0:
            return "P/L: n/a (position was opened before this session)"
        pnl = (sell_price - entry) * qty
        pct = (sell_price / entry - 1.0) * 100.0
        fee_est = (entry * qty + sell_price * qty) * self.fee_rate
        in_position = bool(position)
        opened_ts = position.get("opened_ts") if in_position else None
        self.closed_trades.append({
            "id": position.get("id") if in_position else None,
            "entry": entry, "exit": sell_price, "qty": qty,
            "pnl_quote": pnl, "pnl_pct": pct, "reason": reason,
            "closed_ts": time.time(),
        })
        self.day_pnl += pnl
        if pnl > 0:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
        icon = "📈" if pnl >= 0 else "📉"
        hold = ""
        if opened_ts:
            secs = max(0, int(time.time() - opened_ts))
            hold = f" | held {secs // 3600}h {(secs % 3600) // 60}m"
        trade_tag = f"trade #{position.get('id')} " if in_position and position.get("id") else ""
        entry_ctx = ""
        if in_position:
            adx_txt = self._adx_display(position.get("entry_adx"))
            rsi_txt = ("n/a" if position.get("entry_rsi") is None
                       else f"{float(position['entry_rsi']):.1f}")
            entry_ctx = f"\nAt entry: ADX {adx_txt} | RSI {rsi_txt} | {position.get('reason', '')}"
            if position.get("planned_rr"):
                entry_ctx += f" | planned R:R {position['planned_rr']:.2f}"
        return (f"{icon} {trade_tag}P/L: {pnl:+.4f} {quote} ({pct:+.2f}%) | "
                f"entry {entry:,.2f} -> exit {sell_price:,.2f} | est. fees "
                f"{fee_est:,.4f} {quote}{hold}{entry_ctx}\n{self._stats_line(quote)}")


    # -- TP / SL risk management ----------------------------------------------
    def _tp_sl_levels(self, entry: float = None):
        """Compute (take_profit, stop_loss) prices from an entry price."""
        if entry is None:
            entry = self.avg_entry_price
        if not entry or entry <= 0:
            return None, None
        tp = entry * (1.0 + self.cfg["take_profit_pct"] / 100.0)
        sl = entry * (1.0 - self.cfg["stop_loss_pct"] / 100.0)
        return tp, sl

    def _tp_sl_text(self) -> str:
        """Human-readable TP/SL line used in logs and Telegram messages."""
        tp, sl = self._tp_sl_levels()
        if tp is None:
            return "TP/SL: n/a (no entry price yet)"
        return ("TP: {:,.2f} (+{:g}%) | SL: {:,.2f} (-{:g}%)".format(
            tp, self.cfg["take_profit_pct"], sl, self.cfg["stop_loss_pct"]))

    def _tp_exit_reason(self, price: float):
        """'TAKE-PROFIT ...' once the live price reached the hard target."""
        if not self.avg_entry_price or self.avg_entry_price <= 0:
            return None
        tp, _ = self._tp_sl_levels(self.avg_entry_price)
        if tp and price >= tp:
            return (f"TAKE-PROFIT +{self.cfg['take_profit_pct']:g}% hit "
                    f"(target {tp:,.2f})")
        return None

    def _stop_loss_exit_reason(self, price: float):
        """'STOP-LOSS ...' once the live price reached the hard stop."""
        if not self.avg_entry_price or self.avg_entry_price <= 0:
            return None
        _, sl = self._tp_sl_levels(self.avg_entry_price)
        if sl and price <= sl:
            return (f"STOP-LOSS -{self.cfg['stop_loss_pct']:g}% hit "
                    f"(stop {sl:,.2f})")
        return None

    def _trailing_exit_reason(self, price: float):
        """
        'TRAILING-STOP ...' once the price retraced below the raised stop.

        The post-entry peak is updated on every poll (even before the trail is
        armed), so the stop always trails the real high of the position. The
        trail only arms after the peak climbed TRAILING_ARM_PCT above the
        average entry, which keeps it a pure profit-lock and lets the DCA /
        stop-loss rules own the way down.
        """
        if self.trailing_stop_pct <= 0 or not self.avg_entry_price:
            return None
        pos = self.position
        if not pos:
            return None
        prev_peak = float(pos.get("trail_peak") or self.avg_entry_price)
        peak = max(prev_peak, price)
        pos["trail_peak"] = peak
        if not trailing_stop_hit(self.avg_entry_price, prev_peak, price,
                                 self.trailing_stop_pct, self.trailing_arm_pct):
            return None
        level = trailing_stop_level(self.avg_entry_price, peak,
                                    self.trailing_stop_pct)
        return (f"TRAILING-STOP -{self.trailing_stop_pct:g}% hit "
                f"(peak {peak:,.2f} -> stop {level:,.2f})")

    def _tp_sl_exit_reason(self, price: float):
        """
        Full hard-bracket check: 'TAKE-PROFIT ...' / 'STOP-LOSS ...' or None.

        Kept as one call for the log/smoke-test contract; the tick loop uses the
        split `_tp_exit_reason` / `_stop_loss_exit_reason` helpers so the DCA
        step can sit between the two.
        """
        return self._tp_exit_reason(price) or self._stop_loss_exit_reason(price)

    def _close_now(self, price: float, base_free: float, candle_ts,
                   reason: str) -> None:
        """
        Close the open position with `reason` and mark the candle as handled.

        A STOP-LOSS / TRAILING-STOP exit also starts the re-entry cooldown so
        the bot does not immediately buy back into the chop that just stopped
        it out. A TAKE-PROFIT exit needs no cooldown.
        """
        self.log.info("%s -> closing position @ %.2f", reason, price)
        self.place_sell(price, base_free, reason=reason)
        self.last_signal_candle = candle_ts
        if reason.startswith(("STOP-LOSS", "TRAILING-STOP")):
            self.last_loss_candle = candle_ts

    @staticmethod
    def _adx_display(adx_value) -> str:
        """Render an ADX value (or NaN / None) as a compact display string."""
        try:
            if adx_value is None or pd.isna(adx_value):
                return "n/a"
            return f"{float(adx_value):.1f}"
        except (TypeError, ValueError):
            return "n/a"

    # -- trading --------------------------------------------------------------
    def plan_buy(self, price: float, quote_free: float) -> dict:
        """
        Sizing decision for the next entry (rules live in plan_market_buy).

        Thin wrapper around the pure sizing helper so the trading code, the
        startup report and the smoke test always agree on the numbers.
        """
        base, quote = self.cfg["symbol"].split("/")
        return plan_market_buy(
            price, quote_free,
            order_size_quote=self.cfg["order_size_quote"],
            min_qty=self.min_qty, step=self.step, min_cost=self.min_cost,
            buffer_pct=self.cfg["min_notional_buffer_pct"],
            fee_rate=self.fee_rate,
            tp_pct=self.cfg["take_profit_pct"], sl_pct=self.cfg["stop_loss_pct"],
            slippage_pct=self.cfg["slippage_pct"],
            require_exit_viable=self.cfg["require_exit_viable"],
            quote=quote, base=base,
        )

    def place_buy(self, price: float, quote_free: float, adx_now: float = None,
                  rsi_now: float = None,
                  reason: str = "EMA golden cross (fast above slow)") -> None:
        """
        Market-buy the configured budget.

        The order is sized (lot step + MIN_NOTIONAL + dust risk) and re-checked
        *after* rounding, so a small balance never sends an order the exchange
        would reject. On success the position is registered for tracking (id,
        entry, TP/SL, ADX/RSI, net R:R) which drives the log and the Telegram
        trade journal.
        """
        base, quote = self.cfg["symbol"].split("/")
        plan = self.plan_buy(price, quote_free)
        if not plan["ok"]:
            self.log.warning(
                "BUY skipped: %s | %s | free %.4f %s (minNotional %.2f / minQty "
                "%.8f / budget %g %s)",
                plan["reason"], plan["hint"], quote_free, quote, self.min_cost,
                self.min_qty, self.cfg["order_size_quote"], quote,
            )
            return
        if plan["warn"]:
            self.log.warning("BUY sizing warning: %s", plan["warn"])

        qty = plan["qty"]
        if self.exchange is not None:
            # Live: let ccxt round to the exchange precision and verify the
            # *final* numbers - rounding must never slip below minQty or
            # minNotional behind our back.
            qty = float(self.exchange.amount_to_precision(self.cfg["symbol"], qty))
            if qty < self.min_qty or (self.min_cost and qty * price < self.min_cost):
                self.log.warning(
                    "BUY skipped: exchange rounding left qty=%.8f (%.4f %s) below the "
                    "symbol minimum (minQty %.8f / minNotional %.2f) - the balance is "
                    "just too small for this pair",
                    qty, qty * price, quote, self.min_qty, self.min_cost,
                )
                return

        notional = qty * price
        rr = self._net_rr()
        trade_id = self.trade_counter + 1
        position = {
            "id": trade_id,
            "opened_ts": time.time(),
            "qty": qty, "entry": price, "cost": notional,
            "reason": reason,
            "entry_adx": adx_now, "entry_rsi": rsi_now,
            "planned_rr": rr["rr"],
            "sl_exit_notional": plan["sl_exit_notional"],
            "exit_notional_ok": plan["exit_notional_ok"],
            "trail_peak": price,               # highest price since entry (trailing stop)
            # DCA bookkeeping: how many adds the position received, the extra
            # quote they cost, and the candle/price of the last add (at most one
            # add per candle).
            "dca_count": 0, "dca_spent": 0.0,
            "dca_last_ts": None, "dca_last_price": None,
        }

        if self.exchange is None:  # demo
            self.paper_usdt -= notional                     # spend quote currency
            self.paper_btc += qty * (1.0 - self.fee_rate)   # receive base minus fee
            self.log.info(
                ">> DEMO BUY %s qty=%.8f @ %s (cost %.4f %s)%s",
                self.cfg["symbol"], qty, f"{price:,.2f}", notional, quote,
                " [lot bumped up to pass minNotional]" if plan["bumped"] else "",
            )
            self._register_position(position)
            self.tg.send(self._buy_message(position, rr, quote, base, demo=True))
            return

        order = self.exchange.create_market_buy_order(self.cfg["symbol"], qty)
        fill_price = float(order.get("average") or order.get("price") or price)
        fill_qty = float(order.get("amount") or qty)
        fill_cost = float(order.get("cost") or (fill_price * fill_qty))
        position.update({"entry": fill_price, "qty": fill_qty, "cost": fill_cost})
        self.log.info(
            ">> BUY order placed  id=%s status=%s qty=%s @ %s cost=%s | %s | %s",
            order.get("id"), order.get("status"), fill_qty, f"{fill_price:,.2f}",
            f"{fill_cost:,.4f}", self._tp_sl_text(),
            "lot bumped up to pass minNotional" if plan["bumped"] else "budget sized",
        )
        self._register_position(position)
        self.tg.send(self._buy_message(position, rr, quote, base, demo=False))

    def _register_position(self, position: dict) -> None:
        """Track the freshly opened position (entry price, TP/SL, context)."""
        self.trade_counter = position["id"]
        self.position = position
        self.avg_entry_price = position["entry"]   # TP/SL + P/L anchor

    def _buy_message(self, position: dict, rr: dict, quote: str, base: str,
                     demo: bool) -> str:
        """Self-contained Telegram card for an entry (trade journal entry)."""
        entry = position["entry"]
        tp, sl = self._tp_sl_levels(entry)
        risk = position["qty"] * entry * rr["net_loss_pct"] / 100.0
        reward = position["qty"] * entry * rr["net_win_pct"] / 100.0
        rsi_txt = ("n/a" if position["entry_rsi"] is None
                   else format(float(position["entry_rsi"]), ".1f"))
        lines = [
            f"🟢 BUY #{position['id']}{' (DEMO)' if demo else ''} - {position['reason']}",
            f"{self.cfg['symbol']} @ {entry:,.2f}",
            f"Amount: {position['qty']:.8f} {base} (cost {position['cost']:,.4f} {quote})",
            f"TP: {tp:,.2f} (+{self.cfg['take_profit_pct']:g}%) | "
            f"SL: {sl:,.2f} (-{self.cfg['stop_loss_pct']:g}%)",
            f"Risk {risk:,.4f} {quote} / reward {reward:,.4f} {quote} "
            f"(R:R {rr['rr']:.2f} after costs)",
            f"Filter: ADX {self._adx_display(position['entry_adx'])} "
            f"(needs > {self.cfg['adx_threshold']:g}) | RSI {rsi_txt}",
            self._stats_line(quote),
        ]
        if self.dca_enabled and self.dca_max_entries > 0 and self.dca_drop_pct > 0:
            level = dca_trigger_level(entry, self.dca_drop_pct)
            lines.insert(
                5, f"DCA: up to {self.dca_max_entries} add(s) of "
                   f"{self.dca_size():,.2f} {quote} at -{self.dca_drop_pct:g}% "
                   f"(first trigger {level:,.2f}) instead of an immediate "
                   f"stop-out")
        if not position["exit_notional_ok"]:
            lines.insert(5, "⚠️ stop-loss exit may fall below minNotional (dust risk)")
        return "\n".join(lines)

    # -- DCA / averaging down --------------------------------------------------
    def dca_size(self) -> float:
        """Quote amount spent on one DCA add (0 -> ORDER_SIZE_QUOTE)."""
        return max(0.0, float(self.dca_size_quote or self.cfg["order_size_quote"]))

    def dca_plan(self, price: float, quote_free: float, candle_ts=None) -> dict:
        """
        Ask plan_dca() whether the open position should be averaged down now.

        A no-op plan (ok=False) when DCA is disabled, the bot is flat, the price
        is still above the trigger level, the DCA budget is used up or the
        position was already averaged down on this same candle.
        """
        pos = self.position or {}
        base, quote = self.cfg["symbol"].split("/")
        plan = plan_dca(
            price,
            avg_entry=self.avg_entry_price or 0.0,
            qty=pos.get("qty") or 0.0,
            quote_free=quote_free,
            dca_drop_pct=self.dca_drop_pct,
            dca_size_quote=self.dca_size(),
            dca_count=pos.get("dca_count") or 0,
            dca_max_entries=self.dca_max_entries,
            min_qty=self.min_qty, step=self.step, min_cost=self.min_cost,
            buffer_pct=self.cfg["min_notional_buffer_pct"],
            fee_rate=self.fee_rate,
            tp_pct=self.cfg["take_profit_pct"],
            sl_pct=self.cfg["stop_loss_pct"],
            slippage_pct=self.cfg["slippage_pct"],
            position_cost=pos.get("cost") or 0.0,
            max_position_quote=self.dca_max_position_quote,
            quote=quote, base=base,
        )
        if plan["ok"] and candle_ts is not None and pos.get("dca_last_ts") == candle_ts:
            plan["ok"] = False
            plan["added"] = False
            plan["reason"] = "already averaged down on this candle"
        return plan

    def apply_dca(self, price: float, plan: dict, candle_ts=None) -> bool:
        """
        Send the DCA market buy and fold it into the tracked position.

        The position keeps its trade id, so the journal shows one trade that was
        averaged down: quantity and cost grow, the average entry - and with it
        the TP/SL bracket - is recomputed, `dca_count` is bumped and the next
        trigger automatically sits dca_drop_pct below the new average. Returns
        True when the add was really sent.
        """
        pos = self.position
        if not pos or not plan.get("ok"):
            return False
        base, quote = self.cfg["symbol"].split("/")
        qty = plan["qty"]
        if self.exchange is not None:
            # Rounding must never slip below the exchange minimum.
            qty = float(self.exchange.amount_to_precision(self.cfg["symbol"], qty))
            if qty < self.min_qty or (self.min_cost and qty * price < self.min_cost):
                self.log.warning(
                    "DCA skipped: exchange rounding left qty=%.8f (%.4f %s) below "
                    "the symbol minimum", qty, qty * price, quote)
                return False
        old_entry, old_qty = float(pos["entry"]), float(pos["qty"])
        if self.exchange is None:  # demo
            self.paper_usdt -= qty * price
            self.paper_btc += qty * (1.0 - self.fee_rate)
            fill_price, fill_qty = price, qty
        else:
            order = self.exchange.create_market_buy_order(self.cfg["symbol"], qty)
            fill_price = float(order.get("average") or order.get("price") or price)
            fill_qty = float(order.get("amount") or qty)
        add_cost = fill_price * fill_qty
        pos["qty"] = old_qty + fill_qty
        pos["cost"] = float(pos.get("cost") or old_qty * old_entry) + add_cost
        pos["entry"] = (pos["cost"] / pos["qty"]) if pos["qty"] > 0 else old_entry
        pos["dca_count"] = int(pos.get("dca_count") or 0) + 1
        pos["dca_spent"] = float(pos.get("dca_spent") or 0.0) + add_cost
        pos["dca_last_ts"] = candle_ts
        pos["dca_last_price"] = fill_price
        tp, sl = self._tp_sl_levels(pos["entry"])
        pos["tp"], pos["sl"] = tp, sl
        self.avg_entry_price = pos["entry"]   # TP/SL + P/L anchor
        self.log.info(
            ">> DCA #%d %s added %.8f @ %s (%s %s) | avg entry %s -> %s | %s",
            pos["dca_count"], self.cfg["symbol"], fill_qty, f"{fill_price:,.2f}",
            f"{add_cost:,.4f}", quote, f"{old_entry:,.2f}",
            f"{float(pos['entry']):,.2f}", self._tp_sl_text())
        self.tg.send(self._dca_message(pos, fill_price, fill_qty, add_cost,
                                       old_entry, plan, quote, base,
                                       demo=self.exchange is None))
        return True

    def _dca_message(self, pos: dict, fill_price: float, fill_qty: float,
                     add_cost: float, old_entry: float, plan: dict, quote: str,
                     base: str, demo: bool) -> str:
        """Self-contained Telegram card for a DCA add (trade-journal note)."""
        new_entry = float(pos["entry"])
        avg_move = (new_entry / old_entry - 1.0) * 100.0 if old_entry else 0.0
        upnl = (fill_price / new_entry - 1.0) * 100.0 if new_entry else 0.0
        upnl_quote = (fill_price - new_entry) * float(pos["qty"])
        rsi_txt = ("n/a" if pos.get("rsi") is None
                   else format(float(pos["rsi"]), ".1f"))
        left = max(0, int(self.dca_max_entries) - int(pos.get("dca_count") or 0))
        lines = [
            (f"DCA #{pos['dca_count']}/{self.dca_max_entries}"
             f"{' (DEMO)' if demo else ''} - {self.cfg['symbol']} #{pos.get('id')}"),
            (f"Trigger: price {fill_price:,.2f} is -{plan.get('drop_pct', 0.0):.2f}% "
             f"below the average entry (DCA_DROP_PERCENT={self.dca_drop_pct:g}%)"),
            (f"Added: {fill_qty:.8f} {base} @ {fill_price:,.2f} "
             f"(cost {add_cost:,.4f} {quote})"),
            (f"Avg entry: {old_entry:,.2f} -> {new_entry:,.2f} ({avg_move:+.2f}%) | "
             f"position size {float(pos['cost']):,.4f} {quote}"),
            self._tp_sl_text(),
            (f"Unrealised: {upnl_quote:+.4f} {quote} ({upnl:+.2f}%) | DCA left: "
             f"{left} | filters ADX {self._adx_display(pos.get('entry_adx'))} "
             f"RSI {rsi_txt}"),
            self._stats_line(quote),
        ]
        return "\n".join(lines)

    def place_sell(self, price: float, base_free: float,
                   reason: str = "SELL signal (death cross)") -> None:
        """
        Market-sell the position (full exit).

        Only the quantity the bot bought is sold (SELL_ONLY_TRACKED_QTY), so a
        pre-existing balance - e.g. testnet faucet BTC - is never touched. The
        exit is *never* blocked by the entry risk gates: protecting capital
        always comes first.
        """
        base, quote = self.cfg["symbol"].split("/")
        tracked = (self.position or {}).get("qty")
        qty = self._sellable_qty(base_free)
        if self.cfg["sell_only_tracked_qty"] and tracked:
            qty = min(qty, self._sellable_qty(tracked))
        if qty < self.min_qty:
            self.log.info(
                "SELL skipped: sellable quantity %.8f %s is below minQty %.8f",
                qty, base, self.min_qty,
            )
            return
        if self.min_cost and qty * price < self.min_cost:
            self.log.warning(
                "SELL skipped: proceeds %.4f %s are below minNotional %.2f %s - the "
                "position is unsellable dust. Buy a slightly larger quantity or use a "
                "pair with a lower minNotional next time.",
                qty * price, quote, self.min_cost, quote,
            )
            return

        # Snapshot the TP/SL line before _close_position_pl() clears the entry.
        tp_sl_txt = self._tp_sl_text()

        if self.exchange is None:  # demo
            self.paper_btc -= qty                          # give up the base asset
            self.paper_usdt += qty * price * (1.0 - self.fee_rate)  # minus fee
            pl_txt = self._close_position_pl(price, qty, quote, reason)
            self.log.info(
                ">> DEMO SELL %s qty=%.8f @ %s (position closed) | reason=%s",
                self.cfg["symbol"], qty, f"{price:,.2f}", reason,
            )
            self.tg.send(self._sell_message(price, qty, quote, base, reason,
                                            tp_sl_txt, pl_txt, demo=True))
            return

        qty = float(self.exchange.amount_to_precision(self.cfg["symbol"], qty))
        if qty < self.min_qty:
            self.log.warning("SELL skipped: qty %.8f < minQty after rounding", qty)
            return
        order = self.exchange.create_market_sell_order(self.cfg["symbol"], qty)
        fill_price = float(order.get("average") or order.get("price") or price)
        fill_qty = float(order.get("amount") or qty)
        proceeds = float(order.get("cost") or (fill_price * fill_qty))
        self.log.info(
            ">> SELL order placed  id=%s status=%s qty=%s @ %s proceeds=%s | reason=%s",
            order.get("id"), order.get("status"), fill_qty, f"{fill_price:,.2f}",
            f"{proceeds:,.4f}", reason,
        )
        pl_txt = self._close_position_pl(fill_price, fill_qty, quote, reason)
        self.tg.send(self._sell_message(fill_price, fill_qty, quote, base, reason,
                                        tp_sl_txt, pl_txt, demo=False))

    def _sell_message(self, price: float, qty: float, quote: str, base: str,
                      reason: str, tp_sl_txt: str, pl_txt: str,
                      demo: bool) -> str:
        """Self-contained Telegram card for an exit (journal + reason + P/L)."""
        trade_no = self.trade_counter
        tag = f"#{trade_no}" if trade_no else ""
        return (
            f"🔴 SELL {tag}{' (DEMO)' if demo else ''} - {reason}\n"
            f"{self.cfg['symbol']} @ {price:,.2f}\n"
            f"Amount: {qty:.8f} {base} (proceeds {qty * price:,.4f} {quote})\n"
            f"{tp_sl_txt}\n"
            f"{pl_txt}"
        )

    def _act_on_signal(self, signal, price, in_position, quote_free, base_free, df,
                       adx_now: float = None, rsi_now: float = None,
                       candle_ts: int = None):
        """
        Execute the strategy signal - once per candle, and only when it flips.

        A BUY also has to pass the account-protection gates (daily loss limit,
        losing streak, cooldown after a stop-out). A SELL - the opposite EMA
        cross - is always allowed: never trap the position in a losing trade.
        """
        candle_ts = int(df["ts"].iloc[-1]) if candle_ts is None else int(candle_ts)
        if self.last_signal_candle == candle_ts:
            return  # already acted on this candle

        if signal == "BUY" and not in_position:
            allowed, why = self._entry_gate(candle_ts)
            if not allowed:
                self.log.info("BUY skipped by risk gate: %s", why)
                if self.day_pnl < 0 and not self._day_limit_notified:
                    self._day_limit_notified = True
                    self.tg.send(
                        "⛔ New entries paused\n"
                        f"Reason: {why}\n"
                        "Open positions stay protected - TP/SL exits keep running.\n"
                        f"{self._stats_line(self.cfg['symbol'].split('/')[1])}"
                    )
                self.last_signal_candle = candle_ts
                return
            self.place_buy(price, quote_free, adx_now=adx_now, rsi_now=rsi_now,
                           reason=f"EMA{self.cfg['ema_fast']} crossed above "
                                  f"EMA{self.cfg['ema_slow']} (golden cross)")
            self.last_signal_candle = candle_ts
        elif signal == "SELL" and in_position:
            self.place_sell(price, base_free,
                            reason=f"EMA{self.cfg['ema_fast']} crossed below "
                                   f"EMA{self.cfg['ema_slow']} (death cross)")
            self.last_signal_candle = candle_ts
        # HOLD -> do nothing.

    # -- main loop ------------------------------------------------------------
    def _tick(self):
        """One full strategy iteration: data -> indicators -> signal -> trade."""
        try:
            symbol = self.cfg["symbol"]
            base, quote = symbol.split("/")

            price, df = self.get_market_snapshot()
            df = compute_indicators(df, self.cfg)
            # Signals are read from completed candles only: the newest candle is
            # still forming while we poll, so its crossover can still vanish.
            closed_only = self.cfg["signal_on_closed_candle"] and len(df) > 2
            signal_df = df.iloc[:-1] if closed_only else df
            signal, rsi_now, adx_now, reason = current_signal(
                df, self.cfg["adx_threshold"], closed_only=closed_only,
                rsi_max=self.rsi_max_entry, rsi_min=self.rsi_min_entry,
            )

            balances = self.get_balances()
            base_free = float(balances[base]["free"])
            quote_free = float(balances[quote]["free"])
            in_position = base_free >= self.min_qty

            ema_fast = signal_df["ema_fast"].iloc[-1]
            ema_slow = signal_df["ema_slow"].iloc[-1]
            candle_ts = int(signal_df["ts"].iloc[-1])
            tp, sl = self._tp_sl_levels()
            tp_txt = f"tp={tp:,.2f}" if tp else "tp=n/a"
            sl_txt = f"sl={sl:,.2f}" if sl else "sl=n/a"

            # One-time startup self-check + the daily risk-book rollover.
            if not self._sizing_reported:
                self._sizing_reported = True
                self._log_sizing_report(price, quote_free, base_free)
            if self._roll_day_if_needed(price, quote_free, base_free, quote):
                self.tg.send(
                    f"🗓️ New UTC day {self.day_key} - daily risk counters reset "
                    f"(equity {quote_free + base_free * price:,.4f} {quote})"
                )

            pos_txt = "position=flat"
            if in_position and self.avg_entry_price:
                upnl = (price / self.avg_entry_price - 1.0) * 100.0
                pos_txt = (f"position=#{self.trade_counter} LONG "
                           f"entry={self.avg_entry_price:,.2f} uPnL={upnl:+.2f}%")
            elif in_position:
                pos_txt = "position=LONG (untracked, opened before this session)"

            self.log.info(
                "price=%.2f | EMA%d=%.2f EMA%d=%.2f | RSI%d=%.2f | ADX%d=%.2f "
                "(filter>%g) | signal=%-4s (%s, %s candles) | %s | "
                "free=%.4f %s free=%.8f %s | %s %s",
                price, self.cfg["ema_fast"], ema_fast, self.cfg["ema_slow"], ema_slow,
                self.cfg["rsi_period"], rsi_now, self.cfg["adx_period"], adx_now,
                self.cfg["adx_threshold"], signal, reason,
                "closed" if closed_only else "live",
                pos_txt, quote_free, quote, base_free, base, tp_txt, sl_txt,
            )

            # Risk management, in strict priority order. Every price level is
            # checked against the LIVE price on every poll (they can fire in the
            # middle of a 5m candle), independently of the once-per-candle
            # indicator gating further down:
            #   1. hard Take-Profit         -> close (profit is always taken)
            #   2. trailing stop (if armed) -> close (profit is locked in)
            #   3. DCA add (if enabled)     -> buy more instead of stopping out
            #   4. hard Stop-Loss           -> close
            #   5. EMA entry/exit signal    -> once per candle
            tracked = bool(in_position and self.avg_entry_price
                           and self.avg_entry_price > 0)
            handled = False
            if tracked:
                exit_reason = self._tp_exit_reason(price)
                if exit_reason is None:
                    exit_reason = self._trailing_exit_reason(price)
                if exit_reason:
                    self._close_now(price, base_free, candle_ts, exit_reason)
                    handled = True
                elif self.dca_enabled:
                    plan = self.dca_plan(price, quote_free, candle_ts)
                    if plan["ok"]:
                        if self.apply_dca(price, plan, candle_ts):
                            # If the drop was deeper than the add could
                            # compensate for, the recomputed stop still fires.
                            after = self._tp_sl_exit_reason(price)
                            if after:
                                self._close_now(price, base_free, candle_ts, after)
                            handled = True
                    elif plan["trigger"]:
                        self.log.info("DCA not taken: %s", plan["reason"])

            if not handled:
                stop_hit = self._stop_loss_exit_reason(price) if tracked else None
                if stop_hit:
                    self._close_now(price, base_free, candle_ts, stop_hit)
                else:
                    # Indicator-driven entry/exit - still once per candle.
                    self._act_on_signal(signal, price, in_position, quote_free,
                                        base_free, df, adx_now=adx_now,
                                        rsi_now=rsi_now, candle_ts=candle_ts)
            if self.failures:
                # A full tick succeeded again -> whatever broke has recovered.
                self.tg.send(
                    f"🟢 Reconnected after {self.failures} failure(s) - trading resumed."
                )
            self.failures = 0

        except ccxt.NetworkError as err:
            # Timeout / connection / rate-limit problems come back on their own.
            self._recover("network", err)
        except (ccxt.AuthenticationError, ccxt.PermissionDenied) as err:
            # Wrong keys or locked account - keep looping so a fix is picked up.
            self._recover("credentials", err)
        except (ccxt.InvalidOrder, ccxt.InsufficientFunds, ccxt.ExchangeError) as err:
            # Bad order or balance issue - log and skip this candle.
            self._recover("order/account", err)
        except Exception as err:  # last resort - never let the bot crash
            self.log.exception("Unexpected error (bot keeps running): %s", err)
            self._recover("unexpected", err)

    def _recover(self, kind: str, err: Exception) -> None:
        """Log the failure and sleep with exponential backoff (max 60s)."""
        self.failures += 1
        wait = min(self.cfg["poll_seconds"] * (2 ** min(self.failures - 1, 4)), 60)
        self.log.warning(
            "[%12s issue] %s | consecutive failures=%d -> retrying in %.0fs",
            kind, err, self.failures, wait,
        )
        self._notify_error(kind, err, wait)
        time.sleep(wait)

    def _notify_error(self, kind: str, err: Exception, wait: float) -> None:
        """
        Push critical errors / network disconnections to Telegram, but at most
        once per 5 minutes for the same kind of problem so a long outage does
        not spam the chat.
        """
        if not self.tg.enabled:
            return
        now = time.time()
        last_kind, last_ts = self._last_error_notify
        if kind == last_kind and (now - last_ts) < 300:
            return
        self._last_error_notify = (kind, now)
        self.tg.send(
            "🔴 Trading bot problem\n"
            f"Type: {kind}\n"
            f"Error: {err}\n"
            f"Next retry in {wait:.0f}s - the bot keeps running."
        )

    def close(self) -> None:
        """
        Release the exchange resources (idempotent, never raises).

        ccxt's synchronous client has nothing to shut down, but an async ccxt
        client does - keeping the call in the lifecycle makes the bot safe to
        embed: `try: bot.run() finally: bot.close()`.
        """
        if self.exchange is None:
            return
        closer = getattr(self.exchange, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception as err:  # closing must never break shutdown
                self.log.debug("exchange.close() failed (ignored): %s", err)

    def _run_deadline(self):
        """Shared RUN_FOR_SECONDS handling - see run_deadline()."""
        return run_deadline(self.cfg, self.log)

    def run(self):
        """
        Start the blocking poll loop (the classic production entry point).

        RUN_FOR_SECONDS is a TEST-ONLY convenience: in DEMO_MODE the loop may
        stop itself after N seconds. In production mode the limit is IGNORED -
        the bot runs 24/7 and only the operator can stop it (Ctrl+C).
        """
        cfg = self.cfg
        deadline = self._run_deadline()

        self.log.info(
            "Startup complete. Polling every %ds. Press Ctrl+C to stop.",
            cfg["poll_seconds"],
        )

        while True:
            try:
                self._tick()
                time.sleep(cfg["poll_seconds"])
            except KeyboardInterrupt:
                self.log.info("Keyboard interrupt received - shutting down cleanly.")
                break
            if deadline is not None and time.time() >= deadline:
                self.log.info("TEST RUN over (RUN_FOR_SECONDS) - shutting down cleanly.")
                break

    async def run_async(self):
        """
        Async twin of run() for asyncio applications.

        The strategy, the ccxt calls and the Telegram posts are all blocking, so
        each tick is executed in a worker thread (`loop.run_in_executor`) while
        only the *waiting* happens on the event loop. The loop therefore stays
        responsive (health endpoints, other tasks, a websocket feed, ...) while
        the bot trades exactly like `run()` does, and a slow Telegram retry can
        never freeze the application. Cancelling the task (Ctrl+C /
        asyncio.run shutdown) stops the bot cleanly between ticks - call
        close() afterwards, or use the module-level `run_async()` helper which
        does it for you.

        The strategy core (compute_indicators, current_signal, plan_market_buy,
        reward_risk_after_costs) is pure and synchronous, so it can also be
        awaited from your own code, e.g.:

            signal, rsi, adx, why = await asyncio.to_thread(
                current_signal, df, 25.0, True)
        """
        cfg = self.cfg
        deadline = self._run_deadline()
        loop = asyncio.get_running_loop()   # run_in_executor = 3.7+ compatible
        self.log.info(
            "Startup complete (asyncio). Polling every %ds. Ctrl+C to stop.",
            cfg["poll_seconds"],
        )
        while True:
            await loop.run_in_executor(None, self._tick)
            if deadline is not None and time.time() >= deadline:
                self.log.info("TEST RUN over (RUN_FOR_SECONDS) - shutting down cleanly.")
                return
            await asyncio.sleep(max(0.0, float(cfg["poll_seconds"])))


# ------------------------------------------------------------------------------
# Multi-coin / all-pairs portfolio scanner
# ------------------------------------------------------------------------------
class MultiCoinScanner:
    """
    Dynamically scans and trades a whole universe of spot pairs in parallel.

    Instead of trading one SYMBOL, the scanner:

      1. discovers every active spot pair quoted in SCAN_QUOTE (default USDT)
         from ccxt's load_markets(), optionally filtered to high 24h volume,
         minus SCAN_EXCLUDE and capped to SCAN_MAX_SYMBOLS - or uses an explicit
         SCANNER_SYMBOLS list;
      2. sizes every entry *proportionally*: the free USDT balance is split
         into PORTFOLIO_PARTS equal budgets (the "21 parts" rule from the
         reference video), each part is floored to the pair's minNotional so a
         small ($10 .. $50+) account still places real, exit-viable orders;
      3. applies the same EMA + ADX(>=25) trend filter as the single-symbol bot,
         plus a hard Take-Profit, a Stop-Loss and an optional Trailing-Stop per
         open position;
      4. refreshes per-symbol price/candle data in parallel using a thread pool,
         so scanning 20+ pairs never serialises into one long blocking crawl.

    The blocking ccxt / Telegram work always happens in worker threads, so both
    the classic `run()` loop and the asyncio `run_async()` twin keep the event
    loop responsive while scanning many pairs -- the asyncio twin uses
    `loop.run_in_executor` exactly like the single-symbol bot does.

    DEMO_MODE=true runs the exact same loop on synthetic data with a simulated
    balance, so the scanner can be tested offline without API keys.
    """

    def __init__(self, cfg: dict, exchange=None):
        self.cfg = cfg
        self.log = logging.getLogger("scanner")
        self.exchange = exchange            # injectable for tests; None = demo
        self.lock = threading.RLock()       # guards positions + balances
        self.positions = {}                 # symbol -> open position dict
        self.closed_trades = []
        self.trade_counter = 0
        self.symbol_rules = {}              # symbol -> (min_qty, min_cost, step)
        self.symbols = []                   # the resolved scan universe
        self.paper_usdt = max(0.0, float(cfg.get("initial_balance_usdt", 10.0) or 0.0))
        self.paper_base = {}                # demo: symbol -> base free balance
        self.demo_dfs = {}                  # demo: symbol -> synthetic candle df
        self.failures = 0
        self.last_signal_candle = {}        # symbol -> last acted-upon candle ts
        self.tg = TelegramNotifier(
            cfg.get("telegram_bot_token", ""), cfg.get("telegram_chat_id", ""), self.log,
            max_attempts=cfg.get("telegram_max_attempts", 3),
            retry_delays=cfg.get("telegram_retry_delays", (2.0, 4.0)),
            queue_retry_seconds=cfg.get("telegram_queue_retry_seconds", 30.0),
            queue_max_age=cfg.get("telegram_queue_max_age", 1800.0),
        )
        self.bnb_discount_pct = max(0.0, min(
            25.0, float(cfg.get("bnb_fee_discount_pct", 0.0) or 0.0)))
        # Portfolio sizing knots (pulled from cfg with sensible defaults).
        self.quote = (cfg.get("scan_quote") or "USDT").upper()
        self.parts = max(1, int(cfg.get("portfolio_parts", 21) or 21))
        self.floor_usdt = max(0.0, float(cfg.get("portfolio_floor_usdt", 0.0) or 0.0))
        self.min_24h_quote = max(0.0, float(cfg.get("scan_min_24h_quote", 0.0) or 0.0))
        self.max_symbols = max(1, int(cfg.get("scan_max_symbols", 30) or 30))
        self.exclude = set((cfg.get("scan_exclude") or []))
        self.explicit = [normalize_symbol(s, self.quote) for s in
                         str(cfg.get("scanner_symbols", "")).split(",") if s.strip()]
        self.trailing_pct = max(0.0, float(cfg.get("trailing_stop_pct", 0.0) or 0.0))
        self.trailing_arm_pct = max(0.0, float(cfg.get("trailing_arm_pct", 0.5) or 0.0))
        # DCA / averaging down (same rules as the single-symbol bot; see
        # plan_dca). Per position, so every coin has its own add budget.
        self.dca_enabled = bool(cfg.get("dca_enabled", False))
        self.dca_drop_pct = max(0.0, float(cfg.get("dca_drop_pct", 1.5) or 0.0))
        self.dca_size_quote = max(0.0, float(cfg.get("dca_size_quote", 0.0) or 0.0))
        self.dca_max_entries = max(0, int(cfg.get("dca_max_entries", 0) or 0))
        self.dca_max_position_quote = max(
            0.0, float(cfg.get("dca_max_position_quote", 0.0) or 0.0))
        # Optional RSI entry filter (0 = off), same as the single-symbol bot.
        self.rsi_max_entry = max(0.0, float(cfg.get("rsi_max_entry", 0.0) or 0.0))
        self.rsi_min_entry = max(0.0, float(cfg.get("rsi_min_entry", 0.0) or 0.0))
        self.use_all_pct = max(0.0, float(cfg.get("use_all_balance_pct", 100.0) or 100.0))
        self.buffer_pct = max(0.0, float(cfg.get("min_notional_buffer_pct", 0.0) or 0.0))
        self.require_exit = bool(cfg.get("require_exit_viable", False))
        self.closed_only = bool(cfg.get("signal_on_closed_candle", True))

        if not cfg.get("demo_mode", False):
            if not self.exchange:
                if not cfg.get("api_key") or not cfg.get("api_secret"):
                    raise RuntimeError(
                        "Missing testnet API keys. Either add BINANCE_TESTNET_API_KEY / "
                        "BINANCE_TESTNET_API_SECRET to your .env file, or set DEMO_MODE=true."
                    )
                self.log.info("Creating ccxt Binance exchange object for scanner ...")
                self.exchange = ccxt.binance({
                    "apiKey": cfg["api_key"],
                    "secret": cfg["api_secret"],
                    "enableRateLimit": True,
                    "timeout": 20000,
                    "options": {
                        "defaultType": "spot",
                        "adjustForTimeDifference": True,
                        "fetchMarkets": ["spot"],
                    },
                })
                self.exchange.set_sandbox_mode(True)
            self._init_live()
        else:
            self._init_demo()

    def _init_live(self):
        """Connect / reuse a ccxt exchange; load markets and the scan universe."""
        ex = self.exchange
        if ex is None:
            raise RuntimeError("MultiCoinScanner requires an exchange (or DEMO_MODE)")
        self.log.info("Scanner: loading spot markets from the testnet ...")
        markets = ex.load_markets()
        try:
            self.log.info("Scanner: fetching 24h tickers to sort by trading volume ...")
            tickers = ex.fetch_tickers()
            for sym, tick in (tickers or {}).items():
                if sym in markets and isinstance(tick, dict):
                    markets[sym]["stats"] = tick
        except Exception as err:
            self.log.warning("Scanner: fetch_tickers failed (%s); volume sort will use market stats.", err)
        self.symbol_rules = {s: symbol_rules(ex, s) for s in markets}
        self.symbols = self._resolve_symbols(markets)
        shown = ", ".join(self.symbols[:8]) + ("..." if len(self.symbols) > 8 else "")
        self.log.info("Scanner universe: %d spot %s pair(s) [%s]",
                      len(self.symbols), self.quote, shown or self.quote)

    def _resolve_symbols(self, markets: dict) -> list:
        """Explicit SCANNER_SYMBOLS win; otherwise dynamic + volume filter + cap."""
        if self.explicit:
            return [s for s in self.explicit if s in markets] or self.explicit
        found = discover_spot_usdt_symbols(
            markets, quote=self.quote, min_24h_quote=self.min_24h_quote,
            exclude=self.exclude, max_symbols=0)
        found = [s for s in found if s.upper() not in {e.upper() for e in self.exclude}]
        if self.max_symbols and self.max_symbols > 0:
            found = found[:self.max_symbols]
        return found

    def _init_demo(self):
        """Synthetic offline universe: a few fake pairs advanced as random walks."""
        self.log.warning(
            "SCANNER DEMO_MODE=true -> simulating every pair locally (no exchange).")
        if self.explicit:
            self.symbols = [s if s.endswith("/" + self.quote) else s
                            for s in self.explicit]
        else:
            n = min(self.max_symbols, 5) if self.max_symbols and self.max_symbols <= 5 else 5
            bases = ["BTC", "ETH", "SOL", "BNB", "XRP"][:n]
            self.symbols = [f"{base}/{self.quote}" for base in bases]
        ms = self._timeframe_ms()
        limit = int(self.cfg.get("candle_limit", 150))
        for sym in self.symbols:
            base, _ = sym.split("/")
            self.paper_base.setdefault(base, 0.0)
            rows, price = [], 60_000.0 if base == "BTC" else 3000.0
            now = int(time.time() * 1000)
            for i in range(limit):
                ts = now - (limit - i) * ms
                open_ = price
                price = max(1e-6, open_ * (1.0 + random.gauss(0.0, 0.002)))
                high = max(open_, price) * (1.0 + random.random() * 0.001)
                low = min(open_, price) * (1.0 - random.random() * 0.001)
                rows.append([ts, open_, high, low, price, random.uniform(0.5, 20.0)])
            self.demo_dfs[sym] = pd.DataFrame(
                rows, columns=["ts", "open", "high", "low", "close", "volume"])
            self.symbol_rules[sym] = (0.0001, 10.0, 1e-8)

    # -- small helpers --------------------------------------------------------
    def _timeframe_ms(self) -> int:
        unit = self.cfg["timeframe"][-1]
        mult = int(self.cfg["timeframe"][:-1] or "1")
        secs = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}.get(unit, 60)
        return mult * secs * 1000

    def _net_rr(self) -> dict:
        fee = effective_fee_rate(self.cfg.get("fee_rate", 0.001), self.bnb_discount_pct)
        return reward_risk_after_costs(
            self.cfg["take_profit_pct"], self.cfg["stop_loss_pct"], fee,
            self.cfg["slippage_pct"])

    def _tp_sl_levels(self, entry):
        if not entry or entry <= 0:
            return None, None
        return (entry * (1.0 + self.cfg["take_profit_pct"] / 100.0),
                entry * (1.0 - self.cfg["stop_loss_pct"] / 100.0))

    def get_quote_balance(self) -> float:
        """Free balance in the scan quote currency (live or simulated)."""
        if self.exchange is None:
            return self.paper_usdt
        bal = self.exchange.fetch_balance()
        return float((bal.get(self.quote, {}) or {}).get("free") or 0.0)

    def get_base_balance(self, symbol: str) -> float:
        base, _ = symbol.split("/")
        if self.exchange is None:
            return self.paper_base.get(base, 0.0)
        bal = self.exchange.fetch_balance()
        return float((bal.get(base, {}) or {}).get("free") or 0.0)

    def get_market_snapshot(self, symbol: str):
        """Return (price, candles_df) for one symbol. Live or simulated."""
        if self.exchange is None:
            df = self.demo_dfs[symbol].copy()
            last = float(df["close"].iloc[-1])
            open_ = last
            close = max(1e-6, open_ * (1.0 + random.gauss(0.0, 0.0015)))
            high = max(open_, close) * (1.0 + random.random() * 0.0008)
            low = min(open_, close) * (1.0 - random.random() * 0.0008)
            vol = random.uniform(0.5, 20.0)
            ts = int(df["ts"].iloc[-1]) + self._timeframe_ms()
            new_row = pd.DataFrame([[ts, open_, high, low, close, vol]],
                                   columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)
            df = df.iloc[-int(self.cfg.get("candle_limit", 150)):].reset_index(drop=True)
            df["ts"] = df["ts"].astype("int64")
            self.demo_dfs[symbol] = df
            return close, df
        ticker = self.exchange.fetch_ticker(symbol)
        price = float(ticker.get("last") or ticker.get("close") or 0.0)
        ohlcv = self.exchange.fetch_ohlcv(
            symbol, timeframe=self.cfg["timeframe"], limit=self.cfg["candle_limit"])
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = df[col].astype("float64")
        df["ts"] = df["ts"].astype("int64")
        return price, df

    def _fetch_snapshots(self, symbols):
        """Fetch (price, df) for many symbols in parallel via a thread pool.

        This is what makes multi-coin scanning fast: 20+ exchange round-trips run
        concurrently instead of one long serial crawl, and each one happens off
        the asyncio event loop (worker threads), so the loop never blocks.
        """
        if not symbols:
            return {}
        if self.exchange is None:
            return {s: self.get_market_snapshot(s) for s in symbols}
        limit = max(1, min(16, len(symbols)))   # keep the rate limiter sane
        out = {}

        def _one(symbol):
            try:
                return symbol, self.get_market_snapshot(symbol)
            except Exception:
                return symbol, None

        with ThreadPoolExecutor(max_workers=limit) as pool:
            for symbol, snap in pool.map(_one, symbols):
                if snap is not None:
                    out[symbol] = snap
        return out

    # -- order execution ------------------------------------------------------
    def _place_buy(self, symbol: str, price: float, quote_free: float,
                   budget: float, adx_now=None, rsi_now=None, reason="BUY signal"):
        """Market-buy `budget` quote of `symbol`, respecting its lot rules."""
        min_qty, min_cost, step = self.symbol_rules.get(symbol, (1e-8, 0.0, 1e-8))
        fee = self._fee()
        plan = plan_market_buy(
            price, quote_free, order_size_quote=budget, min_qty=min_qty, step=step,
            min_cost=min_cost, buffer_pct=self.buffer_pct, fee_rate=fee,
            tp_pct=self.cfg["take_profit_pct"], sl_pct=self.cfg["stop_loss_pct"],
            slippage_pct=self.cfg["slippage_pct"],
            require_exit_viable=self.require_exit,
            quote=self.quote, base=symbol.split("/")[0])
        if not plan["ok"]:
            self.log.info("[scan] BUY skip %s: %s (%s)", symbol, plan["reason"], plan["hint"])
            return
        qty, notional = plan["qty"], plan["notional"]
        rr = self._net_rr()
        tp, sl = self._tp_sl_levels(price)
        with self.lock:
            self.trade_counter += 1
            self.positions[symbol] = {
                "id": self.trade_counter, "symbol": symbol, "qty": qty,
                "entry": price, "cost": notional, "tp": tp, "sl": sl,
                "trail_peak": price, "opened_ts": time.time(),
                "reason": reason, "adx": adx_now, "rsi": rsi_now,
                "planned_rr": rr["rr"],
                # DCA bookkeeping: adds received, extra quote spent, and the
                # candle/price of the last add (at most one add per candle).
                "dca_count": 0, "dca_spent": 0.0,
                "dca_last_ts": None, "dca_last_price": None,
            }
        if self.exchange is None:
            self.paper_usdt -= notional
            self.paper_base[symbol.split("/")[0]] += qty * (1.0 - fee)
            self.log.info(">> [scan] DEMO BUY %s qty=%.8f @ %s (cost %.4f %s)",
                          symbol, qty, f"{price:,.2f}", notional, self.quote)
        else:
            order = self.exchange.create_market_buy_order(symbol, qty)
            fill_price = float(order.get("average") or order.get("price") or price)
            fill_qty = float(order.get("amount") or qty)
            self.positions[symbol]["entry"] = fill_price
            self.positions[symbol]["qty"] = fill_qty
            self.positions[symbol]["cost"] = float(order.get("cost") or fill_price * fill_qty)
            fill_tp, fill_sl = self._tp_sl_levels(fill_price)
            self.positions[symbol]["tp"], self.positions[symbol]["sl"] = fill_tp, fill_sl
            self.log.info(">> [scan] BUY %s id=%s qty=%s @ %s cost=%s",
                          symbol, order.get("id"), fill_qty, f"{fill_price:,.2f}",
                          f"{self.positions[symbol]['cost']:,.4f}")
        self._notify_trade("BUY", symbol, self.positions[symbol])

    def _place_sell(self, symbol: str, price: float, qty: float, reason: str):
        """Market-sell `qty` of `symbol` and record the closed trade."""
        min_qty, min_cost, step = self.symbol_rules.get(symbol, (1e-8, 0.0, 1e-8))
        qty = max(0.0, min(float(qty), self.get_base_balance(symbol)))
        qty = floor_to_step(qty, step)
        if qty < min_qty:
            self.log.info("[scan] SELL skip %s: qty %.8f < minQty", symbol, qty)
            return
        if min_cost and qty * price < min_cost:
            self.log.warning("[scan] SELL skip %s: proceeds %.4f %s < minNotional %.2f",
                             symbol, qty * price, self.quote, min_cost)
            return
        with self.lock:
            pos = self.positions.pop(symbol, None)
        if self.exchange is None:
            self.paper_base[symbol.split("/")[0]] -= qty
            self.paper_usdt += qty * price * (1.0 - self._fee())
            fill_price, fill_qty = price, qty
        else:
            order = self.exchange.create_market_sell_order(symbol, qty)
            fill_price = float(order.get("average") or order.get("price") or price)
            fill_qty = float(order.get("amount") or qty)
        entry = pos.get("entry") if pos else 0.0
        pnl = (fill_price - entry) * fill_qty
        if pos:
            self.closed_trades.append({
                "symbol": symbol, "id": pos.get("id"), "qty": fill_qty,
                "entry": entry, "exit": fill_price, "pnl_quote": pnl,
                "reason": reason, "closed_ts": time.time(),
            })
        self.log.info(">> [scan] SELL %s qty=%.8f @ %s pnl=%+.4f %s | %s",
                      symbol, fill_qty, f"{fill_price:,.2f}", pnl, self.quote, reason)
        if pos:
            self._notify_trade("SELL", symbol, pos, pnl=pnl, reason=reason)

    # -- DCA / averaging down --------------------------------------------------
    def _dca_size(self) -> float:
        """
        Quote amount spent on one DCA add.

        DCA_SIZE_QUOTE wins; when it is 0 the portfolio floor (one proportional
        slot) is used so a small account still averages down with a properly
        sized, minNotional-clearing order.
        """
        if self.dca_size_quote > 0:
            return self.dca_size_quote
        return max(0.0, float(self.floor_usdt or 0.0))

    def _dca_plan(self, symbol: str, price: float, quote_free: float,
                  candle_ts=None) -> dict:
        """Ask plan_dca() whether this coin should be averaged down now."""
        pos = self.positions.get(symbol) or {}
        min_qty, min_cost, step = self.symbol_rules.get(symbol, (1e-8, 0.0, 1e-8))
        base, _ = symbol.split("/")
        plan = plan_dca(
            price,
            avg_entry=pos.get("entry") or 0.0,
            qty=pos.get("qty") or 0.0,
            quote_free=quote_free,
            dca_drop_pct=self.dca_drop_pct,
            dca_size_quote=self._dca_size(),
            dca_count=pos.get("dca_count") or 0,
            dca_max_entries=self.dca_max_entries,
            min_qty=min_qty, step=step, min_cost=min_cost,
            buffer_pct=self.buffer_pct, fee_rate=self._fee(),
            tp_pct=self.cfg["take_profit_pct"], sl_pct=self.cfg["stop_loss_pct"],
            slippage_pct=self.cfg["slippage_pct"],
            position_cost=pos.get("cost") or 0.0,
            max_position_quote=self.dca_max_position_quote,
            quote=self.quote, base=base,
        )
        if plan["ok"] and candle_ts is not None and pos.get("dca_last_ts") == candle_ts:
            plan["ok"] = False
            plan["added"] = False
            plan["reason"] = "already averaged down on this candle"
        return plan

    def _apply_dca(self, symbol: str, price: float, plan: dict,
                   candle_ts=None) -> bool:
        """
        Send the DCA add for `symbol` and fold it into that coin's position.

        The average entry (and therefore the TP/SL bracket) is recomputed, so
        the position gets more room exactly like in the single-symbol bot.
        Returns True when the add was really sent.
        """
        pos = self.positions.get(symbol)
        if not pos or not plan.get("ok"):
            return False
        base, _ = symbol.split("/")
        min_qty, min_cost, _step = self.symbol_rules.get(symbol, (1e-8, 0.0, 1e-8))
        qty = plan["qty"]
        if self.exchange is not None:
            qty = float(self.exchange.amount_to_precision(symbol, qty))
            if qty < min_qty or (min_cost and qty * price < min_cost):
                self.log.warning(
                    "[scan] DCA skip %s: exchange rounding left qty=%.8f below "
                    "the symbol minimum", symbol, qty)
                return False
        old_entry, old_qty = float(pos["entry"]), float(pos["qty"])
        if self.exchange is None:
            self.paper_usdt -= qty * price
            self.paper_base[base] = (self.paper_base.get(base, 0.0)
                                     + qty * (1.0 - self._fee()))
            fill_price, fill_qty = price, qty
        else:
            order = self.exchange.create_market_buy_order(symbol, qty)
            fill_price = float(order.get("average") or order.get("price") or price)
            fill_qty = float(order.get("amount") or qty)
        add_cost = fill_price * fill_qty
        with self.lock:
            pos["qty"] = old_qty + fill_qty
            pos["cost"] = float(pos.get("cost") or old_qty * old_entry) + add_cost
            pos["entry"] = (pos["cost"] / pos["qty"]) if pos["qty"] > 0 else old_entry
            pos["dca_count"] = int(pos.get("dca_count") or 0) + 1
            pos["dca_spent"] = float(pos.get("dca_spent") or 0.0) + add_cost
            pos["dca_last_ts"] = candle_ts
            pos["dca_last_price"] = fill_price
            pos["tp"], pos["sl"] = self._tp_sl_levels(pos["entry"])
        self.log.info(
            ">> [scan] DCA #%d %s added %.8f @ %s (%s %s) | avg entry %s -> %s"
            " | new TP: %s SL: %s",
            pos["dca_count"], symbol, fill_qty, f"{fill_price:,.2f}",
            f"{add_cost:,.4f}", self.quote, f"{old_entry:,.2f}",
            f"{float(pos['entry']):,.2f}", f"{float(pos['tp']):,.2f}",
            f"{float(pos['sl']):,.2f}")
        self._notify_dca(symbol, pos, fill_price, fill_qty, add_cost, old_entry,
                         plan)
        return True

    def _notify_dca(self, symbol: str, pos: dict, fill_price: float,
                    fill_qty: float, add_cost: float, old_entry: float,
                    plan: dict) -> None:
        """Optional Telegram card for a scanner DCA add (no-op when disabled)."""
        if not self.tg.enabled:
            return
        try:
            base, _ = symbol.split("/")
            new_entry = float(pos["entry"])
            avg_move = (new_entry / old_entry - 1.0) * 100.0 if old_entry else 0.0
            upnl = (fill_price / new_entry - 1.0) * 100.0 if new_entry else 0.0
            left = max(0, self.dca_max_entries - int(pos.get("dca_count") or 0))
            body = [
                f"DCA #{pos['dca_count']}/{self.dca_max_entries} [{symbol}] "
                f"#{pos.get('id')}",
                f"Trigger: {fill_price:,.2f} is -{plan.get('drop_pct', 0.0):.2f}% "
                f"below the average entry (DCA_DROP_PERCENT={self.dca_drop_pct:g}%)",
                f"Added: {fill_qty:.8f} {base} (cost {add_cost:,.4f} {self.quote})",
                f"Avg entry: {old_entry:,.2f} -> {new_entry:,.2f} ({avg_move:+.2f}%)"
                f" | position size {float(pos['cost']):,.4f} {self.quote}",
                f"TP: {float(pos['tp']):,.2f} | SL: {float(pos['sl']):,.2f}",
                f"Unrealised: {(fill_price - new_entry) * float(pos['qty']):+.4f} "
                f"{self.quote} ({upnl:+.2f}%) | DCA left: {left}",
            ]
            self.tg.send("\n".join(body) + "\n" + self._stats_line())
        except Exception as err:
            self.log.debug("[telegram] scanner DCA card skipped: %s", err)
    def _fee(self) -> float:
        return effective_fee_rate(self.cfg.get("fee_rate", 0.001), self.bnb_discount_pct)

    def _notify_trade(self, kind, symbol, pos, pnl=None, reason=""):
        """Optional Telegram card for a scanner entry/exit (no-op when disabled)."""
        if not self.tg.enabled:
            return
        try:
            header = (f"🟢 BUY [{symbol}] #{pos['id']} - {pos.get('reason')}"
                      if kind == "BUY" else
                      f"🔴 SELL [{symbol}] #{pos.get('id')} - {reason}")
            rr = self._net_rr()
            base, _ = symbol.split("/")
            body = [header, f"{symbol} @ {pos['entry']:,.2f}",
                    f"Amount: {pos['qty']:.8f} {base} "
                    f"(cost {float(pos.get('cost') or 0.0):,.4f} {self.quote})",
                    f"TP: {float(pos['tp']):,.2f} (+{self.cfg['take_profit_pct']:g}%)"
                    f" | SL: {float(pos['sl']):,.2f} (-{self.cfg['stop_loss_pct']:g}%)",
                    f"Filters: ADX {self._fmt(pos.get('adx'))} "
                    f"(needs > {self.cfg['adx_threshold']:g}) | "
                    f"RSI {self._fmt(pos.get('rsi'))}",
                    f"Risk/reward R:R {rr['rr']:.2f} after costs"]
            if kind == "BUY" and self.dca_enabled and self.dca_max_entries > 0:
                body.insert(5, f"DCA: up to {self.dca_max_entries} add(s) of "
                               f"{self._dca_size():,.2f} {self.quote} at "
                               f"-{self.dca_drop_pct:g}% instead of an immediate "
                               f"stop-out")
            if pnl is not None:
                body.append(f"P/L: {pnl:+.4f} {self.quote}")
            self.tg.send("\n".join(body) + "\n" + self._stats_line())
        except Exception as err:
            self.log.debug("[telegram] scanner card skipped: %s", err)

    def _stats_line(self) -> str:
        with self.lock:
            n = len(self.closed_trades)
            wins = sum(1 for t in self.closed_trades if t["pnl_quote"] > 0)
            pnl = sum(t["pnl_quote"] for t in self.closed_trades)
            open_n = len(self.positions)
        return (f"Portfolio: {open_n} open | {n} closed ({wins}W) | "
                f"session P/L {pnl:+.4f} {self.quote}")

    # -- the portfolio cycle --------------------------------------------------
    def _scan_once(self):
        """One full multi-coin cycle: balance -> proportional budget -> signals.

        Exits (TP / SL / trailing stop / EMA death cross) run first and are never
        gated. Entries run next, once per candle, only when EMA golden cross AND
        ADX >= threshold, sized to one proportional slice of the free balance.
        """
        try:
            if not self.symbols:
                self.log.warning("[scan] no symbols to scan - is SCANNER_SYMBOLS empty?")
                return
            quote_free = self.get_quote_balance()
            with self.lock:
                reserved = sum(p.get("cost", 0.0) or 0.0
                               for p in self.positions.values())
            deploy = quote_free * max(0.0, self.use_all_pct) / 100.0
            prop = compute_proportional_budget(
                deploy, parts=self.parts, floor_usdt=self.floor_usdt)
            per_part = prop["per_part"]
            self.log.info(
                "[scan] free %s %s | deploy %s -> %d/%d part(s) @ %s %s each "
                "(reserved %s by %d open)",
                f"{quote_free:,.4f}", self.quote, f"{deploy:,.2f}",
                prop["usable_parts"], prop["parts"], f"{per_part:,.4f}",
                self.quote, f"{reserved:,.4f}", len(self.positions))

            snapshots = self._fetch_snapshots(self.symbols)
            for symbol in self.symbols:
                if symbol not in snapshots:
                    continue
                price, df = snapshots[symbol]
                if price <= 0 or df is None or len(df) < 2:
                    continue
                df = compute_indicators(df, self.cfg)
                closed_only = self.closed_only and len(df) > 2
                signal, rsi, adx, _why = current_signal(
                    df, self.cfg["adx_threshold"], closed_only=closed_only,
                    rsi_max=self.rsi_max_entry, rsi_min=self.rsi_min_entry)
                candle_ts = int(df["ts"].iloc[-1])
                min_qty, min_cost, _ = self.symbol_rules.get(symbol, (1e-8, 0.0, 1e-8))
                base_free = self.get_base_balance(symbol)
                in_position = symbol in self.positions and base_free >= min_qty

                # 1) Profit exits first (take-profit + trailing stop): never
                #    gated by anything, and they always win over a DCA add.
                if in_position:
                    exit_reason = self._exit_reason(symbol, price, signal,
                                                    include_sl=False,
                                                    include_ema=False)
                    if exit_reason is None and self.dca_enabled:
                        # 2) DCA: average down instead of stopping out. The hard
                        #    stop is still untouched at this point (plan_dca
                        #    refuses to add once it is broken).
                        plan = self._dca_plan(symbol, price, quote_free, candle_ts)
                        if plan["ok"]:
                            self._apply_dca(symbol, price, plan, candle_ts)
                        elif plan["trigger"]:
                            self.log.info("[scan] DCA not taken %s: %s",
                                          symbol, plan["reason"])
                    if exit_reason is None:
                        # 3) Hard stop-loss / EMA death cross - never gated, and
                        #    re-evaluated against the recomputed bracket after an
                        #    add (a crash deeper than the DCA can offset exits).
                        exit_reason = self._exit_reason(symbol, price, signal)
                    if exit_reason:
                        self._place_sell(symbol, price,
                                         self.positions[symbol]["qty"], exit_reason)
                        self.last_signal_candle[symbol] = candle_ts
                    continue

                # 2) proportional entry, once per candle.
                if signal != "BUY":
                    continue
                if self.last_signal_candle.get(symbol) == candle_ts:
                    continue
                # `deploy` is the share of the balance this cycle may invest
                # (USE_ALL_BALANCE_PCT), `reserved` is what the open positions
                # already hold - so the rest stays free for DCA adds and fees
                # instead of being fully invested.
                available = max(0.0, quote_free - reserved)
                budget = per_part if per_part > 0 else available
                budget = min(budget, available, max(0.0, deploy - reserved))
                if budget < min_cost * (1.0 + self.buffer_pct / 100.0) and min_cost > 0:
                    self.log.info(
                        "[scan] BUY skip %s: proportional part %s %s < minNotional %s",
                        symbol, f"{budget:,.4f}", self.quote, f"{min_cost:,.2f}")
                    continue
                self._place_buy(symbol, price, available, budget,
                                adx_now=adx, rsi_now=rsi,
                                reason=f"EMA golden cross (ADX {self._fmt(adx)})")
                self.last_signal_candle[symbol] = candle_ts
                with self.lock:
                    reserved = sum(p.get("cost", 0.0) or 0.0
                                   for p in self.positions.values())

            if self.failures:
                self.tg.send(f"🟢 Scanner reconnected after {self.failures} failure(s).")
            self.failures = 0
        except ccxt.NetworkError as err:
            self._recover("network", err)
        except (ccxt.AuthenticationError, ccxt.PermissionDenied) as err:
            self._recover("credentials", err)
        except Exception as err:
            self.log.exception("[scan] unexpected error (scanner keeps running): %s", err)
            self._recover("unexpected", err)

    def _exit_reason(self, symbol: str, price: float, ema_signal: str,
                     include_sl: bool = True, include_ema: bool = True) -> str:
        """
        Close reason when a hard TP/SL/trailing level or an EMA SELL fires.

        `include_sl` / `include_ema` let the scan cycle ask for the *profit*
        exits only (take-profit + trailing stop) first: those always win over a
        DCA add, while the stop-loss and the EMA death cross are evaluated right
        after the add, against the recomputed bracket.
        """
        pos = self.positions.get(symbol)
        if not pos or pos.get("entry") is None or pos["entry"] <= 0:
            return ""
        entry = pos["entry"]
        tp, sl = self._tp_sl_levels(entry)
        if tp and price >= tp:
            return (f"TAKE-PROFIT +{self.cfg['take_profit_pct']:g}% hit "
                    f"(target {tp:,.2f})")
        if include_sl and sl and price <= sl:
            return (f"STOP-LOSS -{self.cfg['stop_loss_pct']:g}% hit "
                    f"(stop {sl:,.2f})")
        if self.trailing_pct > 0:
            prev_peak = float(pos.get("trail_peak") or entry)
            pos["trail_peak"] = max(prev_peak, price)
            if trailing_stop_hit(entry, prev_peak, price, self.trailing_pct,
                                 self.trailing_arm_pct):
                level = trailing_stop_level(entry, max(prev_peak, price),
                                            self.trailing_pct)
                return (f"TRAILING-STOP -{self.trailing_pct:g}% hit "
                        f"(peak {max(prev_peak, price):,.2f} -> stop {level:,.2f})")
        if include_ema and ema_signal == "SELL":
            return "EMA death cross (fast crossed below slow)"
        return ""

    @staticmethod
    def _fmt(value) -> str:
        try:
            if value is None or pd.isna(value):
                return "n/a"
            return f"{float(value):.1f}"
        except (TypeError, ValueError):
            return "n/a"

    def _recover(self, kind: str, err: Exception) -> None:
        self.failures += 1
        wait = min(max(5.0, self.cfg.get("scan_poll_seconds", 60)) *
                   (2 ** min(self.failures - 1, 4)), 300)
        self.log.warning("[scan %s issue] %s | failures=%d -> retrying in %.0fs",
                          kind, err, self.failures, wait)
        time.sleep(wait)

    def run(self):
        """Blocking portfolio loop (classic entry point); Ctrl+C stops cleanly.

        RUN_FOR_SECONDS is honoured exactly like in the single-symbol bot:
        a DEMO run may stop itself, in production the timer is ignored.
        """
        cfg = self.cfg
        deadline = run_deadline(cfg, self.log)
        self.log.info("Scanner running. Polling %d symbol(s) every %ds. Ctrl+C to stop.",
                      len(self.symbols), cfg.get("scan_poll_seconds", 60))
        while True:
            try:
                self._scan_once()
                time.sleep(max(0.0, float(cfg.get("scan_poll_seconds", 60))))
            except KeyboardInterrupt:
                self.log.info("Keyboard interrupt received - shutting the scanner down.")
                break
            if deadline is not None and time.time() >= deadline:
                self.log.info("TEST RUN over (RUN_FOR_SECONDS) - shutting down cleanly.")
                break

    async def run_async(self):
        """Asyncio twin of run(): blocking scans run in a worker thread via
        loop.run_in_executor, so the event loop stays responsive while the
        scanner crawls many pairs."""
        cfg = self.cfg
        deadline = run_deadline(cfg, self.log)
        loop = asyncio.get_running_loop()
        self.log.info("Scanner running (asyncio). Polling %d symbol(s) every %ds.",
                      len(self.symbols), cfg.get("scan_poll_seconds", 60))
        while True:
            await loop.run_in_executor(None, self._scan_once)
            if deadline is not None and time.time() >= deadline:
                self.log.info("TEST RUN over (RUN_FOR_SECONDS) - shutting down cleanly.")
                return
            await asyncio.sleep(max(0.0, float(cfg.get("scan_poll_seconds", 60))))

    def close(self) -> None:
        """Release exchange resources (idempotent, never raises)."""
        ex = self.exchange
        if ex is None:
            return
        closer = getattr(ex, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception as err:
                self.log.debug("scanner exchange.close() failed (ignored): %s", err)


# ------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------
async def run_async(cfg: dict = None) -> None:
    """
    Run the bot inside the *current* asyncio event loop.

    Convenience wrapper for embedding the bot in an async application:

        import asyncio, trading_bot as t
        asyncio.run(t.run_async())          # or: await t.run_async(cfg)

    Every blocking exchange / Telegram call happens in a worker thread, so the
    event loop is never blocked; the exchange is closed on the way out.
    """
    cfg = cfg or load_config()
    setup_logging(cfg["log_level"])
    bot = BinanceTestnetBot(cfg)
    try:
        await bot.run_async()
    finally:
        bot.close()


def main(use_async: bool = None) -> int:
    """
    Entry point: the classic blocking loop, optionally driven by asyncio.

        python trading_bot.py            -> blocking loop (bot.run())
        python trading_bot.py --async    -> asyncio loop (bot.run_async())
        trading_bot.main(use_async=True) -> embedded / automated use
    """
    if use_async is None:
        use_async = any(arg in ("--async", "--asyncio") for arg in sys.argv[1:])
    cfg = load_config()
    use_scan = (cfg["scan_enabled"]
                or any(arg in ("--scan", "--multi", "--all-pairs")
                       for arg in sys.argv[1:]))
    setup_logging(cfg["log_level"])
    log = logging.getLogger("bot")

    log.info("=" * 62)
    log.info("  Binance Spot Testnet Trading Bot")
    log.info("=" * 62)
    log.info("Mode     : %s",
             "LOCAL DEMO SIMULATION" if cfg["demo_mode"]
             else "LIVE TESTNET (paper money @ testnet.binance.vision)")
    log.info("Symbol   : %s   Timeframe: %s (%s candles)", cfg["symbol"], cfg["timeframe"],
             "completed only" if cfg["signal_on_closed_candle"] else "incl. the live one")
    log.info("Strategy : EMA(%d, %d) crossover + RSI(%d) + ADX(%d) trend filter > %g",
             cfg["ema_fast"], cfg["ema_slow"], cfg["rsi_period"], cfg["adx_period"],
             cfg["adx_threshold"])
    log.info("Risk     : Take-Profit +%g%% | Stop-Loss -%g%% (auto, exits always on)",
             cfg["take_profit_pct"], cfg["stop_loss_pct"])
    if cfg["trailing_stop_pct"] > 0:
        log.info("Trail    : -%g%% from the post-entry peak (arms above +%g%%)",
                 cfg["trailing_stop_pct"], cfg["trailing_arm_pct"])
    else:
        log.info("Trail    : off (TRAILING_STOP_PCT=0)")
    if cfg["dca_enabled"] and cfg["dca_max_entries"] > 0:
        _q = cfg["symbol"].split("/")[1]
        log.info("DCA      : up to %d add(s) of %g %s at -%g%% below the average "
                 "entry | per-coin cap %s",
                 cfg["dca_max_entries"],
                 (cfg["dca_size_quote"] or cfg["order_size_quote"]), _q,
                 cfg["dca_drop_pct"],
                 (f"{cfg['dca_max_position_quote']:g} {_q}"
                  if cfg["dca_max_position_quote"] > 0 else "free balance only"))
    else:
        log.info("DCA      : off")
    log.info("RSI gate : %s", (
        f"skip a BUY when RSI > {cfg['rsi_max_entry']:g}"
        + (f" or RSI < {cfg['rsi_min_entry']:g}" if cfg["rsi_min_entry"] > 0 else "")
        if (cfg["rsi_max_entry"] > 0 or cfg["rsi_min_entry"] > 0) else "off"))
    log.info("Risk caps: daily loss %g%% | %d losses in a row | %d candle(s) cooldown "
             "after a stop-out",
             cfg["max_daily_loss_pct"], cfg["max_consecutive_losses"],
             cfg["loss_cooldown_candles"])
    log.info("Order    : %g %s per BUY (minNotional + lot step enforced)",
             cfg["order_size_quote"], cfg["symbol"].split("/")[1])

    if cfg["ema_fast"] >= cfg["ema_slow"]:
        log.warning(
            "EMA_FAST_PERIOD (%d) should be smaller than EMA_SLOW_PERIOD (%d).",
            cfg["ema_fast"], cfg["ema_slow"],
        )
    if cfg["take_profit_pct"] <= 0 or cfg["stop_loss_pct"] <= 0:
        log.warning(
            "TAKE_PROFIT_PCT / STOP_LOSS_PCT must both be > 0; using %.2f%% / %.2f%%.",
            cfg["take_profit_pct"], cfg["stop_loss_pct"],
        )
    if not cfg["demo_mode"] and (not cfg["api_key"] or not cfg["api_secret"]):
        log.error(
            "No API keys found! Create a .env file (see .env.example) or set "
            "DEMO_MODE=true to simulate."
        )
        return 1

    if use_scan:
        log.info("Mode     : %s (MULTI-COIN SCANNER)",
                 "LOCAL DEMO SIMULATION" if cfg["demo_mode"]
                 else "LIVE TESTNET (paper money @ testnet.binance.vision)")
        log.info("Scanner  : %d proportional part(s) of the %s balance | quote %s | "
                 "floor %.2f %s | BNB fee discount %.1f%% | trailing stop %.1f%%",
                 cfg["portfolio_parts"], cfg["scan_quote"], cfg["scan_quote"],
                 cfg["portfolio_floor_usdt"], cfg["scan_quote"],
                 cfg["bnb_fee_discount_pct"], cfg["trailing_stop_pct"])
        _list = [normalize_symbol(s, cfg["scan_quote"]) for s in
                 str(cfg["scanner_symbols"]).split(",") if s.strip()]
        log.info("Coins    : %s",
                 ", ".join(_list) if _list
                 else f"dynamic discovery of every liquid /{cfg['scan_quote']} pair")
        if cfg["dca_enabled"] and cfg["dca_max_entries"] > 0:
            log.info("DCA      : up to %d add(s) of %g %s per coin at -%g%% below "
                     "the average entry | per-coin cap %s",
                     cfg["dca_max_entries"],
                     (cfg["dca_size_quote"] or cfg["portfolio_floor_usdt"]),
                     cfg["scan_quote"], cfg["dca_drop_pct"],
                     (f"{cfg['dca_max_position_quote']:g} {cfg['scan_quote']}"
                      if cfg["dca_max_position_quote"] > 0 else "free balance only"))
        else:
            log.info("DCA      : off")
        log_env_conflicts(log)
        try:
            scanner = MultiCoinScanner(cfg)
        except Exception as err:
            log.error("Failed to initialise the scanner: %s", err)
            return 1
        scanner.tg.verify()
        try:
            if use_async:
                asyncio.run(scanner.run_async())
            else:
                scanner.run()
        except KeyboardInterrupt:
            log.info("Keyboard interrupt received - shutting the scanner down.")
        finally:
            scanner.close()
        return 0

    try:
        bot = BinanceTestnetBot(cfg)
    except Exception as err:  # e.g. ccxt.NotSupported / network during setup
        log.error("Failed to initialise the bot: %s", err)
        return 1

    # Startup self-checks: report any stale shell variables that lost to .env,
    # prove the Telegram token is real right away (clear ERROR when it is not),
    # then say hello - so a misconfiguration can never stay silent.
    log_env_conflicts(log)
    bot.tg.verify()
    bot.notify_startup()  # optional Telegram push (no-op when not configured)

    if use_async:
        log.info(
            "Event loop mode: asyncio - blocking exchange/Telegram calls run in a "
            "worker thread, so the event loop stays responsive."
        )
    try:
        if use_async:
            asyncio.run(bot.run_async())
        else:
            bot.run()
    except KeyboardInterrupt:
        log.info("Keyboard interrupt received - shutting down cleanly.")
    finally:
        bot.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
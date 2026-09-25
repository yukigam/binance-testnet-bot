"""Quick smoke test for trading_bot.py - runs WITHOUT any network access."""
import asyncio
import io
import logging
import math
import os
import sys
import tempfile
import threading
import time

if hasattr(sys.stdout, "reconfigure"):  # PASS lines contain emoji - keep them printable
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import ccxt
import pandas as pd
import requests

import trading_bot as t

failures: list = []


def check(name, cond):
    print(("PASS" if cond else "FAIL") + " | " + name)
    if not cond:
        failures.append(name)


# 1) Sandbox mode must route to the Spot Testnet, not mainnet.
ex = ccxt.binance({"options": {"defaultType": "spot"}})
ex.set_sandbox_mode(True)
public_url = ex.urls["api"]["public"]
check("sandbox -> testnet.binance.vision", "testnet.binance.vision" in public_url)


# 2) EMA + ADX signal detection on synthetic data (no network needed).
def mk(prices):
    n = len(prices)
    return pd.DataFrame(
        {
            "ts": [i * 5 * 60000 for i in range(n)],  # simulated 5m candles
            "open": prices, "high": prices, "low": prices,
            "close": prices, "volume": [1.0] * n,
        }
    )


cfg_sig = {"ema_fast": 8, "ema_slow": 21, "rsi_period": 14,
           "adx_period": 14, "adx_threshold": 20.0}

# A clean, long uptrend so EMA/RSI/ADX are all fully computed.
trend = [100.0 + i * 0.5 for i in range(60)]

# --- Golden cross, gated by a STRONG ADX => BUY -------------------------------
df_up = t.compute_indicators(mk(trend), cfg_sig)
n = len(df_up)
# Force a golden cross to happen exactly on the last candle...
df_up.loc[n - 2, "ema_fast"] = 55.0; df_up.loc[n - 2, "ema_slow"] = 60.0
df_up.loc[n - 1, "ema_fast"] = 65.0; df_up.loc[n - 1, "ema_slow"] = 60.0
# ...with a known, deterministically strong ADX.
df_up.loc[:, "adx"] = 30.0
sig, rsi, adx, reason = t.current_signal(df_up, cfg_sig["adx_threshold"])
check("golden cross + strong ADX -> BUY", sig == "BUY")
check("signal returns the ADX value", abs(float(adx) - 30.0) < 1e-6)
check("RSI within [0,100]", 0.0 <= float(rsi) <= 100.0)

# --- Golden cross with WEAK ADX => filtered to HOLD ---------------------------
df_weak = df_up.copy()
df_weak.loc[:, "adx"] = 5.0  # below the 20 threshold
sig2, _, adx2, reason2 = t.current_signal(df_weak, cfg_sig["adx_threshold"])
check("golden cross + weak ADX -> HOLD (trend filter)",
      sig2 == "HOLD" and "ADX" in str(reason2))

# --- Death cross => SELL, never gated by ADX (risk exit allowed) --------------
df_down = t.compute_indicators(mk(trend), cfg_sig)
m = len(df_down)
df_down.loc[m - 2, "ema_fast"] = 60.0; df_down.loc[m - 2, "ema_slow"] = 55.0
df_down.loc[m - 1, "ema_fast"] = 45.0; df_down.loc[m - 1, "ema_slow"] = 55.0
df_down.loc[:, "adx"] = 5.0  # weak trend on purpose - SELL must still fire
sig3, _, _, _ = t.current_signal(df_down, cfg_sig["adx_threshold"])
check("death cross -> SELL (no trend gate)", sig3 == "SELL")

# --- Flat market -> HOLD -------------------------------------------------------
sig4, _, _, _ = t.current_signal(t.compute_indicators(mk([100] * 12), cfg_sig),
                                 cfg_sig["adx_threshold"])
check("flat market -> HOLD", sig4 == "HOLD")

# --- The ADX trend filter itself: 25 (default) rejects what 20 used to accept ---
df_adx22 = df_up.copy()
df_adx22.loc[:, "adx"] = 22.0            # a weak-ish trend
check("ADX 22 below the new 25 threshold -> HOLD",
      t.current_signal(df_adx22, 25.0)[0] == "HOLD")
check("ADX 22 above the old 20 threshold -> BUY (filter is configurable)",
      t.current_signal(df_adx22, 20.0)[0] == "BUY")
check("current_signal() defaults to the 25 trend filter",
      t.current_signal(df_adx22)[0] == "HOLD")

# --- Closed-candle signals: no repainting / fakeout entries --------------------
# A crossover that only exists on the newest (still forming) candle must be
# ignored in closed-candle mode - it can disappear before that candle closes.
# Fixture: EMA fast <= slow on the two completed candles (k-3, k-2), then the
# cross appears on the live candle k-1.
df_live_cross = t.compute_indicators(mk(trend), cfg_sig)
k = len(df_live_cross)
df_live_cross.loc[k - 3, "ema_fast"] = 50.0; df_live_cross.loc[k - 3, "ema_slow"] = 60.0
df_live_cross.loc[k - 2, "ema_fast"] = 55.0; df_live_cross.loc[k - 2, "ema_slow"] = 60.0
df_live_cross.loc[k - 1, "ema_fast"] = 65.0; df_live_cross.loc[k - 1, "ema_slow"] = 60.0
df_live_cross.loc[:, "adx"] = 30.0
sig_live, _, _, _ = t.current_signal(df_live_cross, 25.0, closed_only=False)
sig_closed, _, _, why_closed = t.current_signal(df_live_cross, 25.0, closed_only=True)
check("live-candle crossover -> BUY when closed_only=False", sig_live == "BUY")
check("live-candle crossover ignored with closed_only=True", sig_closed == "HOLD")
check("closed_candle hold is explained in the reason", "crossover" in str(why_closed))

# ...and a cross that has fully completed (one candle back) does fire.
df_shifted_cross = df_live_cross.copy()
df_shifted_cross.loc[k - 3, "ema_fast"] = 55.0; df_shifted_cross.loc[k - 3, "ema_slow"] = 60.0
df_shifted_cross.loc[k - 2, "ema_fast"] = 65.0; df_shifted_cross.loc[k - 2, "ema_slow"] = 60.0
df_shifted_cross.loc[k - 1, "ema_fast"] = 66.0; df_shifted_cross.loc[k - 1, "ema_slow"] = 60.0
check("cross on the last completed candle -> BUY with closed_only=True",
      t.current_signal(df_shifted_cross, 25.0, closed_only=True)[0] == "BUY")

# --- Net reward/risk maths after fees + slippage --------------------------------
rr_costs = t.reward_risk_after_costs(2.5, 1.0, 0.001, 0.05)
check("round-trip cost = 2 fees + 2 slippages (0.30%)",
      abs(rr_costs["cost_pct"] - 0.30) < 1e-9)
check("net win/loss = +2.20% / -1.30% after costs",
      abs(rr_costs["net_win_pct"] - 2.20) < 1e-9
      and abs(rr_costs["net_loss_pct"] - 1.30) < 1e-9)
check("R:R after costs ~1.69", abs(rr_costs["rr"] - 1.6923) < 0.001)
check("break-even win rate ~37%", abs(rr_costs["break_even_win_rate"] - 37.1) < 0.5)
check("a TP that cannot cover the costs is detected",
      t.reward_risk_after_costs(0.2, 1.0, 0.001, 0.05)["net_win_pct"] < 0)

# --- ADX itself is computed (warm-up then becomes finite) ----------------------
df_adx = t.compute_indicators(mk(trend), cfg_sig)
check("ADX computed after warm-up", bool(not df_adx["adx"].tail(5).isna().any()))
check("ADX inside [0,100]", bool(df_adx["adx"].iloc[-1] <= 100.0))


# 3) Demo-mode paper trading balances.
os.environ["DEMO_MODE"] = "true"
bot = t.BinanceTestnetBot(t.load_config())
_lvl = bot.log.level  # silence the DISABLED warning of the intentional no-op below
bot.log.setLevel(logging.ERROR)
bot.tg = t.TelegramNotifier("", "", bot.log)  # no-op notifier: smoke test stays chat-quiet
bot.log.setLevel(_lvl)
bot.place_buy(60000.0, 1000.0)
btc = bot.get_balances()["BTC"]["free"]
usdt = bot.get_balances()["USDT"]["free"]
check("demo BUY adds BTC", btc > 0.0)
check("demo BUY spends USDT", usdt < 1000.0)

bot.place_sell(61000.0, btc)
# After the 0.1% fee the received BTC is no longer a whole lot, so a tiny
# sub-lot dust (< 1e-6, far below the 0.0001 minQty) may be left over.
check("demo SELL empties BTC (only sub-lot dust remains)",
      bot.get_balances()["BTC"]["free"] < 1e-6)
check("demo SELL returns USDT", bot.get_balances()["USDT"]["free"] > usdt)


# --- 3b) 10 USDT balance: config, minNotional gate and fee residuals -----------
# The bot is validated against a real 10 USDT testnet account, so the demo
# must start at 10.00 USDT, reject BUYs below Binance's ~10 USDT minNotional,
# refuse orders that cannot be exited, and still apply the 0.1% taker fee.
cfg10 = t.load_config()  # reads .env (ORDER_SIZE_QUOTE=10, INITIAL_BALANCE_USDT=10.0)
check("config order size = 10 USDT", abs(cfg10["order_size_quote"] - 10.0) < 1e-9)
check("config initial balance = 10 USDT", abs(cfg10["initial_balance_usdt"] - 10.0) < 1e-9)

bot10 = t.BinanceTestnetBot(cfg10)
bot10.tg = t.TelegramNotifier("", "", bot10.log)  # stay chat-quiet
b = bot10.get_balances()
check("demo starts with exactly 10.00 USDT", abs(b["USDT"]["free"] - 10.0) < 1e-9)
check("demo starts with 0 BTC", b["BTC"]["free"] < 1e-9)

# BUY below the minNotional boundary (~10 USDT) is rejected, balance unchanged.
bot10.place_buy(60000.0, 5.0)
check("BUY below minNotional is skipped", abs(bot10.get_balances()["USDT"]["free"] - 10.0) < 1e-9)

# A budget that lands EXACTLY on minNotional cannot be traded: flooring the
# quantity to the lot step leaves 9.9996 USDT of order value, and Binance checks
# minNotional against the *rounded* order. The bot must refuse it and explain
# why instead of sending an order the exchange would reject.
plan_edge = bot10.plan_buy(60000.0, 10.0)
check("10.00 USDT balance -> entry refused (rounded order < minNotional)",
      plan_edge["ok"] is False and "minNotional" in plan_edge["reason"])
check("refusal names the balance this pair really needs",
      plan_edge["min_balance"] > 10.0 and "need about" in plan_edge["hint"])

# NOTE: this symbol's minNotional is 10 USDT, so a 10.00 USDT order can never be
# exited again (fees would push the sell below minNotional). 10.2 USDT is the
# smallest budget that survives its own fees - exactly what the startup
# "safe minimum account" line reports - so the exit test uses that account.
exit_bot = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
exit_bot.tg = t.TelegramNotifier("", "", exit_bot.log)
exit_bot.paper_usdt = 10.2          # the account was topped up to 10.2 USDT
exit_bot.place_buy(60000.0, 10.2)
usdt_after_buy = exit_bot.get_balances()["USDT"]["free"]
btc10 = exit_bot.get_balances()["BTC"]["free"]
check("BUY accepted once the balance can pay for a minNotional-clearing order",
      0.0 <= usdt_after_buy < 1.0)
check("order value is really >= minNotional (checked AFTER rounding)",
      btc10 / (1.0 - cfg10["fee_rate"]) * 60000.0 >= exit_bot.min_cost - 1e-9)
check("fee shrank the received BTC below the ordered quantity",
      btc10 < exit_bot.position["qty"])
check("order is exit-viable (stop-loss sell stays above minNotional)",
      exit_bot.position["exit_notional_ok"] is True)

# SELL-back: USDT returns the sold (lot-rounded) proceeds minus the 0.1% fee,
# and BTC is emptied up to a sub-lot dust amount.
sold_qty = math.floor(btc10 / exit_bot.step) * exit_bot.step  # what place_sell sells
exit_bot.place_sell(60000.0, btc10)
after10 = exit_bot.get_balances()
check("SELL empties BTC (only sub-lot dust remains)", after10["BTC"]["free"] < 1e-6)
expected_credit = usdt_after_buy + sold_qty * 60000.0 * (1.0 - cfg10["fee_rate"])
check("SELL credits quote minus taker fee",
      abs(after10["USDT"]["free"] - expected_credit) < 1e-6)


# 3d) Exchange-minimum-aware sizing, dust protection and account risk gates -----
# The rules: floor the quantity to the lot step, bump ONE step up when flooring
# would break minNotional (only when the balance can pay for it), refuse orders
# that cannot be exited safely, and always explain WHICH rule refused a trade.
SIZING = dict(order_size_quote=10.0, min_qty=0.0001, step=1e-8, min_cost=10.0,
              fee_rate=0.001, tp_pct=2.5, sl_pct=1.0, slippage_pct=0.05)

p_edge = t.plan_market_buy(60000.0, 10.0, **SIZING)
check("sizing: exactly-minNotional balance is refused",
      p_edge["ok"] is False and "minNotional" in p_edge["reason"])
check("sizing: refusal states the balance the pair really needs",
      p_edge["min_balance"] > 10.0 and "need about" in p_edge["hint"])

p_bump = t.plan_market_buy(60000.0, 10.05, **SIZING)
check("sizing: one lot step up clears minNotional",
      p_bump["ok"] is True and p_bump["bumped"] is True
      and p_bump["notional"] >= 10.0 and p_bump["notional"] <= 10.05)
check("sizing: dust risk detected when the stop-loss exit < minNotional",
      p_bump["exit_notional_ok"] is False and "dust" in p_bump["warn"])

p_safe = t.plan_market_buy(60000.0, 10.2, **dict(SIZING, order_size_quote=10.2))
check("sizing: a bigger budget is exit-viable (no dust warning)",
      p_safe["ok"] is True and p_safe["exit_notional_ok"] is True
      and p_safe["warn"] == "")

p_block = t.plan_market_buy(60000.0, 10.05, **dict(SIZING, require_exit_viable=True))
check("sizing: REQUIRE_EXIT_VIABLE=true blocks a non-exitable order",
      p_block["ok"] is False and "stop-loss exit" in p_block["reason"])

check("sizing: balance below minNotional is refused",
      t.plan_market_buy(60000.0, 4.0, **SIZING)["ok"] is False)
check("sizing: quantity below minQty is refused",
      t.plan_market_buy(60000.0, 10.05, **dict(SIZING, min_qty=0.001))["ok"] is False)
check("sizing: no price -> no order",
      t.plan_market_buy(0.0, 10.0, **SIZING)["ok"] is False)
check("sizing: safe minimum balance covers fees + the -1% stop (~10.12 USDT)",
      10.10 < t.min_viable_balance(10.0, 1e-8, 60000.0, 0.001, 1.0, 0.05) < 10.13)
check("sizing: no minNotional -> no minimum balance",
      t.min_viable_balance(0.0, 1e-8, 60000.0) == 0.0)

# -- Documented defaults (tested without the user's .env values) ----------------
_saved_env = {k: os.environ.pop(k, None) for k in (
    "TAKE_PROFIT_PCT", "STOP_LOSS_PCT", "ADX_THRESHOLD", "ADX_PERIOD",
    "SLIPPAGE_PCT", "MIN_NOTIONAL_BUFFER_PCT", "REQUIRE_EXIT_VIABLE",
    "SIGNAL_ON_CLOSED_CANDLE", "MAX_DAILY_LOSS_PCT", "MAX_CONSECUTIVE_LOSSES",
    "LOSS_COOLDOWN_CANDLES", "SELL_ONLY_TRACKED_QTY")}
try:
    _def = t.load_config()
    check("defaults: TP +2.5% / SL -1.0%",
          (_def["take_profit_pct"], _def["stop_loss_pct"]) == (2.5, 1.0))
    check("defaults: ADX trend filter > 25", _def["adx_threshold"] == 25.0)
    check("defaults: signals from closed candles only",
          _def["signal_on_closed_candle"] is True)
    check("defaults: dust risk is warned, not enforced",
          _def["require_exit_viable"] is False)
    check("defaults: risk caps on (3% daily / 3 losses / 2 candles)",
          (_def["max_daily_loss_pct"], _def["max_consecutive_losses"],
           _def["loss_cooldown_candles"]) == (3.0, 3, 2))
    check("defaults: only the bot's own quantity is sold",
          _def["sell_only_tracked_qty"] is True)
finally:
    os.environ.update({k: v for k, v in _saved_env.items() if v is not None})

# -- Risk gates: protect the account, but never block an exit -------------------
gate = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
gate.tg = t.TelegramNotifier("", "", gate.log)   # chat-quiet no-op notifier
gate.place_buy(60000.0, 10.2)
check("tracking: the open position is registered as trade #1",
      gate.position["id"] == 1
      and abs(gate.position["qty"] - gate.paper_btc / (1.0 - gate.fee_rate)) < 1e-12)
gate.day_key = "2000-01-01"
gate.day_start_equity = 10.2
gate.day_pnl = -0.4                    # more than 3% of the day's starting equity
allowed, why = gate._entry_gate(0)
check("risk gate: daily loss limit blocks new entries",
      allowed is False and "daily loss limit" in why)
gate._act_on_signal("BUY", 60000.0, False, 10.2, 0.0, df_up, candle_ts=1)
check("risk gate: a blocked BUY really opens no position", gate.trade_counter == 1)
gate._act_on_signal("SELL", 61000.0, True, 0.0, gate.paper_btc, df_up, candle_ts=2)
check("risk gate: an exit is NEVER blocked by the gates",
      gate.paper_btc < 1e-7 and gate.last_signal_candle == 2)
gate.day_pnl = 0.0
gate.consecutive_losses = gate.cfg["max_consecutive_losses"]
check("risk gate: losing streak pauses entries",
      gate._entry_gate(0)[0] is False and "losing trades" in gate._entry_gate(0)[1])
gate.consecutive_losses = 0
gate.last_loss_candle = 1000
_cooldown_span = gate.cfg["loss_cooldown_candles"] * gate._timeframe_ms()
check("risk gate: cooldown right after a stop-out",
      gate._entry_gate(1000)[0] is False and "cooldown" in gate._entry_gate(1000)[1])
check("risk gate: cooldown expires after the configured candles",
      gate._entry_gate(1000 + _cooldown_span)[0] is True)
# -- Full take-profit cycle: tracking, journal, statistics ----------------------
cyc = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
cyc.tg = t.TelegramNotifier("", "", cyc.log)
cyc.place_buy(60000.0, 10.2, adx_now=28.4, rsi_now=55.2)
check("tracking: entry context stored (ADX/RSI/reason)",
      cyc.position["entry_adx"] == 28.4 and cyc.position["entry_rsi"] == 55.2
      and "golden cross" in cyc.position["reason"])
check("tracking: the entry keeps its TP/SL levels and planned R:R",
      cyc._tp_sl_levels(cyc.avg_entry_price)[0] > cyc.avg_entry_price
      and cyc.position["planned_rr"] > 1.0)
tp_price = 60000.0 * (1.0 + cyc.cfg["take_profit_pct"] / 100.0)
tp_hit = cyc._tp_sl_exit_reason(tp_price)
check("TP level fires at +2.5%", tp_hit is not None and tp_hit.startswith("TAKE-PROFIT"))
check("inside the bracket no TP/SL exit fires",
      cyc._tp_sl_exit_reason(60000.0 * 1.01) is None)
sl_hit = cyc._tp_sl_exit_reason(60000.0 * (1.0 - cyc.cfg["stop_loss_pct"] / 100.0))
check("SL level fires at -1.0%", sl_hit is not None and sl_hit.startswith("STOP-LOSS"))
cyc.place_sell(tp_price, cyc.paper_btc, reason=tp_hit)
check("journal: the closed trade is recorded with its reason + P/L",
      len(cyc.closed_trades) == 1
      and cyc.closed_trades[0]["reason"].startswith("TAKE-PROFIT")
      and cyc.closed_trades[0]["pnl_quote"] > 0)
check("journal: session statistics count the win",
      cyc._trade_stats()["wins"] == 1 and cyc._trade_stats()["trades"] == 1)
check("journal: position + entry price cleared after the exit",
      cyc.position is None and cyc.avg_entry_price is None)
check("journal: realised P/L also lands in the daily counter", cyc.day_pnl > 0)
check("journal: the stats line reports W/L + P/L",
      "1W/0L" in cyc._stats_line("USDT") and "P/L" in cyc._stats_line("USDT"))

# -- A stop-out: loss booked, losing streak counted -----------------------------
slb = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
slb.tg = t.TelegramNotifier("", "", slb.log)
slb.place_buy(60000.0, 10.2)
stop_price = 60000.0 * (1.0 - slb.cfg["stop_loss_pct"] / 100.0)
slb.place_sell(stop_price, slb.paper_btc, reason=slb._tp_sl_exit_reason(stop_price))
check("stop-out: loss booked and the losing streak counted",
      slb.closed_trades[0]["pnl_quote"] < 0 and slb.consecutive_losses == 1)
check("stop-out: daily P/L turns negative", slb.day_pnl < 0)

# -- Dust protection on the SELL side (minNotional) -----------------------------
dust = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.0))  # dust-risk order
dust.tg = t.TelegramNotifier("", "", dust.log)
dust.place_buy(60000.0, 10.05)
check("dust: entry accepted but flagged as not exit-viable",
      dust.position is not None and dust.position["exit_notional_ok"] is False)
dust_qty = dust.paper_btc
dust.place_sell(60000.0, dust_qty)
check("dust: an exit below minNotional is refused instead of sent",
      abs(dust.paper_btc - dust_qty) < 1e-12 and dust.position is not None)

# -- Safety: only the bot's own quantity is sold --------------------------------
lim = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
lim.tg = t.TelegramNotifier("", "", lim.log)
lim.place_buy(60000.0, 10.2)
lim.paper_btc += 0.25          # e.g. BTC that was already in the testnet wallet
lim.place_sell(60000.0, lim.paper_btc)
check("safety: only the bot's own quantity is sold (the rest stays put)",
      abs(lim.paper_btc - 0.25) < 1e-6)

# ...but after a restart (no tracked quantity) a real position can still be closed.
rst = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
rst.tg = t.TelegramNotifier("", "", rst.log)
rst.paper_btc = 0.25           # position from a previous run
rst.place_sell(60000.0, rst.paper_btc)
check("safety: an untracked position can still be closed after a restart",
      rst.paper_btc < 1e-7)


# 4) Telegram notifier - never crashes the bot, never touches the real network.
class _FakeResponse:
    """Stands in for requests.Response (HTTP 200 + ok:true by default)."""

    def __init__(self, status_code=200, ok=True, text='{"ok": true}', payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload if payload is not None else {"ok": ok}

    def json(self):
        return self._payload


class _Recorder:
    """Stands in for requests.post; can succeed, fail or permanently reject."""

    def __init__(self, behave="ok"):
        self.calls = []
        self.behave = behave

    def __call__(self, url, json=None, timeout=None):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        if self.behave == "fail":
            raise requests.ConnectionError("simulated Telegram outage")
        if self.behave == "reject":
            return _FakeResponse(
                status_code=401, ok=False,
                text='{"ok":false,"error_code":401,"description":"Unauthorized"}',
            )
        return _FakeResponse()


def _drained(tg, rec, text):
    """True once *text* was really delivered and the retry queue is empty."""
    return (tg.pending_count() == 0
            and any(c["json"]["text"] == text for c in rec.calls))


# Isolated disk spools for the notifier tests (never touch the real spool file).
SPOOL = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_spool.jsonl")
SPOOL2 = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_spool2.jsonl")
for _spool_file in (SPOOL, SPOOL2, SPOOL + ".tmp", SPOOL2 + ".tmp"):
    if os.path.exists(_spool_file):
        os.remove(_spool_file)

real_post = t.requests.post
try:
    # 4a) Unconfigured notifier -> disabled, no network call at all.
    tg_off = t.TelegramNotifier("", "", bot.log)
    check("telegram unconfigured -> disabled", tg_off.enabled is False)
    check("telegram disabled send() -> no-op False", tg_off.send("hi") is False)

    # 4b) Configured notifier -> POSTs chat_id + text to the Bot API URL.
    rec = _Recorder()
    t.requests.post = rec
    tg = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=2, retry_delays=(0.05,),
        queue_retry_seconds=0.2, queue_max_age=30.0, spool_path=SPOOL,
    )
    check("telegram configured -> enabled", tg.enabled is True)
    check("telegram send ok -> True", tg.send("hello") is True)
    check("telegram payload has chat_id+text",
          rec.calls and rec.calls[0]["json"] == {"chat_id": "42", "text": "hello"})
    check("telegram url hits api.telegram.org",
          rec.calls and "api.telegram.org" in rec.calls[0]["url"])

    # 4c) Telegram outage -> exactly `max_attempts` quick inline retries, then
    #     the message is queued. No exception may escape, and the whole thing
    #     must finish fast so the trading loop is never blocked for long.
    #     (background_retry=False -> no worker thread -> exact call counting.)
    tg_nobg = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=3, retry_delays=(0.05, 0.1), background_retry=False,
        spool_path=SPOOL,
    )
    fail_rec = _Recorder(behave="fail")
    t.requests.post = fail_rec
    t0 = time.monotonic()
    sent = tg_nobg.send("hello")
    elapsed = time.monotonic() - t0
    check("telegram outage -> no crash, False", sent is False)
    check("telegram outage -> exactly 3 quick attempts", len(fail_rec.calls) == 3)
    check("telegram outage -> failed fast (loop not blocked)", elapsed < 5.0)
    check("telegram outage -> message queued for background retry",
          tg_nobg.pending_count() == 1)
    check("telegram outage -> message also persisted to the disk spool",
          os.path.exists(SPOOL)
          and any("hello" in line
                  for line in open(SPOOL, encoding="utf-8").read().splitlines()))

    # 4c2) Full production path: an outage while queueing, then the network
    #      comes back -> the background worker delivers the queued message on
    #      its own, without any help from the trading code.
    fail_rec2 = _Recorder(behave="fail")
    t.requests.post = fail_rec2
    tg_live = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=2, retry_delays=(0.05,),
        queue_retry_seconds=0.2, queue_max_age=30.0, spool_path=SPOOL2,
    )
    check("telegram outage (live path) -> False", tg_live.send("queued-test") is False)
    rec_ok = _Recorder()
    t.requests.post = rec_ok  # ...the internet comes back
    deadline = time.monotonic() + 15.0
    while (not _drained(tg_live, rec_ok, "queued-test")
           and time.monotonic() < deadline):
        time.sleep(0.05)
    check("telegram queued message delivered after recovery",
          _drained(tg_live, rec_ok, "queued-test"))

    # 4d) Order notifications (emoji + P/L) and error notifications.
    rec2 = _Recorder()
    t.requests.post = rec2
    bot2 = t.BinanceTestnetBot(t.load_config())
    bot2.tg = tg  # wire the bot to the recorder-backed notifier
    bot2.place_buy(60000.0, 1000.0)
    bot2.place_sell(61000.0, bot2.get_balances()["BTC"]["free"])
    texts = [c["json"]["text"] for c in rec2.calls]
    check("BUY telegram msg has 🟢", any("🟢" in x for x in texts))
    check("SELL telegram msg has 🔴", any("🔴" in x for x in texts))
    check("SELL telegram msg has P/L", any("P/L" in x for x in texts))
    check("BUY telegram msg identifies the trade + the reason",
          any("#1" in x and "golden cross" in x for x in texts))
    check("BUY telegram msg carries TP/SL, costs and the ADX filter",
          any("TP:" in x and "SL:" in x and "R:R" in x and "ADX" in x for x in texts))
    check("SELL telegram msg names the exit reason",
          any("death cross" in x for x in texts))
    check("SELL telegram msg tracks entry context + journal stats",
          any("At entry:" in x and "Session:" in x and "held" in x for x in texts))

    bot2.cfg["poll_seconds"] = 0  # so _recover() does not really sleep
    bot2._recover("network", Exception("simulated network down"))
    check("error -> 🔴 telegram msg",
          any("🔴" in c["json"]["text"] for c in rec2.calls))
    n_before = len(rec2.calls)
    bot2._recover("network", Exception("same problem again"))
    check("error notifications deduped within 5 min", len(rec2.calls) == n_before)

    # 4e) Permanent rejection (bad token / chat never pressed START) ->
    #     no retry storm and nothing new is queued: retrying cannot succeed.
    queued_before = tg_nobg.pending_count()
    reject_rec = _Recorder(behave="reject")
    t.requests.post = reject_rec
    check("telegram HTTP 401 -> False, no crash", tg_nobg.send("bad") is False)
    check("telegram HTTP 401 -> exactly one attempt (no retries)",
          len(reject_rec.calls) == 1)
    check("telegram HTTP 401 -> nothing new queued",
          tg_nobg.pending_count() == queued_before)

    # 4f) Startup token verification (getMe): valid -> True, invalid -> ERROR
    #     log that tells the user exactly which .env value to fix. verify()
    #     uses requests.get, so patch that instead of requests.post.
    class _GetMe:
        def __init__(self, status_code=200, payload=None):
            self.calls, self.url = 0, ""
            self.status_code, self.payload = status_code, payload

        def __call__(self, url, timeout=None):
            self.calls, self.url = self.calls + 1, url
            return _FakeResponse(status_code=self.status_code, payload=self.payload)

    real_get = t.requests.get
    tglog = io.StringIO()
    tgh = logging.StreamHandler(tglog)
    tgh.setLevel(logging.INFO)
    bot.log.addHandler(tgh)
    bot.log.setLevel(logging.INFO)
    try:
        tg_v = t.TelegramNotifier(
            "TEST:TOKEN", "42", bot.log,
            queue_retry_seconds=3600.0, spool_path=SPOOL,
        )
        gm_ok = _GetMe(payload={"ok": True, "result": {"username": "test_bot"}})
        t.requests.get = gm_ok
        check("telegram verify (valid token) -> True", tg_v.verify() is True)
        check("telegram verify asks Telegram getMe",
              gm_ok.calls == 1 and "getMe" in gm_ok.url)
        gm_bad = _GetMe(status_code=401)
        t.requests.get = gm_bad
        check("telegram verify (invalid token) -> False", tg_v.verify() is False)
        check("telegram invalid token -> clear ERROR with fix hint",
              "INVALID" in tglog.getvalue()
              and "TELEGRAM_BOT_TOKEN" in tglog.getvalue())

        # 4g) .env must beat stale process environment variables - a leftover
        #     DEMO_MODE=true / RUN_FOR_SECONDS in a terminal used to silently
        #     flip the bot's mode. SYMBOL is guaranteed to exist in .env files.
        env_file = os.path.join(os.path.dirname(os.path.abspath(t.__file__)), ".env")
        if os.path.exists(env_file):
            os.environ["SYMBOL"] = "DOGE/USDT"  # simulate a stale terminal value
            t._load_env_file()                  # .env must win + record conflict
            check(".env beats stale process env (mode can not drift)",
                  t.load_config()["symbol"] != "DOGE/USDT"
                  and "SYMBOL" in t.ENV_CONFLICTS)
            t.ENV_CONFLICTS.clear()
        else:
            check(".env beats stale process env (skipped: no .env file)", True)

        # 4h) Restart safety: a fresh notifier must recover unsent messages
        #     from the disk spool - a crash during an outage loses nothing.
        tg_recovered = t.TelegramNotifier(
            "TEST:TOKEN", "42", bot.log,
            background_retry=False, spool_path=SPOOL,
        )
        check("telegram spool recovered after simulated restart",
              tg_recovered.pending_count() == 1)
    finally:
        os.environ.pop("TELEGRAM_BOT_TOKEN", None)
        t.requests.get = real_get
        bot.log.removeHandler(tgh)
        bot.log.setLevel(logging.NOTSET)
finally:
    t.requests.post = real_post
    for _spool_file in (SPOOL, SPOOL2, SPOOL + ".tmp", SPOOL2 + ".tmp"):
        if os.path.exists(_spool_file):
            os.remove(_spool_file)


# 4i) Ctrl+C during retry -> the message is NOT lost: it is parked on disk
#     before the interrupt is allowed to propagate.
SPOOL_CTRLC = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_ctrlc.jsonl")
for _f in (SPOOL_CTRLC, SPOOL_CTRLC + ".tmp"):
    if os.path.exists(_f):
        os.remove(_f)

real_post = t.requests.post
try:
    tg_ctrlc = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=5, retry_delays=(0.3, 0.3, 0.3, 0.3),
        background_retry=False, spool_path=SPOOL_CTRLC,
    )

    class _InterruptingPost:
        """Fails twice, then raises KeyboardInterrupt to simulate Ctrl+C."""
        def __init__(self):
            self.calls = 0
        def __call__(self, url, json=None, timeout=None):
            self.calls += 1
            if self.calls <= 2:
                raise requests.ConnectionError("simulated outage")
            raise KeyboardInterrupt("user pressed Ctrl+C")

    t.requests.post = _InterruptingPost()
    ctrlc_raised = False
    try:
        tg_ctrlc.send("ctrlc-test-message")
    except KeyboardInterrupt:
        ctrlc_raised = True
    check("telegram Ctrl+C during retry -> interrupt propagates", ctrlc_raised)
    check("telegram Ctrl+C -> message saved to queue", tg_ctrlc.pending_count() == 1)
    check("telegram Ctrl+C -> message persisted to disk",
          os.path.exists(SPOOL_CTRLC)
          and any("ctrlc-test-message" in line
                  for line in open(SPOOL_CTRLC, encoding="utf-8").read().splitlines()))
finally:
    t.requests.post = real_post
    for _f in (SPOOL_CTRLC, SPOOL_CTRLC + ".tmp"):
        if os.path.exists(_f):
            os.remove(_f)


# 4j) Thread safety: many threads sending at once -> every message is either
#     delivered or queued, none are lost and none crash the program.
SPOOL_THREADS = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_threads.jsonl")
for _f in (SPOOL_THREADS, SPOOL_THREADS + ".tmp"):
    if os.path.exists(_f):
        os.remove(_f)

real_post = t.requests.post
try:
    tg_threads = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=2, retry_delays=(0.05,),
        queue_retry_seconds=0.1, background_retry=False, spool_path=SPOOL_THREADS,
    )

    # All attempts fail -> every message must end up in the queue.
    t.requests.post = _Recorder(behave="fail")
    threads = []
    messages = [f"thread-msg-{i}" for i in range(20)]
    errors = []

    def _send_msg(msg):
        try:
            tg_threads.send(msg)
        except Exception as e:
            errors.append(e)

    for msg in messages:
        th = threading.Thread(target=_send_msg, args=(msg,))
        threads.append(th)
        th.start()
    for th in threads:
        th.join(timeout=10)

    check("telegram concurrent sends -> no exceptions escaped", len(errors) == 0)
    check("telegram concurrent sends -> all 20 messages queued",
          tg_threads.pending_count() == 20)
    check("telegram concurrent spool file has 20 lines",
          os.path.exists(SPOOL_THREADS)
          and len([l for l in open(SPOOL_THREADS, encoding="utf-8").read().splitlines()
                   if l.strip()]) == 20)
finally:
    t.requests.post = real_post
    for _f in (SPOOL_THREADS, SPOOL_THREADS + ".tmp"):
        if os.path.exists(_f):
            os.remove(_f)


# 4k) Corrupt spool file recovery: a torn/corrupt line must not crash the
#     notifier - it is skipped and the good messages are still recovered.
SPOOL_CORRUPT = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_corrupt.jsonl")
for _f in (SPOOL_CORRUPT, SPOOL_CORRUPT + ".tmp"):
    if os.path.exists(_f):
        os.remove(_f)

real_post = t.requests.post
try:
    # Write a spool file with one good line and two corrupt lines.
    with open(SPOOL_CORRUPT, "w", encoding="utf-8") as fh:
        fh.write('{"text": "good-message-1", "queued_at": 0, "retries": 0}\n')
        fh.write('THIS IS NOT JSON\n')
        fh.write('{"text": "good-message-2", "queued_at": 0, "retries": 0}\n')
        fh.write('{"bad_shape": true}\n')  # missing "text" key

    tg_corrupt = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        background_retry=False, spool_path=SPOOL_CORRUPT,
    )
    check("telegram corrupt spool -> only good messages recovered",
          tg_corrupt.pending_count() == 2)
    # The recovered queue should contain both good messages.
    with tg_corrupt._queue_lock:
        recovered_texts = [item["text"] for item in tg_corrupt._queue]
    check("telegram corrupt spool -> good messages are intact",
          "good-message-1" in recovered_texts
          and "good-message-2" in recovered_texts)
finally:
    t.requests.post = real_post
    for _f in (SPOOL_CORRUPT, SPOOL_CORRUPT + ".tmp"):
        if os.path.exists(_f):
            os.remove(_f)


# 4l) FIFO ordering: messages must be delivered in the order they were queued.
SPOOL_FIFO = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_fifo.jsonl")
for _f in (SPOOL_FIFO, SPOOL_FIFO + ".tmp"):
    if os.path.exists(_f):
        os.remove(_f)

real_post = t.requests.post
try:
    # background_retry=False -> no worker thread, so we control exactly when
    # the retry cycle runs (no race with a background thread).
    tg_fifo = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=1, retry_delays=(),
        background_retry=False, spool_path=SPOOL_FIFO,
    )

    # Queue three messages while the network is down.
    t.requests.post = _Recorder(behave="fail")
    tg_fifo.send("first")
    tg_fifo.send("second")
    tg_fifo.send("third")
    check("telegram FIFO -> 3 messages queued", tg_fifo.pending_count() == 3)

    # Bring the network back and run one retry cycle BY HAND.
    delivery_order = []
    class _OrderRecorder:
        def __init__(self):
            self.calls = []
        def __call__(self, url, json=None, timeout=None):
            self.calls.append(json["text"])
            delivery_order.append(json["text"])
            return _FakeResponse()

    t.requests.post = _OrderRecorder()
    tg_fifo._run_retry_cycle()  # one synchronous pass through the queue
    check("telegram FIFO -> all 3 delivered", tg_fifo.pending_count() == 0)
    check("telegram FIFO -> delivered in order",
          delivery_order == ["first", "second", "third"])
finally:
    t.requests.post = real_post
    for _f in (SPOOL_FIFO, SPOOL_FIFO + ".tmp"):
        if os.path.exists(_f):
            os.remove(_f)


# 4m) Empty message: must not crash, handled gracefully.
SPOOL_EMPTY = os.path.join(tempfile.gettempdir(), "smoke_test_telegram_empty.jsonl")
for _f in (SPOOL_EMPTY, SPOOL_EMPTY + ".tmp"):
    if os.path.exists(_f):
        os.remove(_f)

real_post = t.requests.post
try:
    tg_empty = t.TelegramNotifier(
        "TEST:TOKEN", "42", bot.log,
        max_attempts=1, retry_delays=(),
        background_retry=False, spool_path=SPOOL_EMPTY,
    )
    # Empty string -> Telegram would reject it, but the notifier must not crash.
    t.requests.post = _Recorder()  # would return 200 if called
    result = tg_empty.send("")
    # The message is sent (Telegram may or may not accept it, but no crash).
    check("telegram empty message -> no crash", isinstance(result, bool))
finally:
    t.requests.post = real_post
    for _f in (SPOOL_EMPTY, SPOOL_EMPTY + ".tmp"):
        if os.path.exists(_f):
            os.remove(_f)


# 5) RUN_FOR_SECONDS is a TEST-ONLY timer: honoured in DEMO_MODE, IGNORED in
#    production so a 24/7 bot can never shut itself down (only Ctrl+C stops it).
log_capture = io.StringIO()
cap = logging.StreamHandler(log_capture)
cap.setLevel(logging.INFO)
bot.log.setLevel(logging.INFO)
bot.log.addHandler(cap)
try:
    # 5a) DEMO_MODE=true -> the timer stops the loop on its own.
    bot.cfg["poll_seconds"] = 0.2
    bot.cfg["run_for_seconds"] = 1
    bot.cfg["demo_mode"] = True
    bot.run()  # must return by itself after ~1s
    demo_log = log_capture.getvalue()
    check("demo test run auto-stops (RUN_FOR_SECONDS honoured)",
          "TEST RUN over" in demo_log)

    # 5b) production mode -> the timer is IGNORED; the loop keeps running past
    #     the would-be deadline until a real Ctrl+C arrives.
    log_capture.truncate(0)
    log_capture.seek(0)
    bot.cfg["run_for_seconds"] = 1
    bot.cfg["demo_mode"] = False  # simulate production (testnet) mode
    tick_calls = {"n": 0}

    def _fake_tick():
        tick_calls["n"] += 1
        if tick_calls["n"] >= 3:  # survive past the 1s deadline first...
            raise KeyboardInterrupt  # ...then the operator presses Ctrl+C
        time.sleep(0.6)

    bot._tick = _fake_tick
    bot.run()
    prod_log = log_capture.getvalue()
    check("production ignores RUN_FOR_SECONDS (warning logged)",
          "IGNORED in production mode" in prod_log)
    check("production did NOT auto-stop on the deadline",
          "TEST RUN over" not in prod_log
          and "Keyboard interrupt received" in prod_log)

    # 5c) A negative/garbage RUN_FOR_SECONDS must never create an instant stop.
    os.environ["RUN_FOR_SECONDS"] = "-7"
    check("negative RUN_FOR_SECONDS clamped to 0",
          t.load_config()["run_for_seconds"] == 0)
    os.environ.pop("RUN_FOR_SECONDS", None)
finally:
    bot.log.removeHandler(cap)
    bot.log.setLevel(logging.NOTSET)


# 6) asyncio: the blocking bot can be driven by an event loop without freezing it.
async_bot = t.BinanceTestnetBot(dict(cfg10, demo_mode=True, poll_seconds=0.05,
                                     run_for_seconds=1))
async_bot.tg = t.TelegramNotifier("", "", async_bot.log)
ticks = {"n": 0}


def _slow_tick():
    """Stands in for a blocking exchange + Telegram round trip."""
    ticks["n"] += 1
    time.sleep(0.05)


async_bot._tick = _slow_tick


async def _drive_bot():
    """Run the bot task and count event-loop heartbeats while it 'blocks'."""
    task = asyncio.create_task(async_bot.run_async())
    beats = 0
    started = time.monotonic()
    while not task.done() and time.monotonic() - started < 15:
        await asyncio.sleep(0.01)     # other coroutines must keep running
        beats += 1
    await task
    return beats


beats = asyncio.run(_drive_bot())
check("asyncio: run_async() executed its ticks", ticks["n"] >= 2)
check("asyncio: blocking ticks run in a worker thread (loop stays responsive)",
      beats >= 20)

# Cancelling the task (Ctrl+C / asyncio.run shutdown) must stop cleanly.
cancel_bot = t.BinanceTestnetBot(dict(cfg10, demo_mode=True, poll_seconds=5,
                                      run_for_seconds=0))
cancel_bot.tg = t.TelegramNotifier("", "", cancel_bot.log)


async def _drive_and_cancel():
    task = asyncio.create_task(cancel_bot.run_async())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
        return "finished"
    except asyncio.CancelledError:
        return "cancelled"


check("asyncio: a cancelled run stops cleanly (CancelledError)",
      asyncio.run(_drive_and_cancel()) == "cancelled")
check("asyncio: bot.run_async() and the module-level run_async() exist",
      callable(getattr(t, "run_async", None))
      and asyncio.iscoroutinefunction(t.BinanceTestnetBot.run_async))


# close() must never raise, even when the exchange refuses to close.
class _BadExchange:
    def close(self):
        raise RuntimeError("boom")


probe = t.BinanceTestnetBot(cfg10)
probe.tg = t.TelegramNotifier("", "", probe.log)
_real_exchange, probe.exchange = probe.exchange, _BadExchange()
try:
    probe.close()
    closed_cleanly = True
except Exception:
    closed_cleanly = False
finally:
    probe.exchange = _real_exchange
check("close() never raises when the exchange fails to close", closed_cleanly)

demo_probe = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2))
demo_probe.close()                     # harmless: the demo bot has no exchange
demo_probe.place_buy(60000.0, 10.2)
check("close() is a harmless no-op for the demo bot", demo_probe.paper_btc > 0)

def _exercise_scanner(scanner, cycles=3):
    """Run a few full portfolio cycles on random demo data; must not crash."""
    try:
        for _ in range(cycles):
            scanner._scan_once()
        return True
    except Exception:
        return False

# 5) Multi-coin / proportional sizing & risk (new) -----------------------------
# 5a) Proportional budget: split the balance into N parts, floored to minNotional.
p10 = t.compute_proportional_budget(10.0, parts=21, floor_usdt=10.0)
check("proportional: $10 / 21 parts / $10 floor -> one safe $10 slot",
      p10["usable_parts"] == 1 and abs(p10["per_part"] - 10.0) < 1e-9
      and abs(p10["active_budget"] - 10.0) < 1e-9)
p50 = t.compute_proportional_budget(50.0, parts=21, floor_usdt=10.0)
check("proportional: $50 / 21 parts / $10 floor -> five $10 slots",
      p50["usable_parts"] == 5 and abs(p50["per_part"] - 10.0) < 1e-9)
p210 = t.compute_proportional_budget(210.0, parts=21, floor_usdt=0.0)
check("proportional: $210 / 21 parts -> 21 x $10 (the video rule)",
      p210["usable_parts"] == 21 and abs(p210["per_part"] - 10.0) < 1e-9)
pmin = t.compute_proportional_budget(10.0, parts=21, floor_usdt=0.0, min_cost=10.0)
check("proportional: minNotional auto-shrinks the parts count",
      pmin["usable_parts"] == 1 and abs(pmin["floor"] - 10.0) < 1e-9)
check("proportional: zero balance -> zero budget, no crash",
      t.compute_proportional_budget(0.0, 21, 10.0)["usable_parts"] == 0)

# 5b) Trailing stop: drags the stop up behind a rising peak and locks profit.
check("trailing stop level = peak - trail%",
      abs(t.trailing_stop_level(100.0, 105.0, 3.0) - 101.85) < 1e-9)
check("trailing stop fires once price drops trail% below the peak",
      t.trailing_stop_hit(100.0, 105.0, 101.8, 3.0) is True
      and t.trailing_stop_hit(100.0, 105.0, 102.5, 3.0) is False)
check("trailing stop is inert when disabled (0%)",
      t.trailing_stop_hit(100.0, 105.0, 100.0, 0.0) is False)

# 5c) BNB fee discount: 0.1% taker cut 25% when paying with BNB.
fee_disc = t.effective_fee_rate(0.001, 25.0)
check("BNB fee discount: 0.1% -> 0.075% at 25% discount",
      abs(fee_disc - 0.00075) < 1e-12)

# 5d) Dynamic symbol discovery: spot / quote / volume-sorted / capped / excluded.
_mk = t.discover_spot_usdt_symbols(
    {
        "BTC/USDT": {"type": "spot", "stats": {"quoteVolume": 1e9}},
        "ETH/USDT": {"type": "spot", "stats": {"quoteVolume": 5e8}},
        "BTCUSDT":  {"type": "spot"},
        "BTC/EUR":  {"type": "spot", "stats": {"quoteVolume": 9e8}},
        "TRASH/USDT": {"type": "spot", "stats": {"quoteVolume": 100}},
        "SHIB/USDT": {"type": "spot", "info": {"quoteVolume": 2e7}},
    },
    "USDT", min_24h_quote=1e6, exclude=("SHIB/USDT",), max_symbols=3)
check("discovery: only high-volume /USDT pairs, volume-sorted + capped",
      _mk == ["BTC/USDT", "ETH/USDT"])
check("discovery: EUR quote and non-/-USDT names are ignored",
      "BTC/EUR" not in _mk and "BTCUSDT" not in _mk)

# 5e) MultiCoinScanner demo lifecycle: proportional BUY -> trailing/TP -> SELL.
scan_cfg = dict(t.load_config(), demo_mode=True, initial_balance_usdt=50.0,
                scanner_symbols="BTC/USDT,ETH/USDT,SOL/USDT", portfolio_parts=21,
                portfolio_floor_usdt=10.0, trailing_stop_pct=2.0)
scan = t.MultiCoinScanner(scan_cfg)
scan.tg = t.TelegramNotifier("", "", scan.log)
check("scanner demo: explicit universe is honoured",
      set(scan.symbols) == {"BTC/USDT", "ETH/USDT", "SOL/USDT"})
scan._place_buy("BTC/USDT", 60000.0, scan.paper_usdt, 10.0, adx_now=30.0)
check("scanner BUY: position opened and sized to the proportional part",
      scan.positions and scan.positions["BTC/USDT"]["entry"] == 60000.0
      and abs(scan.positions["BTC/USDT"]["cost"] - 10.0) < 0.05)
check("scanner BUY: spends the proportional budget (50 -> ~40 left)",
      abs(scan.paper_usdt - 40.0) < 0.1)
check("scanner: no exit inside the TP/SL bracket",
      scan._exit_reason("BTC/USDT", 60500.0, "HOLD") == "")
scan.positions["BTC/USDT"]["trail_peak"] = 61000.0
check("scanner trailing stop fires after a pullback from the peak",
      "TRAILING-STOP" in scan._exit_reason("BTC/USDT", 59750.0, "HOLD"))
check("scanner TP fires at +2.5%",
      "TAKE-PROFIT" in scan._exit_reason("BTC/USDT", 61500.0, "HOLD"))
_qty = scan.positions["BTC/USDT"]["qty"]
scan._place_sell("BTC/USDT", 61000.0, _qty, "TAKE-PROFIT +2.5% hit")
check("scanner SELL: position closed and the trade recorded",
      "BTC/USDT" not in scan.positions and len(scan.closed_trades) == 1
      and scan.closed_trades[0]["pnl_quote"] > 0)
check("scanner close() is a harmless no-op for the demo scanner",
      scan.close() is None)
check("scanner _scan_once() runs several full cycles without crashing",
      _exercise_scanner(scan))

# 5f) Single-symbol bot: the same trailing-stop rule now protects its positions.
trail_bot = t.BinanceTestnetBot(dict(cfg10, order_size_quote=10.2, trailing_stop_pct=2.0))
trail_bot.tg = t.TelegramNotifier("", "", trail_bot.log)
trail_bot.place_buy(60000.0, 10.2)
check("single bot: the trailing stop guard is wired to the entry price",
      trail_bot.trailing_stop_pct == 2.0
      and trail_bot.position.get("trail_peak") == trail_bot.avg_entry_price == 60000.0)
check("single bot: a fixed TP still fires before the trailing stop",
      (trail_bot._tp_sl_exit_reason(60000.0 * 1.025) or "").startswith("TAKE-PROFIT"))
trail_bot.position["trail_peak"] = 61500.0
# The exit string is produced inside _tick; the pure helper behind it must agree.
check("single bot: trailing-stop helper locks profit off the raised peak",
      t.trailing_stop_hit(60000.0, 61500.0, 60250.0, trail_bot.trailing_stop_pct)
      and not t.trailing_stop_hit(60000.0, 61500.0, 60280.0, trail_bot.trailing_stop_pct))

# 5g) asyncio: the scanner loop stays responsive while each cycle "blocks".
async_scan = t.MultiCoinScanner(dict(scan_cfg, scan_poll_seconds=0.05))
async_scan.tg = t.TelegramNotifier("", "", async_scan.log)
scan_ticks = {"n": 0}


def _slow_scan():
    scan_ticks["n"] += 1
    time.sleep(0.05)


async_scan._scan_once = _slow_scan


async def _drive_scanner():
    task = asyncio.create_task(async_scan.run_async())
    beats = 0
    started = time.monotonic()
    while time.monotonic() - started < 0.5:
        await asyncio.sleep(0.01)
        beats += 1
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    return beats


scan_beats = asyncio.run(_drive_scanner())
check("asyncio scanner: ran multiple portfolio cycles", scan_ticks["n"] >= 2)
check("asyncio scanner: blocking scans run in a worker thread (loop responsive)",
      scan_beats >= 20)
check("asyncio scanner: run_async() is an awaitable coroutine",
      asyncio.iscoroutinefunction(t.MultiCoinScanner.run_async))



print("\nSMOKE TEST:", "OK" if not failures else "FAILED -> " + ", ".join(failures))
raise SystemExit(1 if failures else 0)
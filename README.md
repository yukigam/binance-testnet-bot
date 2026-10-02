# Binance Spot Testnet Trading Bot (multi-coin scanner, DCA, trailing stop)

A small, ready-to-run **paper-trading** bot for the **Binance Spot Testnet**.
It trades **5-minute candles** using an **EMA(8, 21) crossover** strategy
(BUY / SELL / HOLD) with an **ADX(14) trend filter** (entries only when
ADX > 25, configurable) read from **completed candles only**, plus an optional
**RSI entry filter** (`RSI_MAX_ENTRY=75` refuses to chase a blow-off top).
Every position is protected by an automatic **Take-Profit (+2.5%)** and
**Stop-Loss (-2.5%)** hard limit, an *armed* **trailing stop** (it only starts
after the price has risen `TRAILING_ARM_PCT=0.5%` above the entry, so it never
pre-empts the risk rules on the way down), and an optional **DCA grid** that
averages down `-1.5%` below the average entry instead of stopping out at once.
Orders are sized so they really pass the exchange's `minNotional` filter
*after* lot rounding — and so that their stop-loss exit can still be sold.

The shipped configuration targets a **50 USDT** test account
(`INITIAL_BALANCE_USDT=50`) trading **BTC, ETH, SOL and BNB** through the
multi-coin scanner, with roughly 20% of the balance kept free for DCA adds and
fees. Every action (entry, DCA add, take-profit, stop-out, trailing exit, risk
pause) is announced on **Telegram**.

> ⚠️ **No real money is ever at risk.** The exchange is forced into ccxt
> sandbox/testnet mode, so every request goes only to
> `https://testnet.binance.vision`. The funds on that site are fake.

---

## 1. Install the required libraries

```bash
pip install -r requirements.txt
```

| Library             | Why it is needed                                  |
|---------------------|---------------------------------------------------|
| `ccxt`              | Official-ish unified crypto exchange API library; connects to the Binance Spot Testnet via `set_sandbox_mode(True)` |
| `pandas`            | Computing EMA / RSI / ADX indicators on the candle data |
| `python-dotenv`     | Loading the API keys from a `.env` file           |
| `requests`          | Sending the Telegram notification messages (Bot API) |

**Optional alternative:** you could use `python-binance` instead of `ccxt` —
`from binance.client import Client; client = Client(api_key, api_secret, testnet=True)`.
This repo uses ccxt.

---

## 2. Get your Binance Testnet API keys

1. Log in (or create an account) on Binance, then open the **Spot Testnet**:
   **https://testnet.binance.vision/**
2. Click **"Generate HMAC SHA256 key"**.
3. Copy the generated **API key** and **secret key**.
4. Click **"Request testnet funds"** to credit your test wallet with fake
   BTC / USDT balances (they have no real value).

> Never paste your real mainnet API keys in this project — the script is
> hard-wired to the testnet, but keeping your mainnet keys private is best practice.

---

## 3. Configure the bot with a `.env` file

```bash
copy .env.example .env
```

Open `.env` and fill in:

```
BINANCE_TESTNET_API_KEY=your_testnet_api_key
BINANCE_TESTNET_API_SECRET=your_testnet_secret_key
```

All other settings (symbol, timeframe, EMA periods, ADX filter, TP/SL,
sizing, risk caps, poll interval, ...) are already filled with sensible
defaults — see the comments in `.env.example`. The blocks you will touch most:

```
STOP_LOSS_PCT=2.5       # hard stop (must sit BELOW DCA_DROP_PERCENT)
TRAILING_STOP_PCT=1.0   # profit lock, armed only after TRAILING_ARM_PCT=0.5%
DCA_ENABLED=true        # DCA_DROP_PERCENT=1.5, DCA_SIZE_QUOTE=10,
                        # DCA_MAX_ENTRIES=2, DCA_MAX_POSITION_QUOTE=30
RSI_MAX_ENTRY=75        # 0 = RSI entry filter off
INITIAL_BALANCE_USDT=50.0
SYMBOLS=BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT   # multi-coin, one position each
ORDER_SIZE_PER_COIN=true  # buy exactly ORDER_SIZE_QUOTE of EACH coin
USE_ALL_BALANCE_PCT=80  # caps what may be invested; rest stays free for DCA
```

`.env` is the **single source of truth**: values in it always override stale
variables inherited from your shell (a leftover `DEMO_MODE=true` or
`RUN_FOR_SECONDS` cannot silently change how the bot runs — conflicts are
reported at startup). To switch modes, edit `.env` instead of prefixing the
command.

---

## 4. Run the bot

```bash
python trading_bot.py
```

You should see real-time logs like this:

```
14:03:22 | INFO    | Sizing check: budget 10 USDT | minNotional 10.00 USDT | safe minimum account 10.21 USDT | free 50.0000 USDT (equity 50.00)
14:03:22 | INFO    | Net reward/risk after costs: win +2.20% / loss -1.80% -> R:R 1.22 (break-even win rate 45.0%) | TP 2.5% | SL 2.5% | 0.30% per round trip
14:03:37 | INFO    | price=59720.45 | EMA8=59801.12 EMA21=59900.34 | RSI14=49.23 | ADX14=27.14 (filter>25) | signal=HOLD (no crossover, closed candles) | position=flat | free=50.0000 USDT free=0.00000000 BTC | tp=n/a sl=n/a
14:03:52 | INFO    | >> BUY order placed  id=1234567 status=closed qty=0.00017 @ 59,720.45 cost=10.1525 | TP: 61,213.46 (+2.5%) | SL: 58,227.44 (-2.5%) | lot bumped up to pass minNotional
14:04:07 | INFO    | price=60540.10 | ... | signal=HOLD (no crossover, closed candles) | position=#1 LONG entry=59,720.45 uPnL=+1.37% | free=39.8475 USDT free=0.00016983 BTC | tp=61,213.46 sl=58,227.44
```

- **BUY** is logged and a market order is placed when EMA fast crosses **above**
  EMA slow (on completed candles) **and** ADX > 25 — a genuinely trending market.
- **SELL** is logged and a market order is placed when EMA fast crosses **below**
  EMA slow — this risk exit is never blocked by the ADX filter, and never by the
  account-protection gates below.
- **HOLD** means no usable crossover happened (the reason is always logged, e.g.
  "golden cross but ADX=21.4 <= 25 (no trend - entry filtered out)").
- **TP / SL**: while a position is open the log (and Telegram) show the live
  Take-Profit (`+2.5%`) and Stop-Loss (`-2.5%`) levels. The bot closes the
  position automatically as soon as the price hits either level, even in the
  middle of a candle, and reports which level fired.
- **Trailing stop**: once the price has climbed `TRAILING_ARM_PCT` (0.5%)
  above the entry, the exit trails `TRAILING_STOP_PCT` (1%) behind the running
  peak — a pure profit lock that can never fire before the position actually
  went up (so it does not pre-empt the DCA or the hard stop on the way down).
- **DCA (average down)**: `-1.5%` below the *current average* entry the bot
  adds `DCA_SIZE_QUOTE` (10 USDT) instead of being stopped out at once — at
  most `DCA_MAX_ENTRIES=2` times, at most once per candle, never past
  `DCA_MAX_POSITION_QUOTE=30`, and never once the hard stop is broken. TP and
  trailing exits always win over a DCA add; afterwards the TP/SL bracket and
  the next trigger move with the new average entry.
- **RSI entry filter**: `RSI_MAX_ENTRY=75` / `RSI_MIN_ENTRY` refuse a *new* BUY
  outside the configured window (exits and DCA adds are never gated). The
  refusal reason is logged, e.g. `golden cross but RSI=78.2 > RSI_MAX_ENTRY=75
  (too overbought to start a trade)`.
- `position=#1 LONG entry=... uPnL=...` tracks the open trade — its number,
  entry price and unrealised P/L. Every closed trade is journaled with its
  reason, P/L, holding time and the running session statistics (also visible in
  Telegram).
- The bot only acts **once per candle** on indicator signals, so it does not
  spam orders while a signal stays active (TP/SL are checked on every poll).

### Position sizing on a small (10–50 USDT) balance

Binance rejects an order whose *rounded* value is below the symbol's
`minNotional` — and it rejects a **sell** that would be worth less than that
too (the position becomes unsellable dust). The bot therefore:

1. floors the quantity to the symbol's lot step,
2. bumps it by **one lot step** when that flooring would fall below
   `minNotional` (only when the balance can pay for it),
3. **refuses** the entry with an exact message (`need about 10.12 USDT free ...`)
   when even that is impossible, instead of sending an order the exchange rejects,
4. prints a **dust-risk warning** when the stop-loss exit itself would fall below
   `minNotional` (`REQUIRE_EXIT_VIABLE=true` refuses such entries completely).

Consequence: on a pair with a 10 USDT `minNotional` a 10.00 USDT budget is too
tight once fees and the stop distance are included — the startup line
`safe minimum account` prints the balance your symbol and TP/SL need.

### Account protection (a small balance must survive a bad streak)

- `MAX_DAILY_LOSS_PCT=3.0` — no **new** positions once the day's realised loss
  reaches 3% of the day's starting equity (reset at 00:00 UTC).
- `MAX_CONSECUTIVE_LOSSES=3` — pause entries after three losers in a row.
- `LOSS_COOLDOWN_CANDLES=2` — stay flat for two candles after a stop-out instead
  of immediately re-entering the same chop that just stopped the bot out.
- All of them are on by default (set them to `0` to disable) and **none of them
  can block an exit** — TP/SL and the death-cross exit always run.

### Demo mode (no API keys needed)

You can watch the exact same loop with simulated price data before you have keys:
set `DEMO_MODE=true` inside `.env` and run `python trading_bot.py`.

A random-walk price feed is generated locally and simulated BUY/SELL orders are
logged with the `DEMO` prefix (DCA adds, trailing exits and the RSI filter work
exactly as in live mode). The demo simulates the same sized account you are
testing against on the real testnet — **50.00 USDT** by default
(`INITIAL_BALANCE_USDT`), with `ORDER_SIZE_QUOTE=10` and a realistic **0.1%
taker fee** (`FEE_RATE=0.001`) applied to paper fills, so leftover dust and
residual balances behave like the live account. `RUN_FOR_SECONDS=12` makes a
demo run stop itself after N seconds (test-only; ignored in production, honoured
by both the single-symbol loop and the scanner). `python smoke_test.py`
verifies this scenario (minNotional gate, lot-step bump, dust protection, fee
residuals) without any network access.

### Multi-coin portfolio (parallel) & all-USDT-pairs scanner

The classic bot trades one `SYMBOL`. List several coins in `SYMBOLS` and the bot
becomes a parallel multi-coin portfolio — one independent position per coin:

```
# .env
SYMBOLS=BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT
ORDER_SIZE_PER_COIN=true   # buy exactly ORDER_SIZE_QUOTE of EACH coin
ORDER_SIZE_QUOTE=50        # -> 50 USDT of BTC, 50 of ETH, ... (free balance caps)
```

Just run it — with 2+ coins the multi-coin portfolio is switched on
automatically. For fully dynamic / all-pairs mode use `SCAN_ENABLED=true` or a
CLI flag:

```bash
# dynamic discovery of every liquid /USDT pair:
python trading_bot.py --scan          # sync loop
python trading_bot.py --scan --async  # asyncio loop (worker threads keep it responsive)
```

What it does:

1. **Coin universe.** A `SYMBOLS` list with 2+ coins (e.g.
   `BTC/USDT,ETH/USDT,SOL/USDT,BNB/USDT`) is traded in parallel — the bot
   enters *every* coin that shows an active BUY setup in the same cycle, each as
   its own independent position. For fully dynamic mode (`SCAN_ENABLED=true` /
   `--scan`) it instead calls `load_markets()` and picks every active SPOT pair
   quoted in `SCAN_QUOTE` (USDT by default), optionally dropped to the
   high-volume ones via `SCAN_MIN_24H_QUOTE`, minus `SCAN_EXCLUDE`, capped at
   `SCAN_MAX_SYMBOLS` (the legacy `SCANNER_SYMBOLS` list is used when `SYMBOLS`
   is empty).
2. **Position sizing.** With `ORDER_SIZE_PER_COIN=true` (auto-on for a 2+ coin
   `SYMBOLS` list) *every* entry spends exactly `ORDER_SIZE_QUOTE`, so
   `ORDER_SIZE_QUOTE=50` buys 50 USDT of each coin — the free balance is the
   only limit. Otherwise the free balance is split into `PORTFOLIO_PARTS` equal
   budgets (5 parts of ~10 USDT on the shipped 50 USDT account). On a $10
   account each slot is floored to `PORTFOLIO_FLOOR_USDT` / minNotional, so it
   auto-degrades to one safe, exit-viable slot; on $50+ it spreads across
   several properly-sized slots. `USE_ALL_BALANCE_PCT` (80%) caps how much may
   be invested at once, so the remainder stays free for DCA adds and fees
   instead of being fully invested.
3. **Same risk rules, per pair.** Each open position gets its own hard
   Take-Profit, Stop-Loss, *armed* Trailing-Stop (`TRAILING_STOP_PCT` +
   `TRAILING_ARM_PCT`) and its own DCA grid (`DCA_*`), while the EMA + ADX
   trend filter (`ADX_THRESHOLD`, default 25) and the RSI filter
   (`RSI_MAX_ENTRY`) gate every new entry. Exit priority per cycle:
   take-profit → trailing stop → DCA add → stop-loss → EMA death cross (the
   bracket is re-checked after a DCA add).
4. **Non-blocking, parallel scanning.** Per-symbol price/candle fetches run in a
   thread pool, and the `--async` loop runs each full cycle in a worker thread,
   so the event loop stays responsive while the scanner crawls many pairs.

`DEMO_MODE=true` runs the same portfolio loop on synthetic data so you can watch
it before adding keys.

### Running inside an asyncio application

The classic loop is blocking, which is fine for a terminal bot. To embed the bot
in an async application (FastAPI, aiogram, ...), use the async entry point: the
strategy and every ccxt/Telegram call stay identical, they merely run in a worker
thread so the event loop is never blocked.

```bash
python trading_bot.py --async       # asyncio-driven loop, same bot
```

```python
import asyncio
import trading_bot as t

await t.run_async(t.load_config())   # or: asyncio.run(t.run_async())
```

The strategy core (`compute_indicators`, `current_signal`, `plan_market_buy`,
`reward_risk_after_costs`) is pure and synchronous, so it can also be reused
directly from your own async code, e.g.
`await asyncio.to_thread(current_signal, df, 25.0, True)` (Python 3.9+).

---

## 5. Error handling

The bot is built so it **never crashes on a temporary network problem**:

- `ccxt.NetworkError` (timeouts, connection resets, rate limits) → logged and retried.
- Wrong / missing testnet keys → clear log message, keeps looping.
- Bad orders / insufficient balance → logged, that candle is skipped.
- Any unexpected exception → logged with traceback, loop continues.
- Exponential backoff (15s → 30s → 60s, capped at 60s) between consecutive failures.
- Telegram alert (if configured) on network problems / critical errors, plus a
  🟢 "reconnected" notice when trading resumes.
- `Ctrl+C` stops the loop cleanly (in `--async` mode the task is cancelled and
  the exchange session is closed). This is the ONLY way a production bot stops:
  `RUN_FOR_SECONDS` is ignored outside `DEMO_MODE=true`, so the bot always runs
  24/7 (a warning is logged if the variable is set in production).

## 6. Telegram notifications (optional)

The bot can push messages straight to your phone:

- 🤖 **Startup** — mode, symbol, strategy, ADX filter, TP/SL, the **net R:R
  after costs**, the active risk caps and your current testnet balance.
- 🟢 **Every BUY** — trade number, entry price, quantity, cost, TP/SL levels,
  risk/reward in USDT, R:R after costs, the signal that fired and the ADX/RSI
  values at entry.
- 🔴 **Every SELL** — trade number, exit price, proceeds, the **reason**
  (death cross / which hard TAKE-PROFIT or STOP-LOSS level fired), P/L in USDT
  and %, estimated fees, holding time, the entry context and the running
  session statistics (trades, W/L, win rate, P/L, today's P/L).
- ⛔ **Risk gate** — when new entries are paused (daily loss limit / losing
  streak / cooldown after a stop-out) and 🗓️ when a new UTC day resets them.
- 🔴 **Critical errors** — network disconnections and other serious problems
  (at most once every 5 minutes per problem type, so a long outage does not
  spam the chat, plus a 🟢 "reconnected" message when trading resumes).

### One-time setup (2 minutes)

1. In Telegram, talk to **@BotFather** → send `/newbot` → choose a name →
   copy the **bot token** (looks like `123456789:AAH9x...`).
2. **Important:** open a chat with your new bot and press **START** (or send
   it any message). Telegram forbids bots from messaging a user first.
3. Find your **chat id**: message **@userinfobot** and it replies with your
   id — or open `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` in a
   browser after messaging your bot and read `"chat":{"id":123...}`.
4. Put both values in your `.env` file:

```
TELEGRAM_BOT_TOKEN=123456789:AAH9x...
TELEGRAM_CHAT_ID=123456789
```

5. Restart the bot — the 🤖 startup message should arrive within seconds.

> 🛡️ **Fail-safe by design:** every send is wrapped in try/except and retried
> automatically: up to `TELEGRAM_MAX_ATTEMPTS` quick inline attempts, then a
> background queue keeps re-sending every `TELEGRAM_QUEUE_RETRY_SECONDS` (for
> up to `TELEGRAM_QUEUE_MAX_AGE` seconds). If Telegram is unreachable (no
> internet, connection timeout, DNS error, ...) the bot only logs a
> `[telegram] ...` warning and keeps trading - the loop is never blocked for
> long and never crashed by it. Leaving the two values empty disables
> notifications entirely.

### Startup self-check (why are my messages not arriving?)

Every time the bot starts it checks the Telegram setup and says exactly what
is wrong if something is off:

- logs the configured values with the token **masked** (`token=8902...UDVk`),
  so you can see what was actually loaded and where it came from;
- calls Telegram's `getMe` once — an invalid/revoked `TELEGRAM_BOT_TOKEN`
  produces a clear `ERROR ... TELEGRAM_BOT_TOKEN is INVALID` line at startup
  instead of orders showing up while messages silently never arrive;
- treats `.env` as the **single source of truth**: its values always override
  stale shell variables (a leftover `DEMO_MODE=true`, `RUN_FOR_SECONDS` or
  dummy token from a test terminal can no longer flip the bot's mode or stop
  it), and every overridden variable is reported at startup;
- logs `[telegram] delivered: <message>` on every successful send, and
  rejected sends name the exact value to fix (`TELEGRAM_BOT_TOKEN` for
  HTTP 401/404, `TELEGRAM_CHAT_ID` for HTTP 400/403).

## 7. Self-test (no network needed)

A smoke test ships with the project. It never touches the network and never
places a real order. It covers:

- sandbox routing (`testnet.binance.vision`);
- EMA crossover signals, the ADX > 25 trend filter, **closed-candle** signals
  and the RSI entry filter (`RSI_MAX_ENTRY` / `RSI_MIN_ENTRY`);
- net reward/risk maths after fees and slippage;
- demo paper trading, fee residuals and the small (10 / 50 USDT) balances;
- the exchange-minimum sizing rules: lot-step bump, `minNotional` refusal after
  rounding, dust-risk detection and the "safe minimum balance" helper;
- DCA planning: trigger grid, max entries, per-coin cap, the broken-stop guard,
  the minNotional-checked add, and the one-add-per-candle rule;
- the *armed* trailing stop (it stays inert until the price rose
  `TRAILING_ARM_PCT` above the entry, then trails the peak);
- trade tracking + journal statistics (take-profit cycle, stop-out, streak);
- the account risk gates (daily loss limit, losing streak, cooldown) and the
  guarantee that an exit is never blocked by them;
- Telegram notifier behaviour (retries, background queue, disk spool, message
  content) and the fail-safe startup self-checks;
- logging format safety (every `%-format` log message uses only valid
  conversion specs, so no log line is ever silently swallowed);
- the asyncio entry point (`run_async`) and clean cancellation;
- the multi-coin portfolio: parsing the `SYMBOLS` list, fixed per-coin sizing
  (`ORDER_SIZE_PER_COIN`), parallel positions in several coins at once,
  dynamic `/USDT` symbol discovery, proportional position sizing from $10 to
  $50+, per-pair TP/SL + armed trailing stop + DCA, BNB fee-discount handling,
  and a non-blocking asyncio scanner loop.

```bash
python smoke_test.py     # all checks should print PASS
```

---

## 8. Files

| File                 | Purpose                                                        |
|----------------------|----------------------------------------------------------------|
| `trading_bot.py`     | The complete bot (strategy, sizing, risk gates, order placement, logging, retries, Telegram notifications, sync + asyncio entry points) |
| `.env.example`       | Template for your testnet keys and bot settings                |
| `.gitignore`         | Keeps `.env`, logs, spools and the virtualenv out of git       |
| `requirements.txt`   | Python dependencies                                            |
| `smoke_test.py`      | Optional network-free self-test                                |

## 9. Disclaimer

This is an educational project. Strategy parameters like EMA(8, 21) with an
ADX(14) > 25 filter, a fixed +2.5% / -2.5% TP/SL, a -1.5% DCA grid and a 1%
trailing stop are deliberately simple, and even on the testnet you should only
trade amounts you are comfortable experimenting with. Always start with
`DEMO_MODE=true`.
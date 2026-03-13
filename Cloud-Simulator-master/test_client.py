"""
Smart test client with CSV tick logging.
Saves all ticks to: logs/ticks_YYYY-MM-DD.csv

Output format matches the Simulator terminal style:
  HH:MM:SS.mmm [INFO ] Simulator: [TICK] INSTRUMENT   ltp=X   iv=X   delta=X   gamma=X   theta=X   vega=X   oi=X

Usage: python test_client.py
"""
import asyncio
import json
import csv
import os
import boto3
import websockets
from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
    IST = ZoneInfo("Asia/Kolkata")
except ImportError:
    IST = None

# ── Config ───────────────────────────────────────────────────────────────
WS_URI      = "ws://localhost:8000/ws"
STRIKE_GAP  = 50
RANGE       = 1150
BUCKET_NAME = "mq-data-feed-dump-s3-prod-v1"
BATCH_SIZE  = 10
BATCH_DELAY = 0.5
LOG_DIR     = "logs"
FLUSH_EVERY = 100
TARGET_DATE = "2026-01-08"  # ← date for instrument mapping & expiry lookup


# ── Logger ────────────────────────────────────────────────────────────────

class TickLogger:
    COLUMNS = [
        "tick_num", "received_at", "instrument",
        "ts", "ts_ist",
        "ltp", "oi",
        "iv", "delta", "gamma", "theta", "vega",
        "strike", "expiry",
    ]

    def __init__(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        fname        = f"ticks_{TARGET_DATE}.csv"
        self.path    = os.path.join(LOG_DIR, fname)
        self._file   = open(self.path, "w", newline="", buffering=1)
        self._writer = csv.DictWriter(self._file, fieldnames=self.COLUMNS)
        self._writer.writeheader()
        self._count  = 0
        print(f"[Logger] ✓ Logging ticks to → {self.path}\n")

    def write(self, tick_num: int, data: dict):
        ts_ms = data.get("ts") or 0
        ts_ist = ""
        try:
            dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            if IST:
                dt = dt.astimezone(IST)
            ts_ist = dt.strftime("%H:%M:%S")
        except Exception:
            pass

        self._writer.writerow({
            "tick_num":    tick_num,
            "received_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "instrument":  data.get("instrument", ""),
            "ts":          ts_ms,
            "ts_ist":      ts_ist,
            "ltp":         data.get("ltp", 0),
            "oi":          data.get("oi", 0),
            "iv":          data.get("iv", ""),
            "delta":       data.get("delta", ""),
            "gamma":       data.get("gamma", ""),
            "theta":       data.get("theta", ""),
            "vega":        data.get("vega", ""),
            "strike":      data.get("strike", ""),
            "expiry":      data.get("expiry", ""),
        })

        self._count += 1
        if self._count % FLUSH_EVERY == 0:
            self._file.flush()
            print(f"[Logger] Flushed — {self._count:,} ticks saved to {self.path}")

    def close(self):
        self._file.flush()
        self._file.close()
        print(f"\n[Logger] ✓ Done. {self._count:,} ticks saved → {self.path}")


# ── Helpers ──────────────────────────────────────────────────────────────

def get_atm(price: float) -> float:
    return round(price / STRIKE_GAP) * STRIKE_GAP

def get_strikes_in_range(atm: float) -> list:
    lower = int((atm - RANGE) / STRIKE_GAP) * STRIKE_GAP
    upper = int((atm + RANGE) / STRIKE_GAP) * STRIKE_GAP
    return [float(s) for s in range(int(lower), int(upper) + STRIKE_GAP, STRIKE_GAP)]

def fetch_option_symbols(date: str) -> list:
    parsed = datetime.strptime(date, "%Y-%m-%d")
    y, m, d = parsed.year, f"{parsed.month:02}", f"{parsed.day:02}"
    key = f"dailyInstruments/{y}/{m}/{d}/mapping-instruments-{y}-{m}-{d}.json"
    try:
        s3   = boto3.resource("s3")
        body = s3.Object(BUCKET_NAME, key).get()["Body"].read().decode("utf-8").replace("'", '"')
        instruments = json.loads(body)
        nifty = [i for i in instruments if str(i["tradingSymbol"]).startswith("NIFTY")]
        print(f"[S3] Loaded {len(nifty)} NIFTY instruments for {date}")
        return nifty
    except Exception as e:
        print(f"[S3] Failed: {e}")
        return []

def select_instruments(nifty_instruments: list, strikes: list) -> list:
    strike_set = set(strikes)
    selected = []
    for opt in nifty_instruments:
        try:
            if float(opt["strike"]) in strike_set:
                sym = opt["tradingSymbol"]
                if sym.endswith("CE") or sym.endswith("PE"):
                    selected.append(sym)
        except Exception:
            continue
    return selected

def nearest_expiry_str(date_str: str = None) -> str:
    base = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.now()
    days_ahead = (3 - base.weekday()) % 7
    expiry = base + timedelta(days=days_ahead)
    return expiry.strftime("%y%m%d")


# ── Terminal print — matches Simulator output style ───────────────────────

def _fmt(val, width: int = 10) -> str:
    """Format a value: float to 4dp, None stays 'None', aligned."""
    if val is None or val == "" or val == "None":
        return f"{'None':<{width}}"
    try:
        return f"{float(val):<{width}.4f}"
    except (TypeError, ValueError):
        return f"{str(val):<{width}}"

def print_tick(tick_count: int, data: dict):
    instrument = data.get("instrument", "")
    ltp        = data.get("ltp", 0)
    oi         = data.get("oi", 0)
    iv         = data.get("iv")
    delta      = data.get("delta")
    gamma      = data.get("gamma")
    theta      = data.get("theta")
    vega       = data.get("vega")

    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]

    print(
        f"{now} [INFO ] Simulator: [TICK] "
        f"{instrument:<32}  "
        f"ltp={_fmt(ltp, 10)}  "
        f"iv={_fmt(iv, 10)}  "
        f"delta={_fmt(delta, 10)}  "
        f"gamma={_fmt(gamma, 10)}  "
        f"theta={_fmt(theta, 10)}  "
        f"vega={_fmt(vega, 10)}  "
        f"oi={oi}"
    )


# ── Single session ────────────────────────────────────────────────────────

async def run_session(option_symbols: list, logger: TickLogger):
    async with websockets.connect(
        WS_URI,
        ping_interval=20,
        ping_timeout=60,
        close_timeout=10,
        max_size=10 * 1024 * 1024,
    ) as ws:
        print("[Client] ✓ Connected to server")

        await ws.send(json.dumps({
            "action": "subscribe",
            "instruments": ["NSE_INDEX|Nifty 50"]
        }))

        atm_determined       = False
        symbols_to_subscribe = option_symbols.copy()
        tick_count           = 0

        async for raw in ws:
            data = json.loads(raw)

            if "status" in data:
                print(f"[Client] ✓ {data['status']} — {len(data.get('instruments', []))} instruments")
                continue

            tick_count += 1
            instrument = data.get("instrument", "")
            ltp        = float(data.get("ltp") or 0)

            # ── Log to CSV (all fields) ──────────────────────────────────
            logger.write(tick_count, data)

            # ── Print in Simulator style ─────────────────────────────────
            print_tick(tick_count, data)

            # ── On first index tick — subscribe to options ───────────────
            if not atm_determined and instrument == "NSE_INDEX|Nifty 50" and ltp > 0:
                atm_determined = True

                if not symbols_to_subscribe:
                    atm     = get_atm(ltp)
                    strikes = get_strikes_in_range(atm)
                    print(f"\n[Client] Index LTP={ltp:.2f}  ATM={atm:.0f}")
                    print(f"[Client] Range: {strikes[0]:.0f} → {strikes[-1]:.0f}  ({len(strikes)} strikes)")

                    nifty_instruments = fetch_option_symbols(TARGET_DATE)

                    if nifty_instruments:
                        symbols_to_subscribe = select_instruments(nifty_instruments, strikes)
                    else:
                        exp = nearest_expiry_str(TARGET_DATE)
                        symbols_to_subscribe = []
                        for offset in range(-4, 5):
                            k = int(atm + offset * STRIKE_GAP)
                            symbols_to_subscribe += [f"NIFTY{exp}{k}CE", f"NIFTY{exp}{k}PE"]

                print(f"[Client] Subscribing to {len(symbols_to_subscribe)} option instruments...\n")
                asyncio.create_task(_subscribe_batches(ws, symbols_to_subscribe))

        return symbols_to_subscribe


async def _subscribe_batches(ws, symbols: list):
    total   = len(symbols)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, total, BATCH_SIZE):
        batch     = symbols[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        now       = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        try:
            await ws.send(json.dumps({"action": "subscribe", "instruments": batch}))
            print(
                f"{now} [DEBUG] Simulator: Sent batch {batch_num}/{batches} "
                f"({len(batch)} instruments)"
            )
        except Exception as e:
            print(f"[Client] ✗ Batch {batch_num} failed: {e}")
            return
        await asyncio.sleep(BATCH_DELAY)
    print(f"\n[Client] ✓ All {total} options subscribed. Streaming + logging...\n")


# ── Main with auto-reconnect ──────────────────────────────────────────────

async def main():
    print(f"[Client] Connecting to {WS_URI}...")
    print(f"[Client] Using instrument data for: {TARGET_DATE}")
    logger         = TickLogger()
    option_symbols = []
    retry_delay    = 2

    try:
        while True:
            try:
                option_symbols = await run_session(option_symbols, logger)

            except websockets.exceptions.ConnectionClosedError as e:
                print(f"\n[Client] ✗ Connection closed: {e}")
                print(f"[Client] Reconnecting in {retry_delay}s...\n")
                await asyncio.sleep(retry_delay)

            except (ConnectionResetError, OSError) as e:
                print(f"\n[Client] ✗ Network error: {e}")
                print(f"[Client] Reconnecting in {retry_delay}s...\n")
                await asyncio.sleep(retry_delay)

            except KeyboardInterrupt:
                break

    finally:
        logger.close()


if __name__ == "__main__":
    asyncio.run(main())
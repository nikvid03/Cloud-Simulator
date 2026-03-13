import asyncio
import json
import os
import psycopg2
import psycopg2.pool
from datetime import datetime
from collections import defaultdict
from fastapi import WebSocket
from starlette.websockets import WebSocketState


class FeedDistributor:
    def __init__(self, poll_interval: float = 0.5, only_on_price_change: bool = False):
        self.poll_interval        = poll_interval
        self.only_on_price_change = only_on_price_change

        self._subscriptions: dict[str, set[WebSocket]] = defaultdict(set)
        self._last_ts:  dict[str, int]   = {}
        self._last_ltp: dict[str, float] = {}

        self._task: asyncio.Task | None = None
        self._running = False
        self._pool = None
        self._init_pool()

    # ------------------------------------------------------------------ #
    #  Connection pool                                                     #
    # ------------------------------------------------------------------ #

    def _init_pool(self):
        try:
            self._pool = psycopg2.pool.SimpleConnectionPool(
                minconn=2, maxconn=20,
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
            )
            print("[FeedDistributor] ✓ DB connection pool created")
        except Exception as e:
            print(f"[FeedDistributor] ✗ Failed to create DB pool: {e}")
            self._pool = None

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def subscribe(self, websocket: WebSocket, instruments: list[str]):
        if not self._is_alive(websocket):
            print(f"[FeedDistributor] ✗ Skipping subscribe — socket already closed")
            return
        for inst in instruments:
            self._subscriptions[inst].add(websocket)
        self._ensure_running()

    def unsubscribe(self, websocket: WebSocket, instruments: list[str]):
        for inst in instruments:
            self._subscriptions[inst].discard(websocket)

    def disconnect(self, websocket: WebSocket):
        for inst in list(self._subscriptions.keys()):
            self._subscriptions[inst].discard(websocket)
        print(f"[FeedDistributor] Cleaned up {websocket.client}")

    # ------------------------------------------------------------------ #
    #  WebSocket alive check                                               #
    # ------------------------------------------------------------------ #

    def _is_alive(self, websocket: WebSocket) -> bool:
        try:
            return websocket.client_state == WebSocketState.CONNECTED
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Poll loop                                                           #
    # ------------------------------------------------------------------ #

    def _ensure_running(self):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._poll_loop())
            self._task.add_done_callback(self._on_task_done)
            print("[FeedDistributor] Polling loop started")

    def _on_task_done(self, task: asyncio.Task):
        try:
            exc = task.exception()
            if exc:
                print(f"[FeedDistributor] ✗ Poll loop crashed: {exc}")
                import traceback
                traceback.print_exception(type(exc), exc, exc.__traceback__)
        except asyncio.CancelledError:
            pass
        self._running = False

    async def _poll_loop(self):
        print("[FeedDistributor] Poll loop running...")
        while True:
            await asyncio.sleep(self.poll_interval)
            self._purge_dead_sockets()

            active = [inst for inst, clients in self._subscriptions.items() if clients]
            if not active:
                continue

            today = datetime.now().strftime("%Y-%m-%d")
            for instrument in active:
                try:
                    new_ticks = await asyncio.to_thread(
                        self._fetch_new_ticks, instrument, today
                    )
                    if new_ticks:
                        await self._broadcast(instrument, new_ticks)
                except Exception as e:
                    print(f"[FeedDistributor] ✗ Error polling {instrument}: {e}")

    def _purge_dead_sockets(self):
        for inst in list(self._subscriptions.keys()):
            dead = {ws for ws in self._subscriptions[inst] if not self._is_alive(ws)}
            if dead:
                self._subscriptions[inst] -= dead

    # ------------------------------------------------------------------ #
    #  DB fetch — fetches ALL available columns                           #
    # ------------------------------------------------------------------ #

    def _is_index(self, instrument: str) -> bool:
        return instrument.startswith("NSE_INDEX")

    def _fetch_new_ticks(self, instrument: str, date: str) -> list[dict]:
        if self._pool is None:
            self._init_pool()
            if self._pool is None:
                return []

        last_ts = self._last_ts.get(instrument, 0)
        conn = None

        try:
            conn = self._pool.getconn()
            cur = conn.cursor()

            if self._is_index(instrument):
                # Index instruments — map indexLtp → ltp, no iv/delta/oi
                price_cols = """
                    "indexLtp"  AS ltp,
                    0           AS oi,
                    NULL        AS iv,
                    NULL        AS delta,
                    NULL        AS gamma,
                    NULL        AS theta,
                    NULL        AS vega
                """
            else:
                # Option / futures — fetch every greeks column available
                price_cols = """
                    "ltp",
                    "oi",
                    "iv",
                    "delta",
                    "gamma",
                    "theta",
                    "vega"
                """

            query = f"""
                SELECT DISTINCT ON (ts)
                    ts,
                    instrument,
                    strike,
                    expiry,
                    {price_cols}
                FROM "datafeedschema"."{os.getenv('TABLE_NAME')}"
                WHERE instrument = %s
                  AND tickd = %s
                  AND ts > %s
                ORDER BY ts ASC
            """
            cur.execute(query, (instrument, date, last_ts))
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            cur.close()

            if not rows:
                return []

            ticks = [dict(zip(cols, row)) for row in rows]
            self._last_ts[instrument] = ticks[-1]["ts"]

            if self.only_on_price_change:
                filtered = []
                prev_ltp = self._last_ltp.get(instrument)
                for tick in ticks:
                    ltp = float(tick.get("ltp") or 0)
                    if ltp != prev_ltp:
                        filtered.append(tick)
                        prev_ltp = ltp
                if filtered:
                    self._last_ltp[instrument] = float(filtered[-1].get("ltp") or 0)
                return filtered

            return ticks

        except Exception as e:
            print(f"[FeedDistributor] ✗ DB query failed for {instrument}: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return []
        finally:
            if conn:
                self._pool.putconn(conn)

    # ------------------------------------------------------------------ #
    #  Broadcast — sends ALL fields to subscribers                        #
    # ------------------------------------------------------------------ #

    async def _broadcast(self, instrument: str, ticks: list[dict]):
        subscribers = list(self._subscriptions.get(instrument, []))
        dead = set()

        for tick in ticks:
            payload = json.dumps({
                "instrument": tick.get("instrument"),
                "ts":         tick.get("ts"),
                "ltp":        float(tick.get("ltp")   or 0),
                "oi":         float(tick.get("oi")    or 0),
                "iv":         tick.get("iv"),           # ← Greeks
                "delta":      tick.get("delta"),
                "gamma":      tick.get("gamma"),
                "theta":      tick.get("theta"),
                "vega":       tick.get("vega"),
                "strike":     tick.get("strike"),
                "expiry":     str(tick.get("expiry", "")),
            }, default=str)

            for ws in subscribers:
                if not self._is_alive(ws):
                    dead.add(ws)
                    continue
                try:
                    await ws.send_text(payload)
                except Exception as e:
                    print(f"[FeedDistributor] ✗ Send failed: {e}")
                    dead.add(ws)

        for ws in dead:
            self.disconnect(ws)
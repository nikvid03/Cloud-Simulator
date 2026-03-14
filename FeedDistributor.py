import zmq
import logging
import threading
from collections import defaultdict
import json

logger = logging.getLogger(__name__)


class FeedDistributor:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(FeedDistributor, cls).__new__(cls)
        return cls._instance

    def config(self, zmq_pull_addr: str):
        self.zmq_pull_addr = zmq_pull_addr
        self.context = zmq.Context()
        self.pull_socket = self.context.socket(zmq.PULL)

        # instrument -> set of WebSocket connections
        self.vInsToConn = defaultdict(set)

        # ws -> (asyncio event loop, asyncio.Queue)
        self.connToQueue = {}

        self.map_lock = threading.Lock()
        self.running = False

    def start(self):
        if self.running:
            return
        self.pull_socket.bind(self.zmq_pull_addr)
        self.running = True
        self.thread = threading.Thread(target=self.__run, daemon=True)
        self.thread.start()

    def __run(self):
        while self.running:
            try:
                data = self.pull_socket.recv()
                raw = data.decode()
                try:
                    ticks_list = json.loads(raw)
                    if not isinstance(ticks_list, list):
                        logger.warning("Received non-list ZMQ message, skipping")
                        continue
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON batch received, skipping message")
                    continue

                for tick in ticks_list:
                    tick_json = json.dumps(tick)
                    # Instrument name is a key inside the feeds dict (Upstox format)
                    feeds = tick.get("feeds", {})
                    for instrument in feeds:
                        with self.map_lock:
                            clients = list(self.vInsToConn.get(instrument, set()))

                        for ws in clients:
                            entry = self.connToQueue.get(ws)
                            if entry:
                                loop, q = entry
                                try:
                                    # Thread-safe put into asyncio.Queue via the event loop
                                    loop.call_soon_threadsafe(q.put_nowait, tick_json)
                                except Exception as e:
                                    logger.error(
                                        "Queue put error for instrument %s: %s", instrument, e
                                    )
            except Exception as e:
                if self.running:
                    logger.error("Error in __run loop: %s", e, exc_info=True)

    def register(self, ws, loop, queue):
        """Associate a WebSocket with its asyncio event loop and queue."""
        with self.map_lock:
            self.connToQueue[ws] = (loop, queue)

    def subscribe(self, ws, instrument_list):
        with self.map_lock:
            for ins in instrument_list:
                self.vInsToConn[ins].add(ws)

    def unsubscribe(self, ws, instrument_list):
        with self.map_lock:
            for ins in instrument_list:
                self.vInsToConn[ins].discard(ws)
                if not self.vInsToConn[ins]:
                    del self.vInsToConn[ins]

    def disconnect(self, ws):
        with self.map_lock:
            for ins in list(self.vInsToConn):
                self.vInsToConn[ins].discard(ws)
                if not self.vInsToConn[ins]:
                    del self.vInsToConn[ins]
            self.connToQueue.pop(ws, None)

    def stop(self):
        self.running = False
        if hasattr(self, "thread"):
            self.thread.join(timeout=2)
        self.pull_socket.close()
        self.context.term()

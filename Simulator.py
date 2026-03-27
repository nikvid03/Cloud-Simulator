import time
import queue
import threading
import logging
import json
import zmq
import pandas as pd
from DBManager import DBManager
from DataLossTracker import DataLossTracker
from typing import List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RequestWindow:
    date: str       # e.g. "2025-07-24"
    start_time: int # epoch ms
    end_time: int   # epoch ms


class SimulatorConfigs:
    def __init__(self, pTargetAddress, pReqData: List[RequestWindow], pSpeed, pBatch=None, pInstruments=None):
        self.uReqData = pReqData
        self.uSpeed = pSpeed
        self.uBatch = pBatch
        self.uTargetAddress = pTargetAddress
        self.uInstruments = pInstruments  # list of instrument names to replay; None = all


class DbConfigs:
    def __init__(self):
        pass


class DataQueue:
    INSERTION_TIMEOUT = 1   # seconds
    RETRIEVAL_TIMEOUT = 5   # seconds

    def __init__(self):
        self.vQueue = queue.Queue(maxsize=5000)
        self.vCurrDataArray = []
        self.tracker = None

    def put(self, pDataPacket):
        if self.vBatchSize:
            if pDataPacket["currentTs"] <= (self.vNextTs - self.vBatchSize):
                if self.tracker:
                    instrument = next(iter(pDataPacket["feeds"]), "UNKNOWN")
                    self.tracker.record_stale_drop(instrument, pDataPacket["currentTs"])
                return  # stale, drop

            while pDataPacket["currentTs"] > (self.vNextTs - self.vBatchSize):
                self.__put({
                    "time_stamp": self.vNextTs,
                    "data": self.vCurrDataArray
                })
                self.vCurrDataArray = []
                self.vNextTs += self.vBatchSize

            self.vCurrDataArray.append(pDataPacket)

        else:
            if pDataPacket["currentTs"] < self.vNextTs:
                if self.tracker:
                    instrument = next(iter(pDataPacket["feeds"]), "UNKNOWN")
                    self.tracker.record_stale_drop(instrument, pDataPacket["currentTs"])
                return  # stale, drop

            if len(self.vCurrDataArray):
                if self.vCurrDataArray[-1]["currentTs"] == pDataPacket["currentTs"]:
                    self.vCurrDataArray.append(pDataPacket)
                else:
                    self.__put(
                        {"time_stamp": self.vCurrDataArray[0]["currentTs"], "data": self.vCurrDataArray}
                    )
                    self.vCurrDataArray = [pDataPacket]
            else:
                self.vCurrDataArray.append(pDataPacket)

            self.vNextTs = pDataPacket["currentTs"]

    def start(self, batch_size=None, start_ts=None, tracker=None):
        self.vCurrDataArray = []
        self.vBatchSize = batch_size
        self.vNextTs = start_ts
        self.tracker = tracker

    def end(self):
        """Flush any remaining accumulated ticks."""
        if len(self.vCurrDataArray):
            self.__put(
                {"time_stamp": self.vCurrDataArray[0]["currentTs"], "data": self.vCurrDataArray}
            )
            self.vCurrDataArray = []

    def get(self):
        return self.__get()

    def __put(self, pData):
        while True:
            try:
                self.vQueue.put(pData, timeout=self.INSERTION_TIMEOUT)
                return
            except queue.Full:
                logger.warning("Queue full, waiting to insert batch")
                continue

    def __get(self):
        try:
            return self.vQueue.get(timeout=self.RETRIEVAL_TIMEOUT)
        except queue.Empty:
            return None


class Simulator:
    DB_FETCH_BATCH_MS = 600000  # 10 minutes
    MAX_FETCH_RETRIES = 3

    def __init__(self, pSimConfig: SimulatorConfigs, pDbConfig: DbConfigs):
        self.vDataQueue = DataQueue()
        self.vSimConfig = pSimConfig
        self.vDbManager = DBManager()
        self.vRealStartTime = None  # lazy-init on first packet
        self.vSimStartTime = None
        self.vSpeed = pSimConfig.uSpeed
        self.vFetchDone = threading.Event()
        self.vTracker = DataLossTracker()
        self.zmq_context = zmq.Context()
        self.push_socket = self.zmq_context.socket(zmq.PUSH)
        self.push_socket.connect(pSimConfig.uTargetAddress)

    def start(self):
        self.vSimStartTime = self.vSimConfig.uReqData[0].start_time
        fetch_thread = threading.Thread(target=self.__fetch_loop, daemon=True)
        sender_thread = threading.Thread(target=self.__sender_loop, daemon=True)
        fetch_thread.start()
        sender_thread.start()

    def print_summary(self):
        """Print data loss summary — called on both natural completion and CTRL+C."""
        self.vTracker.print_summary()

    def __fetch_loop(self):
        try:
            for req_obj in self.vSimConfig.uReqData:
                start_ts = req_obj.start_time
                self.vDataQueue.start(self.vSimConfig.uBatch, start_ts, tracker=self.vTracker)

                while start_ts < req_obj.end_time:
                    end_ts = min(req_obj.end_time, start_ts + self.DB_FETCH_BATCH_MS)
                    df = self.__fetch_with_retry(req_obj.date, start_ts, end_ts)
                    if self.vSimConfig.uInstruments and not df.empty:
                        dropped = df[~df["instrument"].isin(self.vSimConfig.uInstruments)]
                        for _, row in dropped.iterrows():
                            # FetchBatch returns `ts` column; FetchDay aliases it as `ts_ms`. Handle both.
                            ts = int(row.get("ts_ms", row.get("ts", 0)))
                            self.vTracker.record_filter_drop(str(row["instrument"]), ts)
                        df = df[df["instrument"].isin(self.vSimConfig.uInstruments)]
                    logger.info("Fetched %d rows for %d-%d", len(df), start_ts, end_ts)
                    for _, row in df.iterrows():
                        tick_data = self.__convert_to_upstox(row)
                        self.vDataQueue.put(tick_data)
                    start_ts = end_ts + 1

                self.vDataQueue.end()

        except Exception as e:
            logger.error("__fetch_loop crashed: %s", e, exc_info=True)
        finally:
            self.vFetchDone.set()
            logger.info("Fetch loop complete")

    _TUNNEL_DOWN_MARKERS = ("Connection refused", "SSL SYSCALL", "could not connect")

    def __fetch_with_retry(self, date, start_ts, end_ts):
        for attempt in range(self.MAX_FETCH_RETRIES):
            try:
                return self.vDbManager.FetchBatch(date, start_ts, end_ts)
            except Exception as e:
                err = str(e)
                logger.warning("FetchBatch attempt %d failed: %s", attempt + 1, e)
                if attempt == self.MAX_FETCH_RETRIES - 1:
                    logger.error(
                        "Skipping chunk %d-%d after %d failures",
                        start_ts, end_ts, self.MAX_FETCH_RETRIES
                    )
                    self.vTracker.record_failed_chunk(date, start_ts, end_ts)
                    return pd.DataFrame()
                if any(m in err for m in self._TUNNEL_DOWN_MARKERS):
                    logger.warning(
                        "Tunnel appears down — recycling connection pool, waiting 30s for recovery"
                    )
                    self.vDbManager.engine.dispose()
                    time.sleep(30)
                else:
                    time.sleep(2 ** attempt)

    def __sender_loop(self):
        while True:
            data = self.vDataQueue.get()
            if data is None:
                if self.vFetchDone.is_set():
                    logger.info("Fetch complete and queue drained. Sender exiting.")
                    self.vTracker.print_summary()
                    break
                continue  # fetch still in progress, keep waiting

            # Lazy-init real clock on first packet to avoid race condition
            if self.vRealStartTime is None:
                self.vRealStartTime = time.time()

            self.__send_packet(data)

    def __send_packet(self, pData):
        packet_ts = pData["time_stamp"]
        data_array = pData["data"]

        real_elapsed = (time.time() - self.vRealStartTime) * 1000  # ms
        sim_elapsed = (packet_ts - self.vSimStartTime) / self.vSpeed  # ms
        wait_ms = sim_elapsed - real_elapsed

        if wait_ms > 100:
            time.sleep(wait_ms / 1000)

        try:
            self.push_socket.send_string(json.dumps(data_array))
            logger.debug("Sent batch of %d ticks at sim_ts=%d", len(data_array), packet_ts)

            # Tick-by-tick terminal output
            for tick in data_array:
                feeds = tick.get("feeds", {})
                for instrument, feed_data in feeds.items():
                    if "ltpc" in feed_data:
                        # NSE_INDEX path
                        ltpc = feed_data["ltpc"]
                        logger.info(
                            "[TICK] %-35s  ltp=%-10s  cp=%s",
                            instrument, ltpc.get("ltp", ""), ltpc.get("cp", "")
                        )
                    elif "ff" in feed_data:
                        # Options / equity path
                        mff = feed_data["ff"].get("marketFF", {})
                        ltpc = mff.get("ltpc", {})
                        greeks = mff.get("optionGreeks", {})
                        efeed = mff.get("eFeedDetails", {})
                        logger.info(
                            "[TICK] %-35s  ltp=%-10s  iv=%-8s  delta=%-8s  oi=%s",
                            instrument,
                            ltpc.get("ltp", ""),
                            greeks.get("iv", ""),
                            greeks.get("delta", ""),
                            efeed.get("oi", "")
                        )
        except Exception as e:
            logger.error("Failed to send over ZMQ: %s", e, exc_info=True)

    def __convert_to_upstox(self, data):
        ts_ms = int(data["ts_ms"])
        if str(data["instrument"]).startswith("NSE_INDEX|"):
            return {
                "feeds": {
                    data["instrument"]: {
                        "ltpc": {
                            "ltp": data["indexLtp"],
                            "ltt": data["ltt"],
                            "cp": data["cp"]
                        }
                    }
                },
                "currentTs": ts_ms
            }
        else:
            return {
                "feeds": {
                    data["instrument"]: {
                        "ff": {
                            "marketFF": {
                                "ltpc": {
                                    "ltp": data["ltp"],
                                    "ltt": data["ltt"],
                                    "ltq": data["ltq"],
                                    "cp": data["cp"]
                                },
                                "marketLevel": {
                                    "bidAskQuote": []
                                },
                                "optionGreeks": {
                                    "up": data["up"],
                                    "iv": data["iv"],
                                    "delta": data["delta"],
                                    "theta": data["theta"],
                                    "gamma": data["gamma"],
                                    "vega": data["vega"]
                                },
                                "marketOHLC": {
                                    "ohlc": []
                                },
                                "eFeedDetails": {
                                    "atp": data["atp"],
                                    "oi": data["oi"],
                                    "poi": data["poi"],
                                    "tbq": data["total_buy_qty"],
                                    "tsq": data["total_sell_qty"]
                                }
                            }
                        }
                    }
                },
                "currentTs": ts_ms
            }

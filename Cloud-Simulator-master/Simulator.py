import time
import queue
from DBManager import DBManager
from typing import List, Dict
from dataclasses import dataclass
import threading
import zmq
import json

@dataclass
class RequestWindow:
    date: str  # e.g. "2025-07-24"
    start_time: int  # epoch time
    end_time: int

class SimulatorConfigs:
    
    def __init__ (self, pTargetAddress, pReqData: List[RequestWindow], pSpeed, pBatch = None): # Instead take array of object (date, start_time, end_time)
        self.uReqData = pReqData
        self.uSpeed = pSpeed
        self.uBatch = pBatch
        self.uTargetAddress = pTargetAddress
        pass

class DbConfigs:
    def __init__ (self):
        pass

class DataQueue:

    def __init__ (self):
        self.INSERTION_TIMEOUT = 1000
        self.RETRIEVAL_TIMEOUT = 5000
        self.vQueue = queue.Queue(maxsize=5000) #TODO
        self.vCurrDataArray = []
        pass

    def put (self, pDataPacket): #in upstox format
        if self.vBatchSize:
            if pDataPacket["currentTs"] <= (self.vNextTs - self.vBatchSize):
                #ignore, time already passed
                return
            
            # empty packet scenario
            while pDataPacket["currentTs"] > (self.vNextTs - self.vBatchSize):
                self.__put({
                    "time_stamp": self.vNextTs,
                    "data": self.vCurrDataArray
                })
                self.vCurrDataArray = []
                self.vNextTs += self.vBatchSize

            self.vCurrDataArray.append(pDataPacket)
            
        else:
            if pDataPacket["currentTs"] < self.vNextTs: # start time is greater.
                return
            
            if len(self.vCurrDataArray):
                if self.vCurrDataArray[-1]["currentTs"] == pDataPacket["currentTs"]:
                    self.vCurrDataArray.append(pDataPacket)
                else:
                    self.__put (
                        {"time_stamp": self.vCurrDataArray[0]["currentTs"], "data": self.vCurrDataArray}
                    )
                    self.vCurrDataArray = [pDataPacket]

            else:
                self.vCurrDataArray.append(pDataPacket)
            
            self.vNextTs = pDataPacket["currentTs"]

    def start (self, batch_size = None, start_ts = None):
        self.vCurrDataArray = []
        self.vBatchSize = batch_size # None means no batching, this is to batch based on ms
        self.vNextTs = start_ts # send data at vNextTs (inclusive), in case of non batch its the epoch of last datapacket

    def end (self):
        if len(self.vCurrDataArray):
            self.__put(
                {"time_stamp": self.vCurrDataArray[0].time_stamp, "data": self.vCurrDataArray}
            )
            self.vCurrDataArray = []
            
    def get (self):
        return self.__get()
        
    def __put (self, pData):
        # print("inside actual put, queue size: ", self.vQueue.qsize())
        while True:
            try:
                self.vQueue.put (pData, timeout=self.INSERTION_TIMEOUT)
                return
            except queue.Full:
                print("[Queue] Full. Waiting to put batch...")
                continue
    
    def __get (self):
        try:
            data = self.vQueue.get(timeout=self.RETRIEVAL_TIMEOUT)
            return data
        except Exception as e:
            print("returning none!!!!")
            return None


class Simulator:

    def __init__ (self, pSimConfig: SimulatorConfigs, pDbConfig: DbConfigs):
        self.vDataQueue = DataQueue() # TODO uBatch can be null
        self.vSimConfig = pSimConfig
        self.vDbManager = DBManager()
        self.vRealStartTime = None
        self.vSimStartTime = None
        self.vSpeed = pSimConfig.uSpeed
        self.zmq_context = zmq.Context()
        self.push_socket = self.zmq_context.socket(zmq.PUSH)
        self.push_socket.connect(pSimConfig.uTargetAddress)

    def start (self):
        fetch_thread = threading.Thread(target=self.__fetch_loop, daemon=True)
        sender_thread = threading.Thread(target=self.__sender_loop, daemon=True)
        self.vSimStartTime = self.vSimConfig.uReqData[0].start_time

        fetch_thread.start()
        sender_thread.start()
        pass

    def send (self):
        
        pass

    def __fetch_loop (self):
        for req_obj in self.vSimConfig.uReqData: #
            start_ts = req_obj.start_time
            self.vDataQueue.start(self.vSimConfig.uBatch, start_ts)

            # df = self.vDbManager.FetchDay(req_obj.date)
            # for _, row in df.iterrows():
            #     tick_data = self.__convert_to_upstox(row)
            #     if tick_data["currentTs"] > req_obj.end_time:
            #         break
            #     self.vDataQueue.put(tick_data)

            # self.vDataQueue.end()
            
            DB_FETCH_BATCH_OF_MS = 1800000 #30 minutes around 3600 entries
            while start_ts < req_obj.end_time:
                end_ts = min(req_obj.end_time, start_ts + DB_FETCH_BATCH_OF_MS)
                df = self.vDbManager.FetchBatch(req_obj.date, start_ts, end_ts)
                print("data fetched", len(df))
                for _, row in df.iterrows():
                    tick_data = self.__convert_to_upstox(row)
                    self.vDataQueue.put(tick_data)
                    # print(_)
                start_ts = end_ts + 1
            

    def __sender_loop (self):
        self.vRealStartTime = time.time()
        while True:
            data = self.vDataQueue.get()
            # print(f"[Sender] Got batch of size {len(data['data'])}")
            if data:
                self.__send_packet(data)
    
    def __send_packet(self, pData):
        packet_ts = pData["time_stamp"]
        data_array = pData["data"]

        real_elapsed = (time.time() - self.vRealStartTime) * 1000  # ms
        sim_elapsed = (packet_ts - self.vSimStartTime) / self.vSpeed
        wait_ms = sim_elapsed - real_elapsed
        print("wait_ms: ", wait_ms)
        if wait_ms > 100:
            wait_s = wait_ms / 1000

            time.sleep(wait_s)

        # print(f"[Simulator] Sending {len(data_array)} ticks at simulated ts={packet_ts}")
        messages = [json.dumps(tick) for tick in data_array]
        message_str = "|".join(messages)  # same delimiter used by FeedDistributor
        try:
            self.push_socket.send_string(message_str)
            print(f"[Simulator] Sent batch of {len(data_array)} ticks at sim_ts={packet_ts}")
        except Exception as e:
            print(f"[Simulator] Failed to send over ZMQ: {e}")

    def __convert_to_upstox(self, data):
        ts_ms = int(data["ts"])
        # print("ts_ms: ", ts_ms)
        if (data["instrument"]=="NSE_INDEX|Nifty 50"):
            return {
                "feeds":{
                    data["instrument"] : {
                        "ltpc":{
                            "ltp":data["indexLtp"],
                            "ltt": data["ltt"],
                            "cp":data["cp"]
                        }
                    }
                },
                "currentTs":ts_ms
            }
        else:
            return {
                "feeds":{
                    data["instrument"]: {
                        "ff":{
                            "marketFF": {
                                "ltpc":{
                                    "ltp":data["ltp"],
                                    "ltt":data["ltt"],
                                    "ltq":data["ltq"],
                                    "cp":data["cp"]
                                },
                                "marketLevel":{
                                    "bidAskQuote":[]
                                },
                                "optionGreeks":{
                                    "up":data["up"],
                                    "iv":data["iv"],
                                    "delta":data["delta"],
                                    "theta":data["theta"],
                                    "gamma":data["gamma"],
                                    "vega":data["vega"]
                                },
                                "marketOHLC":{
                                    "ohlc":[]
                                },
                                "eFeedDetails":{
                                    "atp":data["atp"],
                                    "oi":data["oi"],
                                    "poi":data["poi"],
                                    "tbq":data["total_buy_qty"],
                                    "tsq":data["total_sell_qty"]
                                }
                            }
                        }
                    }
                },
                "currentTs":ts_ms
            }

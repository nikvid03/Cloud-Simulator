import signal
import logging
from logging_config import setup_logging
from Simulator import Simulator, SimulatorConfigs, RequestWindow, DbConfigs
from FeedDistributor import FeedDistributor
from WebSocketServer import WebSocketServer
from dotenv import load_dotenv

load_dotenv()

ZMQ_ADDRESS = "tcp://127.0.0.1:5555"

logger = logging.getLogger(__name__)


def main():
    setup_logging()

    req_data = [
        RequestWindow(date="2025-08-06", start_time=1754457000000, end_time=1754471400000)
    ]

    # Set to a list of instrument names to replay only those instruments.
    # Set to None to replay all instruments (default).
    # Example: ["NSE_INDEX|Nifty 50", "NSE_OPT|NIFTY25AUG24500CE", "NSE_FO|NIFTY25AUGFUT"]
    instruments = None

    sim_config = SimulatorConfigs(
        pTargetAddress=ZMQ_ADDRESS,
        pReqData=req_data,
        pSpeed=10,
        pBatch=None,
        pInstruments=instruments
    )
    db_config = DbConfigs()

    simulator = Simulator(pSimConfig=sim_config, pDbConfig=db_config)

    distributor = FeedDistributor()
    distributor.config(zmq_pull_addr=ZMQ_ADDRESS)

    def shutdown(sig, frame):
        logger.info("Shutting down gracefully.")
        simulator.print_summary()
        distributor.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    simulator.start()
    distributor.start()

    server = WebSocketServer(host="127.0.0.1", port=8000)
    server.start()  # blocks until uvicorn exits


if __name__ == "__main__":
    main()

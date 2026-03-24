import os
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self):
        connection_string = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        self.engine = create_engine(
            connection_string,
            connect_args={
                "connect_timeout": 30,
                "options": "-c statement_timeout=600000"  # 10 min query timeout (large tick batches over remote RDS)
            },
            pool_pre_ping=True  # health-check connections before use
        )
        self.table_name = os.getenv('TABLE_NAME') or 'upstox_table'

    def FetchDay(self, date: str):
        """Fetch entire day's data ordered by timestamp."""
        query = f"""
            SELECT *, ts AS ts_ms
            FROM datafeedschema.{self.table_name}
            WHERE tickd = %(tickd)s
            ORDER BY ts
        """
        df = pd.read_sql(query, self.engine, params={"tickd": date})
        return df

    def FetchBatch(self, date: str, start_epoch: int, end_epoch: int):
        """Fetch ticks for date within [start_epoch, end_epoch]."""
        logger.info("Fetching %s epoch %d-%d", date, start_epoch, end_epoch)
        query = f"""
            SELECT *, ts AS ts_ms FROM datafeedschema.{self.table_name}
            WHERE tickd = %(tickd)s
              AND ts >= %(start_epoch)s
              AND ts <= %(end_epoch)s
            ORDER BY ts
        """
        params = {"tickd": date, "start_epoch": start_epoch, "end_epoch": end_epoch}
        df = pd.read_sql(query, self.engine, params=params)
        return df

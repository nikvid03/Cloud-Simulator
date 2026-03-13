import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

class DBManager:
    def __init__(self):
        # ideally get from pDbConfig, here use env vars
        connection_string = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        print(connection_string)    
        self.engine = create_engine(connection_string)
        self.table_name = os.getenv('TABLE_NAME') or 'upstox_table'

    def FetchDay(self, date: str):
        """
        Fetch entire day's data filtered by tickd.
        """
        query = f"""
            SELECT *,
                (extract(epoch from ts::timestamp) * 1000)::bigint as ts_ms
            FROM datafeedschema.{self.table_name}
            WHERE tickd = %(tickd)s
            ORDER BY ts
        """
        df = pd.read_sql(query, self.engine, params={"tickd": date})
        return df

    def FetchBatch(self, date: str, start_epoch: int, end_epoch: int):
        """
        Fetch data in tickd=date where ts_ms between start_epoch and end_epoch.
        """
        print("fetching batch", date, start_epoch, end_epoch)
        query = f"""
            SELECT * FROM datafeedschema.{self.table_name} WHERE tickd = %(tickd)s
              AND ts >= %(start_epoch)s
              AND ts <= %(end_epoch)s
            ORDER BY ts
        """
        params = {"tickd": date, "start_epoch": start_epoch, "end_epoch": end_epoch}
        df = pd.read_sql(query, self.engine, params=params)
        return df

import pandas as pd
import boto3
import json
import os
import psycopg2
from dotenv import load_dotenv
import sys
from datetime import datetime, timedelta
import numpy as np
 
# Load environment variables
load_dotenv()
 
# Variables
start_date = "2026-01-01"  # Start date
end_date = "2026-03-31"    # End date
prev_period_consideration = 7  # in days
sd_to_consider = 2    
 
# S3 and Database functions
def fetch_s3_instruments(date):
    """Fetch instruments mapping from S3 for a given date"""
    parsed_date = datetime.strptime(date, "%Y-%m-%d")
    year = parsed_date.year
    month = f"{parsed_date.month:02}"
    day = f"{parsed_date.day:02}"
   
    BUCKET_NAME = 'mq-data-feed-dump-s3-prod-v1'
    KEY = f'dailyInstruments/{year}/{month}/{day}/mapping-instruments-{year}-{month}-{day}.json'
   
    try:
        s3 = boto3.resource('s3')
        response = s3.Object(BUCKET_NAME, KEY).get()
        data = response['Body'].read().decode("utf-8").replace("'", '"')
        options_array = json.loads(data)
        return options_array
    except Exception as e:
        print(f"  ✗ Failed to fetch S3 data for {date}: {e}")
        return None
 
def test_db_connection():
    """Test if database connection works"""
    print("\nTesting database connection...")
    print(f"  Host: {os.getenv('DB_HOST')}")
    print(f"  Port: {os.getenv('DB_PORT')}")
    print(f"  Database: {os.getenv('DB_NAME')}")
    print(f"  User: {os.getenv('DB_USER')}")
   
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        conn.close()
        print("✓ Database connection successful!\n")
        return True
    except psycopg2.OperationalError as e:
        print(f"✗ Database connection failed!")
        print(f"  Error: {e}\n")
        print("Troubleshooting steps:")
        print("1. Check if PostgreSQL is running: sudo systemctl status postgresql")
        print("2. Verify credentials in .env file")
        print("3. Check if port 5432 is accessible")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}\n")
        return False
 
def getData(instrument, date):
    """Get data from the database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
 
        cur = conn.cursor()
        query = f"""SELECT * FROM "datafeedschema"."{os.getenv("TABLE_NAME")}"
        WHERE "instrument" = %s AND "tickd" = %s;"""
        cur.execute(query, (instrument, date))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        if rows:
            return pd.DataFrame(rows, columns=columns)
        else:
            return pd.DataFrame(columns=columns)
    except Exception as e:
        # print(f"Query failed for {instrument}: {e}")
        return pd.DataFrame()
 
def get_atm(price, gap):
    return round(float(price) / gap) * gap
 
def getNiftyInstruments(nifty50, options_array, strike_range_multiplier=1.0):
    """
    Get Nifty options for ALL expiries
    Select 7 strikes above and 7 strikes below ATM (total 15 strikes)
    """
    current_price = nifty50['indexLtp'].iloc[0]
    atm_strike = get_atm(current_price, 50)
   
    lower = 7
    upper = 7
    nifty_strikes = [atm_strike + (i * 50) for i in range(-lower, upper + 1)]
 
    nifty_options = [option for option in options_array
                     if str(option['tradingSymbol']).startswith("NIFTY")]
 
    nifty_options.sort(key=lambda x: datetime.strptime(x['expiry'], '%Y-%m-%d'))
   
    if not nifty_options:
        return [], {}, [], None, None
   
    unique_expiries = sorted(list(set([opt['expiry'] for opt in nifty_options])))
    print(f"  DEBUG: Found {len(unique_expiries)} unique expiries: {unique_expiries}")
 
    instruments = []
    instruments_mapping = {}
   
    # ✅ Process ALL expiries (no filter)
    for option in nifty_options:
        expiry = option['expiry']
        strike = float(option['strike'])
       
        if strike in nifty_strikes:
            if strike not in instruments_mapping:
                instruments_mapping[strike] = {}
            if expiry not in instruments_mapping[strike]:
                instruments_mapping[strike][expiry] = {}
           
            if option['tradingSymbol'].endswith('CE'):
                instruments.append(option['tradingSymbol'])
                instruments_mapping[strike][expiry]['CE'] = option['tradingSymbol']
            elif option['tradingSymbol'].endswith('PE'):
                instruments.append(option['tradingSymbol'])
                instruments_mapping[strike][expiry]['PE'] = option['tradingSymbol']
 
    # Return all expiries info
    current_expiry = unique_expiries[0] if unique_expiries else None
    last_expiry = unique_expiries[-1] if unique_expiries else None
 
    return instruments, instruments_mapping, nifty_strikes, current_expiry, last_expiry
 
def processData(df):
    if df.empty:
        return df
       
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    df['ts'] = df['ts'].dt.tz_convert('Asia/Kolkata')
    df = df.sort_values(by='ts').reset_index(drop=True)
    df = df.sort_values('ts').set_index('ts')
    df.index = df.index.tz_convert("Asia/Kolkata")
    date_only = df.index[0].date()
    start = pd.Timestamp(f"{date_only} 09:15:00", tz="Asia/Kolkata")
    end = pd.Timestamp(f"{date_only} 15:30:00", tz="Asia/Kolkata")
    full_index = pd.date_range(start, end, freq="1s", tz="Asia/Kolkata")
    resampled = df.resample("1s").last()
    resampled = resampled.reindex(full_index)
    resampled = resampled.bfill().ffill().reset_index()
    resampled = resampled.rename(columns={"index": "ts"})
    df = resampled
 
    return df
 
def getInstrumentsData(date, instruments, include_index=True):
    """
    Fetch and save data for instruments for a specific date
    """
    # Create folder for this date
    folder_name = f"nifty_data_{date}"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
 
    successful = 0
    failed = 0
   
    # Add Nifty 50 index to the list if requested
    all_instruments = instruments.copy()
    if include_index:
        all_instruments.insert(0, "NSE_INDEX|Nifty 50")
   
    for i, instrument in enumerate(all_instruments, 1):
        # Reduced verbosity - only show every 10th instrument or important ones
        if i == 1 or i % 10 == 0 or i == len(all_instruments):
            print(f"  [{i}/{len(all_instruments)}] Processing {instrument}...")
 
        try:
            # Get raw data
            df = getData(instrument, date)
 
            if df.empty:
                failed += 1
                continue
 
            # Process data
                        # Process data
            processed_df = processData(df)
 
            # Keep only required columns
            required_cols = ["ts","instrument", "strike", "expiry","ltp", "oi"]
            processed_df = processed_df[[c for c in required_cols if c in processed_df.columns]]
 
            # Save to CSV
            safe_name = instrument.replace("|", "_").replace(" ", "_")
            file_path = os.path.join(folder_name, f"{safe_name}.csv")
            processed_df.to_csv(file_path, index=False)
 
 
            successful += 1
           
        except Exception as e:
            failed += 1
 
    return successful, failed, folder_name
 
def get_trading_days(start_date, end_date):
    """
    Generate list of dates between start and end (excluding weekends)
    You can customize this to exclude holidays as well
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
   
    dates = []
    current = start
    while current <= end:
        # Exclude Saturday (5) and Sunday (6)
        if current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
   
    return dates
 
# Main execution
if __name__ == "__main__":
    print("="*60)
    print("NIFTY OPTIONS - MONTHLY DATA FETCHER")
    print("="*60)
   
    # Test database connection first
    if not test_db_connection():
        print("\n⚠️  Cannot proceed without database connection.")
        sys.exit(1)
   
    # Get all trading days6
    trading_days = ["2026-01-09"]
    print(f"\nFound {len(trading_days)} trading days between {start_date} and {end_date}")
    print(f"Days: {trading_days[:5]}...{trading_days[-2:]}\n")
   
    proceed = input("Proceed with data download for all these dates? (y/n): ")
    if proceed.lower() != 'y':
        print("Cancelled.")
        sys.exit(0)
   
    # Track overall progress
    total_successful = 0
    total_failed = 0
    processed_dates = 0
    skipped_dates = 0
   
    print("\n" + "="*60)
    print("Starting monthly data download...")
    print("="*60 + "\n")
   
    for date_idx, date in enumerate(trading_days, 1):
        print(f"\n{'='*60}")
        print(f"Processing Date {date_idx}/{len(trading_days)}: {date}")
        print(f"{'='*60}")
       
        # Step 1: Fetch S3 instruments mapping
        print(f"  Fetching S3 instruments mapping...")
        options_array = fetch_s3_instruments(date)
        if options_array is None:
            print(f"  ⚠️  Skipping {date} - no S3 data available")
            skipped_dates += 1
            continue
       
        # Step 2: Fetch Nifty 50 index data
        print(f"  Fetching Nifty 50 index data...")
        nifty50 = getData("NSE_INDEX|Nifty 50", date)
       
        if nifty50.empty:
            print(f"  ⚠️  Skipping {date} - no Nifty index data")
            skipped_dates += 1
            continue
       
        print(f"  ✓ Nifty range: {nifty50['indexLtp'].min():.2f} - {nifty50['indexLtp'].max():.2f}")
       
        # Step 3: Get instruments list
        print(f"  Generating instruments list...")
        instruments, instruments_mapping, strike_range, current_expiry, next_expiry = getNiftyInstruments(
            nifty50,
            options_array,
            strike_range_multiplier=1.0
        )
       
        if not instruments:
            print(f"  ⚠️  Skipping {date} - no instruments found")
            skipped_dates += 1
            continue
       
        print(f"  ✓ All Expiries: {current_expiry} to {next_expiry}")
        print(f"  ✓ Total Instruments: {len(instruments)}, Strikes: {len(instruments_mapping)}")
       
        # Step 4: Download data
        print(f"  Downloading data for {len(instruments) + 1} instruments...")
        successful, failed, folder = getInstrumentsData(date, instruments, include_index=True)
       
        total_successful += successful
        total_failed += failed
        processed_dates += 1
       
        print(f"\n  Date Summary: ✓ {successful} successful, ✗ {failed} failed")
        print(f"  Saved to: {folder}")
   
    # Final summary
    print("\n" + "="*60)
    print("MONTHLY DOWNLOAD COMPLETE")
    print("="*60)
    print(f"Dates processed: {processed_dates}/{len(trading_days)}")
    print(f"Dates skipped: {skipped_dates}")
    print(f"Total files downloaded: {total_successful}")
    print(f"Total failures: {total_failed}")
    print("="*60)
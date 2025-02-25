import asyncio
import logging
import os
from datetime import date, datetime
import asyncpg
import redis.asyncio as redis
import json
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def connect_to_database():
    """Establish connection to PostgreSQL database using asyncpg."""
    try:
        pool = await asyncpg.create_pool(
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database=os.getenv('POSTGRES_DB'),
            host=os.getenv('POSTGRES_HOST'),
            port=5432
        )
        logger.info("Successfully connected to PostgreSQL database")
        return pool
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise

async def connect_to_redis():
    """Establish connection to Redis using redis.asyncio."""
    try:
        redis_conn = await redis.Redis(host='localhost', port=6379, decode_responses=True)
        logger.info("Successfully connected to Redis")
        return redis_conn
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        raise

async def get_latest_records(pool):
    """Fetch the latest record for each ticker from PostgreSQL."""
    try:
        async with pool.acquire() as conn:
            query = """
                SELECT ticker, timestamp, expiration, spot, zero_gamma, 
                       major_pos_vol, major_neg_vol, 
                       sum_gex_vol
                FROM livegex_gex
                WHERE (ticker, timestamp) IN (
                    SELECT ticker, MAX(timestamp) 
                    FROM livegex_gex 
                    GROUP BY ticker
                )
                AND expiration >= $1
                AND spot > 0
                AND zero_gamma > 0
                AND major_pos_vol > 0
                AND major_neg_vol > 0
            """
            today = date.today().isoformat()  # Convert date to string in 'YYYY-MM-DD' format
            logger.info(f"Executing query with today = {today}")
            rows = await conn.fetch(query, today)  # Use fetch() to get all matching rows
            for row in rows:
                logger.info(f"Found record for ticker: {row['ticker']}")
                yield row
    except Exception as e:
        logger.error(f"Error fetching latest records: {e}")
        raise

async def check_new_records(pool, last_timestamps):
    """Check for new records with later timestamps for each ticker every 5 seconds."""
    while True:
        try:
            async with pool.acquire() as conn:
                for ticker, last_timestamp in list(last_timestamps.items()):
                    query = """
                        SELECT timestamp, ticker, expiration, spot, zero_gamma, 
                               major_pos_vol, major_neg_vol, sum_gex_vol
                        FROM livegex_gex
                        WHERE ticker = $1
                        AND timestamp > $2
                        AND expiration >= $3
                        AND spot > 0
                        AND zero_gamma > 0
                        AND major_pos_vol > 0
                        AND major_neg_vol > 0
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """
                    today = date.today().isoformat()  # Convert date to string in 'YYYY-MM-DD' format
                    logger.debug(f"Checking for new records for ticker {ticker}, last timestamp: {last_timestamp}, today: {today}")
                    row = await conn.fetchrow(query, ticker, last_timestamp, today)  # Use fetchrow() for a single row
                    if row:
                        logger.info(f"Found new record for ticker {ticker}")
                        last_timestamps[ticker] = row['timestamp']  # Update last timestamp using column name
                        yield row
            await asyncio.sleep(20)  # Check every 5 seconds
        except Exception as e:
            logger.error(f"Error checking new records: {e}")
            await asyncio.sleep(20)  # Wait before retrying

async def fetch_and_publish_data():
    """Fetch and stream data from PostgreSQL, publishing to Redis Pub/Sub."""
    pool = await connect_to_database()
    redis_conn = await connect_to_redis()

    try:
        # Initialize last_timestamps dictionary to track the latest timestamp for each ticker
        last_timestamps = {}

        # Get and publish initial latest records for each ticker
        logger.info("Starting to fetch and publish initial latest records")
        async for row in get_latest_records(pool):
            ticker = row['ticker']  # Access by column name instead of index
            last_timestamps[ticker] = row['timestamp']  # Access by column name
            data = {
                "msg_type": "gex2",  # Add message type to indicate fake data
                "data": {"timestamp": row['timestamp'].isoformat(),
                    "ticker": row['ticker'],
                    "expiration": row['expiration'],
                    "spot": float(row['spot']),
                    "zero_gamma": float(row['zero_gamma']),
                    "major_pos_vol": float(row['major_pos_vol']),
                    "major_neg_vol": float(row['major_neg_vol']),
                    "sum_gex_vol": float(row['sum_gex_vol'])
                },
                "trades": {},
                "strikes": {},
            }
            
            data['event_id'] = f"gex2:{datetime.now().isoformat()}"  # Match SSE event ID format
            message_data = json.dumps(data)
            await redis_conn.publish('gex2', message_data)
            logger.info(f"Published initial data for {ticker} to Redis channel 'gex2'")

        # Continuously check for new records every 5 seconds
        logger.info("Starting to check for new records every 5 seconds")
        async for row in check_new_records(pool, last_timestamps):
            ticker = row['ticker']
            last_timestamps[ticker] = row['timestamp']  # Update last timestamp
            data = {
                "msg_type": "gex2",  # Add message type to indicate fake data
                "data": {"timestamp": row['timestamp'].isoformat(),
                    "ticker": row['ticker'],
                    "expiration": row['expiration'],
                    "spot": float(row['spot']),
                    "zero_gamma": float(row['zero_gamma']),
                    "major_pos_vol": float(row['major_pos_vol']),
                    "major_neg_vol": float(row['major_neg_vol']),
                    "sum_gex_vol": float(row['sum_gex_vol'])
                },
                "trades": {},
                "strikes": {},
            }
            data['event_id'] = f"gex2:{datetime.now().isoformat()}"  # Match SSE event ID format
            message_data = json.dumps(data)
            await redis_conn.publish('gex2', message_data)
            logger.info(f"Published new data for {ticker} to Redis channel 'gex2'")

    except Exception as e:
        logger.error(f"Error in fetch_and_publish_data: {e}")
    finally:
        await pool.close()
        await redis_conn.aclose()

async def main():
    """Main function to run the script."""
    try:
        await fetch_and_publish_data()
    except Exception as e:
        logger.error(f"Main error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import logging
import os
from datetime import date, datetime
import asyncpg
import redis.asyncio as redis
import json
from dotenv import load_dotenv

load_dotenv()

GEX_CHANNEL = "gex3"
SLEEP_TIME = 20

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def connect_to_database():
    """Establish connection to PostgreSQL database using asyncpg."""
    try:
        pool = await asyncpg.create_pool(
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            database="api",
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
        # Use environment variable for Redis host with localhost as fallback
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_conn = await redis.Redis(
            host=redis_host,
            port=6379,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0
        )
        # Test connection
        await redis_conn.ping()
        logger.info(f"Successfully connected to Redis at {redis_host}")
        return redis_conn
    except Exception as e:
        logger.error(f"Error connecting to Redis: {e}")
        raise


async def get_latest_records(pool):
    """Fetch the latest record for each ticker from PostgreSQL (gexray3 table)."""
    try:
        async with pool.acquire() as conn:
            query = """
                    SELECT msg_type, timestamp, ticker, expiration, spot, zero_gamma, major_pos_vol, major_neg_vol, sum_gex_vol, minor_pos_vol, minor_neg_vol, trades, strikes
                    FROM gexray3
                    WHERE (ticker \
                        , timestamp) IN (
                        SELECT ticker \
                        , MAX (timestamp)
                        FROM gexray3
                        GROUP BY ticker
                        )
                      AND spot \
                        > 0
                      AND zero_gamma \
                        > 0 \
                    """
            rows = await conn.fetch(query)
            for row in rows:
                logger.info(f"Found record for ticker: {row['ticker']}")
                yield row
    except Exception as e:
        logger.error(f"Error fetching latest records: {e}")
        raise


async def check_new_records(pool, last_timestamps):
    """Check for new records with later timestamps for each ticker every 20 seconds."""
    while True:
        try:
            async with pool.acquire() as conn:
                for ticker, last_timestamp in list(last_timestamps.items()):
                    query = """
                            SELECT msg_type, timestamp, ticker, expiration, spot, zero_gamma, major_pos_vol, major_neg_vol, sum_gex_vol, minor_pos_vol, minor_neg_vol, trades, strikes
                            FROM gexray3
                            WHERE ticker = $1
                              AND timestamp \
                                > $2
                              AND spot \
                                > 0
                              AND zero_gamma \
                                > 0
                            ORDER BY timestamp DESC
                                LIMIT 1 \
                            """
                    logger.debug(f"Checking for new records for ticker {ticker}, last timestamp: {last_timestamp}")
                    row = await conn.fetchrow(query, ticker, last_timestamp)
                    if row:
                        logger.info(f"Found new record for ticker {ticker}")
                        last_timestamps[ticker] = row['timestamp']
                        yield row
            await asyncio.sleep(SLEEP_TIME)  # Check every 20 seconds
        except Exception as e:
            logger.error(f"Error checking new records: {e}")
            await asyncio.sleep(SLEEP_TIME)  # Wait before retrying


async def fetch_and_publish_data():
    """Fetch and stream data from PostgreSQL gexray3 table, publishing to Redis Pub/Sub."""
    pool = await connect_to_database()
    redis_conn = await connect_to_redis()

    try:
        # Initialize last_timestamps dictionary to track the latest timestamp for each ticker
        last_timestamps = {}

        # Get and publish initial latest records for each ticker
        logger.info("Starting to fetch and publish initial latest records from gexray3")
        async for row in get_latest_records(pool):
            ticker = row['ticker']
            last_timestamps[ticker] = row['timestamp']

            # Format data for Redis
            data = {
                "msg_type": GEX_CHANNEL,
                "data": {
                    "timestamp": row['timestamp'].isoformat(),
                    "ticker": row['ticker'],
                    "expiration": row['expiration'],
                    "spot": float(row['spot']),
                    "zero_gamma": float(row['zero_gamma']),
                    "major_pos_vol": float(row['major_pos_vol']),
                    "major_neg_vol": float(row['major_neg_vol']),
                    "sum_gex_vol": float(row['sum_gex_vol']),
                    "minor_pos_vol": float(row['minor_pos_vol']),
                    "minor_neg_vol": float(row['minor_neg_vol'])
                },
                # Parse JSON fields from database
                "trades": json.loads(row['trades']) if row['trades'] else [],
                "strikes": json.loads(row['strikes']) if row['strikes'] else []
            }

            data['event_id'] = f"{GEX_CHANNEL}:{datetime.now().isoformat()}"
            message_data = json.dumps(data)

            # Publish to the gex channel
            await redis_conn.publish(GEX_CHANNEL, message_data)
            logger.info(f"Published initial data for {ticker} to Redis channel {GEX_CHANNEL}")

        # Continuously check for new records every 20 seconds
        logger.info("Starting to check for new records every 20 seconds")
        async for row in check_new_records(pool, last_timestamps):
            ticker = row['ticker']
            last_timestamps[ticker] = row['timestamp']

            # Format data for Redis
            data = {
                "msg_type": "gex2",
                "data": {
                    "timestamp": row['timestamp'].isoformat(),
                    "ticker": row['ticker'],
                    "expiration": row['expiration'],
                    "spot": float(row['spot']),
                    "zero_gamma": float(row['zero_gamma']),
                    "major_pos_vol": float(row['major_pos_vol']),
                    "major_neg_vol": float(row['major_neg_vol']),
                    "sum_gex_vol": float(row['sum_gex_vol']),
                    "minor_pos_vol": float(row['minor_pos_vol']),
                    "minor_neg_vol": float(row['minor_neg_vol'])
                },
                # Parse JSON fields from database
                "trades": json.loads(row['trades']) if row['trades'] else [],
                "strikes": json.loads(row['strikes']) if row['strikes'] else []
            }

            data['event_id'] = f"{GEX_CHANNEL}:{datetime.now().isoformat()}"
            message_data = json.dumps(data)

            # Publish to the gex channel
            await redis_conn.publish(GEX_CHANNEL, message_data)
            logger.info(f"Published new data for {ticker} to Redis channel {GEX_CHANNEL}")

    except Exception as e:
        logger.error(f"Error in fetch_and_publish_data: {e}")
    finally:
        await pool.close()
        await redis_conn.aclose()


async def main():
    """Main function to run the script."""
    try:
        await fetch_and_publish_data()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Main error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
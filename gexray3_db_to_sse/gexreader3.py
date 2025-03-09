"""Gexrayreader3

This script gets the latest records from the PostgreSQL 'gexray3' table and publishes to a Redis pubsub.

It connects to the new API and publishes data in GEX3 format, which includes additional fields
like minor_pos_vol, minor_neg_vol, and JSON for trades and strikes.

The script polls the database for new records and publishes them to Redis.

This is run in a docker container that is started and stopped with crontab.
When changing the script make sure to build the container.

```
cd /apps/gexreader/gexray3_db_to_sse

docker compose stop gexrayreader3
docker compose build

# Run the container and check the logs for errors.
docker compose up -d
docker logs gexrayreader3 -f

# This will leave the container built and ready to start by cron.
docker stop gexrayreader3

# Make sure the container is ready to run
docker ps -a
```

Attributes
    REDIS_CLIENT_NAME (str): Gives the connection a name for redis client list.
        You can check the name in redis-cli with CLIENT_LIST
    REDIS_PUBSUB_CHANNEL (str): This is the redis pubsub channel it publishes to.
        This is important because it will decide what SSE channel to publish to and also what `msg_type` is in the
        SSE message that is sent.
    LOOP_SLEEP_TIME (int): This is how many seconds between each fetch/publish loop.

Environment Variables:
    REDIS_HOST (str):
    REDIS_PORT (int):
    REDIS_DB (str):
    POSTRGRES_USER (str):
    POSTGRES_PASSWORD (str):
    POSTGRES_DB (str):
    POSTGRES_HOST (str):
    POSTGRES_PORT (int):


Todo:
    * Check the asyncpg connection and connection pool.

"""

import asyncio
import json
import logging
import os
from datetime import date, datetime
from dataclasses import dataclass
from typing import Dict, Any, AsyncGenerator, Optional

import asyncpg
import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Module Attributes
REDIS_CLIENT_NAME = "gexrayreader3"
GEX_CHANNEL = "gex2"
SLEEP_TIME = 5


@dataclass
class DBConfig:
    """Database connection configuration."""
    user: str = os.getenv('POSTGRES_USER', 'postgres')
    password: str = os.getenv('POSTGRES_PASSWORD', '')
    database: str = os.getenv('POSTGRES_DB', None)
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = os.getenv('POSTGRES_PORT', 5432)


@dataclass
class RedisConfig:
    """Redis connection configuration."""
    host: str = os.getenv('REDIS_HOST', 'localhost')
    port: int = int(os.getenv('REDIS_PORT', '6379'))
    db: int = int(os.getenv('REDIS_DB', '0'))
    client_name: str = REDIS_CLIENT_NAME


class GexrayReader3:
    """Class to handle fetching and publishing data from the gexray3 table."""

    def __init__(
            self,
            db_config: Optional[DBConfig] = None,
            redis_config: Optional[RedisConfig] = None
    ):
        """
        Initialize the GexrayReader3.

        Args:
            db_config: Database connection configuration
            redis_config: Redis connection configuration
        """
        self.db_config = db_config or DBConfig()
        self.redis_config = redis_config or RedisConfig()
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_conn: Optional[redis.Redis] = None
        self._last_timestamps: Dict[str, datetime] = {}

    async def connect_to_database(self) -> asyncpg.Pool:
        """Establish connection to PostgreSQL database using asyncpg."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                host=self.db_config.host,
                port=self.db_config.port
            )
            logger.info("Successfully connected to PostgreSQL database")
            return self.db_pool
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    async def connect_to_redis(self) -> redis.Redis:
        """Establish connection to Redis using redis.asyncio."""
        try:
            pool = redis.ConnectionPool(
                host=self.redis_config.host,
                port=self.redis_config.port,
                db=self.redis_config.db,
                socket_keepalive=True,
                socket_timeout=10,
                client_name=self.redis_config.client_name
            )
            self.redis_conn = redis.Redis(connection_pool=pool, decode_responses=True)
            logger.info("Successfully connected to Redis")
            return self.redis_conn
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise

    async def get_latest_records(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Fetch the latest record for each ticker from PostgreSQL (gexray3 table)."""
        try:
            async with self.db_pool.acquire() as conn:
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

    async def check_new_records(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Check for new records with later timestamps for each ticker every 20 seconds."""
        while True:
            try:
                async with self.db_pool.acquire() as conn:
                    for ticker, last_timestamp in list(self._last_timestamps.items()):
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
                            self._last_timestamps[ticker] = row['timestamp']
                            yield row
                await asyncio.sleep(SLEEP_TIME)  # Check every SLEEP_TIME seconds
            except Exception as e:
                logger.error(f"Error checking new records: {e}")
                await asyncio.sleep(SLEEP_TIME)  # Wait before retrying

    async def publish_record(self, row: Dict[str, Any], is_initial: bool = False) -> None:
        """
        Publish a record to Redis Pub/Sub.

        Args:
            row: Database row to publish
            is_initial: Whether this is an initial record fetch
        """
        try:
            ticker = row['ticker']

            # Format data for Redis
            data = {
                "msg_type": "gex3",
                "data": {
                    "timestamp": row['timestamp'].isoformat(),
                    "ticker": ticker,
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
            await self.redis_conn.publish(GEX_CHANNEL, message_data)

            record_type = "initial" if is_initial else "new"
            logger.info(f"Published {record_type} data for {ticker} to Redis channel {GEX_CHANNEL}")
        except Exception as e:
            logger.error(f"Error publishing to Redis: {e}")

    async def fetch_and_publish_data(self) -> None:
        """
        Fetch and stream data from PostgreSQL gexray3 table, publishing to Redis Pub/Sub.

        This method:
        1. Connects to database and Redis
        2. Fetches initial data
        3. Continuously checks for new data
        """
        try:
            # Connect to services
            await self.connect_to_database()
            await self.connect_to_redis()

            # Initialize last_timestamps dictionary to track the latest timestamp for each ticker
            self._last_timestamps = {}

            # Get and publish initial latest records for each ticker
            logger.info("Starting to fetch and publish initial latest records from gexray3")
            async for row in self.get_latest_records():
                ticker = row['ticker']
                self._last_timestamps[ticker] = row['timestamp']
                await self.publish_record(row, is_initial=True)

            # Continuously check for new records
            logger.info(f"Starting to check for new records every {SLEEP_TIME} seconds")
            async for row in self.check_new_records():
                await self.publish_record(row)

        except Exception as e:
            logger.error(f"Error in fetch_and_publish_data: {e}")
        finally:
            # Clean up resources
            if self.db_pool:
                await self.db_pool.close()
            if self.redis_conn:
                await self.redis_conn.aclose()


async def main() -> None:
    """Main function to run the script."""
    try:
        reader = GexrayReader3()
        await reader.fetch_and_publish_data()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Main error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
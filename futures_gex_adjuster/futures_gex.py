import asyncio
import asyncpg
import json
import logging
import os
from datetime import date, datetime
from dataclasses import dataclass
import redis.asyncio as redis
from typing import Dict, Any, AsyncGenerator, Optional
from dotenv import load_dotenv

load_dotenv()

# Configure logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Module Attributes
REDIS_CLIENT_NAME = "gexrayreader2"
REDIS_PUBSUB_CHANNEL = "gex2"
LOOP_SLEEP_TIME = 10


@dataclass
class DBConfig:
    """Database connection configuration."""

    user: str = os.getenv("POSTGRES_USER", "postgres")
    password: str = os.getenv("POSTGRES_PASSWORD", "")
    database: str = "api"
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))


@dataclass
class RedisConfig:
    """Redis connection configuration."""

    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    db: int = int(os.getenv("REDIS_DB", "0"))
    client_name: str = REDIS_CLIENT_NAME


ES_QUERY = """
    SELECT 
        (SELECT price 
         FROM market_data 
         WHERE symbol = 'ESUSD' 
         ORDER BY timestamp DESC 
         LIMIT 1) -
        (SELECT price 
         FROM market_data 
         WHERE symbol = '^SPX' 
         ORDER BY timestamp DESC 
         LIMIT 1) AS price_difference
"""

NQ_QUERY = """
    SELECT 
        (SELECT price 
         FROM market_data 
         WHERE symbol = 'NQUSD' 
         ORDER BY timestamp DESC 
         LIMIT 1) -
        (SELECT price 
         FROM market_data 
         WHERE symbol = '^NDX' 
         ORDER BY timestamp DESC 
         LIMIT 1) AS price_difference
"""

RTY_QUERY = """
    SELECT 
        (SELECT price 
         FROM market_data 
         WHERE symbol = 'RTYUSD' 
         ORDER BY timestamp DESC 
         LIMIT 1) -
        (SELECT price 
         FROM market_data 
         WHERE symbol = '^RUT' 
         ORDER BY timestamp DESC 
         LIMIT 1) AS price_difference
"""


class FuturesReader:
    """Class to handle fetching and publishing GEX data."""

    def __init__(
        self,
        db_config: Optional[DBConfig] = None,
        redis_config: Optional[RedisConfig] = None,
    ):
        """
        Initialize the FuturesReader.

        Args:
            db_config: Database connection configuration
            redis_config: Redis connection configuration
        """
        self.db_config = db_config or DBConfig()
        self.redis_config = redis_config or RedisConfig()
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_conn: Optional[redis.Redis] = None
        self._last_timestamps: Dict[str, datetime] = {}
        self.symbols = [
            {"cash": "SPX", "futures": "ES", "query": ES_QUERY},
            # {"cash": "NDX", "futures": "NQ", "query": NQ_QUERY},
            # {"cash": "RUT", "futures": "RTY", "query": RTY_QUERY"},
        ]

    async def connect_to_database(self) -> asyncpg.Pool:
        """Establish connection to PostgreSQL database using asyncpg."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                host=self.db_config.host,
                port=self.db_config.port,
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
                client_name=self.redis_config.client_name,
            )
            self.redis_conn = redis.Redis(connection_pool=pool, decode_responses=True)
            logger.info("Successfully connected to Redis")
            return self.redis_conn
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise

    async def get_ratio(self, query: str) -> Optional[float]:
        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch(query)
                if not rows:
                    raise ValueError("No ratio found.")
                price_difference = rows[0]["price_difference"]
                if price_difference is None:
                    raise ValueError("Price difference is NULL.")
                ratio = float(price_difference)
                logger.info(f"Ratio: {ratio}")
                return ratio
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            raise

    async def get_latest_records(
        self, ticker: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Fetch the latest record for each ticker from PostgreSQL."""
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT ticker,
                           time AS timestamp,
                           zero_gamma_avg AS zero_gamma,
                           major_pos_vol_avg AS major_pos_vol,
                           major_neg_vol_avg AS major_neg_vol,
                           minor_pos_vol_avg AS minor_pos_vol,
                           minor_neg_vol_avg AS minor_neg_vol,
                           spot_open AS open,
                           spot_high AS high,
                           spot_low AS low,
                           spot_close AS close,
                           sum_gex_vol_avg AS sum_gex_vol
                    FROM gexray3_minute_agg_view
                    WHERE ticker = $1
                    AND timestamp >= $2
                    ORDER BY timestamp ASC
                """
                today = date.today()
                logger.info(f"Executing query for ticker = {ticker}, date = {today}")
                rows = await conn.fetch(query, ticker, today)
                for row in rows:
                    logger.info(
                        f"Found record for ticker: {row['ticker']}, timestamp: {row['timestamp']}"
                    )
                    yield dict(row)
        except PostgresError as e:
            logger.error(f"Database error fetching records for ticker {ticker}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching records for ticker {ticker}: {e}")
            raise

    async def check_new_records(
        self, ticker, last_timestamp
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Check for new records with later timestamps for each ticker every 5 seconds."""
        while True:
            try:
                async with self.db_pool.acquire() as conn:
                    query = """
                        SELECT ticker,
                               time AS timestamp,
                               zero_gamma_avg AS zero_gamma,
                               major_pos_vol_avg AS major_pos_vol,
                               major_neg_vol_avg AS major_neg_vol,
                               minor_pos_vol_avg AS minor_pos_vol,
                               minor_neg_vol_avg AS minor_neg_vol,
                               spot_open AS open,
                               spot_high AS high,
                               spot_low AS low,
                               spot_close AS close,
                               sum_gex_vol_avg AS sum_gex_vol
                        FROM gexray3_minute_agg_view
                        WHERE ticker = $1
                        AND timestamp >= $2
                        ORDER BY timestamp ASC
                    """
                    logger.debug(
                        f"Checking for new records for ticker {ticker}, last timestamp: {last_timestamp}"
                    )
                    row = await conn.fetchrow(query, ticker, last_timestamp)
                    if row:
                        logger.info(f"Found new record for ticker {ticker}")
                        self._last_timestamps[ticker] = row[
                            "timestamp"
                        ]  # Update last timestamp using column name
                        yield row
                await asyncio.sleep(LOOP_SLEEP_TIME)
            except Exception as e:
                logger.error(f"Error checking new records: {e}")
                await asyncio.sleep(LOOP_SLEEP_TIME)

    async def publish_record(
        self, row: Dict[str, Any]
    ) -> None:  # TODO adjust for gexray3
        """
        Publish a record to Redis Pub/Sub.

        Args:
            row: Database row to publish
        """
        try:
            ticker = row["ticker"]
            data = {
                "msg_type": REDIS_PUBSUB_CHANNEL,  # Add message type to indicate fake data
                "data": {
                    "timestamp": row["timestamp"].isoformat(),
                    "ticker": ticker,
                    "expiration": row["expiration"],
                    "spot": float(row["spot"]),
                    "zero_gamma": float(row["zero_gamma"]),
                    "major_pos_vol": float(row["major_pos_vol"]),
                    "major_neg_vol": float(row["major_neg_vol"]),
                    "sum_gex_vol": float(row["sum_gex_vol"]),
                },
                "trades": {},
                "strikes": {},
            }

            data["event_id"] = f"{REDIS_PUBSUB_CHANNEL}:{datetime.now().isoformat()}"
            message_data = json.dumps(data)
            await self.redis_conn.publish(REDIS_PUBSUB_CHANNEL, message_data)
            logger.info(
                f"Published data for {ticker} to Redis channel {REDIS_PUBSUB_CHANNEL}"
            )
        except Exception as e:
            logger.error(f"Error publishing to Redis: {e}")

    async def fetch_and_publish_data(self) -> None:  # TODO still need to fix
        """Fetch and stream data from PostgreSQL, publishing to Redis Pub/Sub."""
        try:
            # Connect to services
            await self.connect_to_database()
            await self.connect_to_redis()

            # Initialize last_timestamps dictionary to track the latest timestamp for each ticker
            self._last_timestamps = {}

            # Get and publish initial latest records for each ticker
            logger.info("Starting to fetch and publish initial latest records")
            for symbol in self.symbols:
                async for row in self.get_latest_records(
                    symbol["cash"]
                ):  # TODO need to pass ticker
                    # TODO adjust ratios
                    # TODO change ticker
                    futures_ticker = symbol["futures"]
                    self._last_timestamps[symbol["cash"]] = row["timestamp"]
                    await self.publish_record(row)

                # Continuously check for new records every LOOP_SLEEP_TIME seconds
                logger.info(
                    f"Starting to check for new records every {LOOP_SLEEP_TIME} seconds"
                )
                async for row in self.check_new_records():
                    await self.publish_record(row)

        except Exception as e:
            logger.error(f"Error in fetch_and_publish_data: {e}")
        finally:
            # Clean up resources
            if self.db_pool:
                await self.db_pool.close()
            if self.redis_conn:
                await self.redis_conn.close()


async def main() -> None:
    """Main function to run the script."""
    try:
        reader = FuturesReader()
        await reader.fetch_and_publish_data()
    except Exception as e:
        logger.error(f"Main error: {e}")


if __name__ == "__main__":
    asyncio.run(main())

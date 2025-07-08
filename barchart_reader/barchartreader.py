"""Barchart Reader

This script gets GEX data from the PostgreSQL 'gexray3' table and publishes to a Redis pubsub
for the barchart SSE endpoint. It replicates the logic from the /historical-gex/{ticker} API endpoint
but runs continuously and publishes data every 2 seconds.

The script polls the database for each ticker and publishes formatted barchart data to Redis.

This is run in a docker container that is started and stopped with crontab.
When changing the script make sure to build the container.

```
cd /apps/gexreader/barchart_db_to_sse

docker compose stop barchartreader
docker compose build

# Run the container and check the logs for errors.
docker compose up -d
docker logs barchartreader -f

# This will leave the container built and ready to start by cron.
docker stop barchartreader

# Make sure the container is ready to run
docker ps -a
```

Attributes
    REDIS_CLIENT_NAME (str): Gives the connection a name for redis client list.
    REDIS_PUBSUB_CHANNEL (str): This is the redis pubsub channel it publishes to ("barchart").
    LOOP_SLEEP_TIME (int): This is how many seconds between each fetch/publish loop (2 seconds).
    TICKERS (list): List of tickers to monitor for barchart data.

Environment Variables:
    REDIS_HOST (str):
    REDIS_PORT (int):
    REDIS_DB (str):
    POSTGRES_USER (str):
    POSTGRES_PASSWORD (str):
    POSTGRES_DB (str):
    POSTGRES_HOST (str):
    POSTGRES_PORT (int):
"""

import asyncio
import json
import logging
import os
from datetime import date, datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

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
REDIS_CLIENT_NAME = "barchartreader"
BARCHART_CHANNEL = "barchart"
LOOP_SLEEP_TIME = 2  # 2 seconds between polls

# Tickers to monitor (you can expand this list)
TICKERS = [
    "SPXW", "VIX", "RUTW", "VXX", "SPY", "QQQ", "IWM", "DIA", "TQQQ",
    "AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA",
    "TSLA", "AMD", "PLTR", "SMCI", "MSTR", "COIN", "BABA"
]

# Time intervals to look back (in minutes)
INTERVALS = [0, 1, 5, 15]


@dataclass
class DBConfig:
    """Database connection configuration."""
    user: str = os.getenv('POSTGRES_USER', 'postgres')
    password: str = os.getenv('POSTGRES_PASSWORD', '')
    database: str = os.getenv('POSTGRES_DB', None)
    host: str = os.getenv('POSTGRES_HOST', 'localhost')
    port: int = int(os.getenv('POSTGRES_PORT', '5432'))


@dataclass
class RedisConfig:
    """Redis connection configuration."""
    host: str = os.getenv('REDIS_HOST', 'localhost')
    port: int = int(os.getenv('REDIS_PORT', '6379'))
    db: int = int(os.getenv('REDIS_DB', '0'))
    client_name: str = REDIS_CLIENT_NAME


def get_strike_range_multipliers(spot_price: float) -> tuple[float, float]:
    """
    Calculate the strike range multipliers based on spot price.

    Args:
        spot_price: Current spot price of the underlying

    Returns:
        tuple: (lower_multiplier, upper_multiplier) for filtering strikes
    """
    if spot_price > 1000:
        return (0.985, 1.015)
    elif spot_price <=1000 and spot_price > 500:
        return (0.97, 1.03)
    elif spot_price <= 500 and spot_price > 200:
        return (0.95, 1.05)
    elif spot_price <= 200 and spot_price > 100:
        return (0.93, 1.07)
    else:
        return (0.90, 1.10)

# Replace the existing strike filtering logic with this:
def filter_strikes_by_price_range(strikes_array, spot_price):
    """
    Filter strikes based on dynamic price range relative to spot price.
    """
    gex_by_strike = []

    # Get the appropriate range multipliers for this spot price
    lower_mult, upper_mult = get_strike_range_multipliers(spot_price)

    for strike in strikes_array:
        # Skip empty or invalid strike entries
        if not strike or len(strike) < 4:
            continue

        try:
            strike_price = float(strike[0])

            # Filter strikes within the dynamic range
            if (spot_price * lower_mult) <= strike_price <= (spot_price * upper_mult):
                total_gex = float(strike[3])  # 4th element is total GEX

                # Set other values to None since the structure doesn't match the original API
                call_iv = None
                put_iv = None
                exposure = None

                gex_by_strike.append({
                    "strike": strike_price,
                    "gex": total_gex,
                    "call_iv": call_iv,
                    "put_iv": put_iv,
                    "exposure": exposure
                })

        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"Skipping invalid strike data: {strike}, Error: {e}")
            continue

    return gex_by_strike


class BarchartReader:
    """Class to handle fetching and publishing barchart data from the gexray3 table."""

    def __init__(
            self,
            db_config: Optional[DBConfig] = None,
            redis_config: Optional[RedisConfig] = None
    ):
        """
        Initialize the BarchartReader.

        Args:
            db_config: Database connection configuration
            redis_config: Redis connection configuration
        """
        self.db_config = db_config or DBConfig()
        self.redis_config = redis_config or RedisConfig()
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_conn: Optional[redis.Redis] = None

    async def connect_to_database(self) -> asyncpg.Pool:
        """Establish connection to PostgreSQL database using asyncpg."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                host=self.db_config.host,
                port=self.db_config.port,
                min_size=1,
                max_size=10
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

    async def get_barchart_data_for_ticker(self, ticker: str, target_date: date) -> Dict[str, Any]:
        """
        Get barchart data for a specific ticker, replicating the /historical-gex/{ticker} endpoint logic.

        Args:
            ticker: Ticker symbol (e.g., "SPY")
            target_date: Date to fetch data for

        Returns:
            dict: Formatted barchart data for the ticker
        """
        try:
            # Get current UTC time
            current_time = datetime.now(timezone.utc)

            # Process each time interval
            interval_data = {}

            async with self.db_pool.acquire() as conn:
                for interval_min in INTERVALS:
                    # Calculate the target timestamp for this interval
                    if interval_min == 0:
                        interval_label = "current"
                    else:
                        interval_label = f"{interval_min}min_ago"

                    target_time = current_time - timedelta(minutes=interval_min)

                    # SQL query to fetch the most recent GEX data point
                    # Note: Using raw SQL with asyncpg, so we need to handle timezone differently than SQLAlchemy
                    query = """
                        SELECT 
                            timestamp AT TIME ZONE 'UTC' as timestamp, 
                            strikes,
                            spot
                        FROM gexray3
                        WHERE ticker = $1
                          AND DATE(timestamp AT TIME ZONE 'UTC') = $2
                          AND timestamp AT TIME ZONE 'UTC' <= $3
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """

                    # Convert target_time to naive UTC for the query (like SQLAlchemy does)
                    target_time_utc = target_time.replace(tzinfo=None)

                    row = await conn.fetchrow(query, ticker, target_date, target_time_utc)

                    # Process the data if available
                    if row:
                        timestamp, strikes_data, spot_price = row

                        # Process the strikes data and filter by strike window (0.97% to 1.03% of spot)
                        gex_by_strike = []
                        if strikes_data:
                            # Parse strikes_data if it's a JSON string
                            if isinstance(strikes_data, str):
                                try:
                                    strikes_array = json.loads(strikes_data)
                                except json.JSONDecodeError as e:
                                    logger.warning(f"Error parsing strikes JSON for {ticker}: {e}")
                                    strikes_array = []
                            else:
                                strikes_array = strikes_data

                            # Use the new dynamic filtering function
                            gex_by_strike = filter_strikes_by_price_range(strikes_array, spot_price)

                        # Sort by strike price for display
                        sorted_by_strike = sorted(gex_by_strike, key=lambda x: x["strike"] if x["strike"] is not None else 0)

                        # Add to interval data
                        interval_data[interval_label] = {
                            "timestamp": int(timestamp.timestamp() * 1000),  # Milliseconds for JS
                            "strike_data": sorted_by_strike,
                            "spot_price": float(spot_price),
                            "minutes_ago": interval_min
                        }
                    else:
                        # No data found for this interval
                        interval_data[interval_label] = {
                            "timestamp": None,
                            "strike_data": [],
                            "spot_price": None,
                            "minutes_ago": interval_min
                        }

            # Calculate statistics
            statistics = self.calculate_statistics(interval_data)

            # Compile final response
            response = {
                "ticker": ticker,
                "date": str(target_date),
                "intervals": interval_data,
                "statistics": statistics
            }

            return response

        except Exception as e:
            logger.error(f"Error fetching barchart data for ticker {ticker}: {e}")
            return None

    def calculate_statistics(self, interval_data: Dict) -> Dict[str, Any]:
        """Calculate summary statistics across all intervals."""
        stats = {}

        # If we have current data, calculate net, positive, and negative GEX
        if "current" in interval_data and interval_data["current"]["strike_data"]:
            current_data = interval_data["current"]["strike_data"]

            net_gex = sum(item["gex"] for item in current_data)
            positive_gex = sum(item["gex"] for item in current_data if item["gex"] > 0)
            negative_gex = sum(item["gex"] for item in current_data if item["gex"] < 0)

            stats["net_gex"] = net_gex
            stats["positive_gex"] = positive_gex
            stats["negative_gex"] = negative_gex

        # Calculate changes between intervals if we have multiple intervals
        intervals = [k for k in interval_data.keys() if k != "current" and interval_data[k]["strike_data"]]

        if "current" in interval_data and intervals:
            stats["changes"] = {}

            current_strike_map = {item["strike"]: item["gex"] for item in interval_data["current"]["strike_data"]}

            for interval in intervals:
                interval_strike_map = {item["strike"]: item["gex"] for item in interval_data[interval]["strike_data"]}

                # Find common strikes
                common_strikes = set(current_strike_map.keys()) & set(interval_strike_map.keys())

                if common_strikes:
                    # Calculate average change
                    changes = [current_strike_map[strike] - interval_strike_map[strike] for strike in common_strikes]
                    avg_change = sum(changes) / len(changes)

                    # Calculate significant changes (> 10%)
                    significant_changes = []
                    for strike in common_strikes:
                        current_gex = current_strike_map[strike]
                        past_gex = interval_strike_map[strike]

                        if abs(current_gex) > 0 and abs(past_gex) > 0:
                            pct_change = (current_gex - past_gex) / abs(past_gex) * 100

                            if abs(pct_change) >= 10:
                                significant_changes.append({
                                    "strike": strike,
                                    "current_gex": current_gex,
                                    "past_gex": past_gex,
                                    "change": current_gex - past_gex,
                                    "pct_change": pct_change
                                })

                    stats["changes"][interval] = {
                        "avg_change": avg_change,
                        "significant_changes": sorted(significant_changes, key=lambda x: abs(x["pct_change"]), reverse=True)
                    }

        return stats

    async def publish_barchart_data(self, ticker: str, data: Dict[str, Any]) -> None:
        """
        Publish barchart data to Redis Pub/Sub.

        Args:
            ticker: Ticker symbol
            data: Barchart data to publish
        """
        try:
            # Format data for Redis (matching the expected SSE format)
            message = {
                "msg_type": "barchart_data",
                "ticker": ticker,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event_id": f"{BARCHART_CHANNEL}:{ticker}:{datetime.now().isoformat()}"
            }

            message_json = json.dumps(message)

            # Publish to the barchart channel
            await self.redis_conn.publish(BARCHART_CHANNEL, message_json)

            logger.debug(f"Published barchart data for {ticker} to Redis channel {BARCHART_CHANNEL}")

        except Exception as e:
            logger.error(f"Error publishing barchart data for {ticker} to Redis: {e}")

    async def fetch_and_publish_data_loop(self) -> None:
        """
        Continuously fetch and publish barchart data for all tickers.
        """
        try:
            # Connect to services
            await self.connect_to_database()
            await self.connect_to_redis()

            logger.info(f"Starting barchart data loop for {len(TICKERS)} tickers every {LOOP_SLEEP_TIME} seconds")

            while True:
                # Get current date
                target_date = datetime.now(timezone.utc).date()

                # Process each ticker
                for ticker in TICKERS:
                    try:
                        # Get barchart data for this ticker
                        barchart_data = await self.get_barchart_data_for_ticker(ticker, target_date)

                        if barchart_data:
                            # Publish to Redis
                            await self.publish_barchart_data(ticker, barchart_data)
                        else:
                            logger.warning(f"No barchart data found for ticker {ticker}")

                    except Exception as e:
                        logger.error(f"Error processing ticker {ticker}: {e}")
                        continue

                # Wait before next iteration
                await asyncio.sleep(LOOP_SLEEP_TIME)

        except Exception as e:
            logger.error(f"Error in fetch_and_publish_data_loop: {e}")
            raise
        finally:
            # Clean up resources
            if self.db_pool:
                await self.db_pool.close()
            if self.redis_conn:
                await self.redis_conn.aclose()


async def main() -> None:
    """Main function to run the script."""
    try:
        # Validate environment variables
        required_env_vars = ['POSTGRES_DB', 'POSTGRES_HOST', 'REDIS_HOST']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            return

        reader = BarchartReader()
        await reader.fetch_and_publish_data_loop()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Main error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
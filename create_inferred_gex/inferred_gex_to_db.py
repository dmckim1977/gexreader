import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict
import signal

import asyncpg
import redis.asyncio as redis
import numpy as np
import pandas as pd
import py_vollib_vectorized
import pytz
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Configuration
REDIS_SUBSCRIPTION_CHANNEL = "inferred_trades"
REDIS_PUBLISH_CHANNEL = "inferred_gex"
AGGREGATION_INTERVAL = 1.0  # seconds
RISK_FREE_RATE = 0.025
DAYS_IN_YEAR_DTE = 262
NY_TIMEZONE = pytz.timezone("America/New_York")

# Configurable ticker list - process all tickers from inferred_trades by default
TICKER_LIST = ["SPXW", "SPY", "QQQ", "TSLA", "NVDA"]  # Can be set to None to process all tickers


class InferredGexProcessor:
    """Process inferred trades and calculate GEX in real-time."""
    
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.redis_conn: Optional[redis.Redis] = None
        self.trade_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self.running = True
        
    async def connect_to_database(self):
        """Establish connection to PostgreSQL database using asyncpg."""
        try:
            self.db_pool = await asyncpg.create_pool(
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=os.getenv("POSTGRES_DB"),
                host=os.getenv("POSTGRES_HOST"),
                port=5432,
            )
            logger.info("Successfully connected to PostgreSQL database")
            return self.db_pool
        except Exception as e:
            logger.exception(f"Error connecting to PostgreSQL: {e}")
            raise

    async def connect_to_redis(self):
        """Establish connection to Redis."""
        try:
            pool = redis.ConnectionPool(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=int(os.getenv("REDIS_DB", "0")),
                socket_keepalive=True,
                socket_timeout=10,
            )
            self.redis_conn = redis.Redis(connection_pool=pool, decode_responses=True)
            logger.info("Successfully connected to Redis")
            return self.redis_conn
        except Exception as e:
            logger.exception(f"Error connecting to Redis: {e}")
            raise

    async def subscribe_to_inferred_trades(self):
        """Subscribe to the inferred_trades Redis channel."""
        try:
            pubsub = self.redis_conn.pubsub()
            await pubsub.subscribe(REDIS_SUBSCRIPTION_CHANNEL)
            logger.info(f"Subscribed to Redis channel: {REDIS_SUBSCRIPTION_CHANNEL}")
            
            async for message in pubsub.listen():
                if not self.running:
                    break
                    
                if message['type'] == 'message':
                    try:
                        trade_data = json.loads(message['data'])
                        await self.process_trade(trade_data)
                    except Exception as e:
                        logger.exception(f"Error processing trade message: {e}")
                        
        except Exception as e:
            logger.exception(f"Error in Redis subscription: {e}")

    async def process_trade(self, trade_data: Dict):
        """Process individual trade and add to buffer."""
        try:
            ticker = trade_data.get('root')
            
            # Filter by ticker list if specified
            if TICKER_LIST and ticker not in TICKER_LIST:
                return
                
            # Create buffer key by ticker and expiration
            expiration = trade_data.get('expiration')
            buffer_key = f"{ticker}_{expiration}"
            
            # Add trade to buffer
            self.trade_buffer[buffer_key].append(trade_data)
            
        except Exception as e:
            logger.exception(f"Error processing trade: {e}")

    def calculate_dte(self, expiration_str: str) -> float:
        """Calculate days to expiration."""
        try:
            exp_date = datetime.strptime(expiration_str, "%Y-%m-%d")
            ny_now = datetime.now(NY_TIMEZONE)
            ny_now_naive = ny_now.replace(tzinfo=None)
            
            # Set expiration to 4 PM ET
            exp_datetime = exp_date.replace(hour=16, minute=0, second=0, microsecond=0)
            
            dte_seconds = (exp_datetime - ny_now_naive).total_seconds()
            dte_days = max(0, dte_seconds / (24 * 60 * 60))
            
            return dte_days / DAYS_IN_YEAR_DTE
        except Exception as e:
            logger.exception(f"Error calculating DTE for {expiration_str}: {e}")
            return 0.01  # Minimum DTE to avoid division by zero

    def calculate_iv_and_greeks(self, df: pd.DataFrame, dte: float) -> pd.DataFrame:
        """Calculate implied volatility and Greeks for options."""
        try:
            # Calculate midpoint price from trade price (assuming it's fair value)
            df['mid'] = df['price']
            df['flag'] = df['right'].str.lower()
            
            # Calculate IV
            df['iv'] = py_vollib_vectorized.vectorized_implied_volatility(
                df['mid'],
                df['spot'],
                df['strike'],
                dte,
                RISK_FREE_RATE,
                df['flag'],
                q=0,
                model="black_scholes",
                return_as="numpy",
                on_error="ignore",
            )
            
            # Calculate gamma only for valid IV
            valid_mask = df['iv'].notna()
            df.loc[valid_mask, 'gamma'] = py_vollib_vectorized.vectorized_gamma(
                df.loc[valid_mask, 'flag'],
                df.loc[valid_mask, 'spot'],
                df.loc[valid_mask, 'strike'],
                dte,
                RISK_FREE_RATE,
                df.loc[valid_mask, 'iv'],
                model="black_scholes",
                return_as="numpy",
            )
            
            return df
            
        except Exception as e:
            logger.exception(f"Error calculating IV and Greeks: {e}")
            return df

    def calculate_gex_from_trades(self, trades: List[Dict], dte: float) -> Dict[str, Any]:
        """Calculate GEX from aggregated trades."""
        try:
            if not trades:
                return None
                
            # Convert to DataFrame
            df = pd.DataFrame(trades)
            
            # Aggregate trades by strike and right
            agg_df = df.groupby(['strike', 'right', 'spot']).agg({
                'size': 'sum',  # Total volume
                'price': 'mean',  # Average price
                'signed_premium': 'sum'  # Total signed premium
            }).reset_index()
            
            # Calculate IV and Greeks
            df_greeks = self.calculate_iv_and_greeks(agg_df, dte)
            
            # Calculate GEX for each option
            df_greeks['option_gex'] = (
                df_greeks['gamma'] * 
                df_greeks['size'] * 
                df_greeks['spot'] * 
                100
            )
            
            # Apply put multiplier (-1 for puts)
            put_mask = df_greeks['right'] == 'P'
            df_greeks.loc[put_mask, 'option_gex'] *= -1
            
            # Aggregate by strike
            strike_gex = df_greeks.groupby('strike').agg({
                'option_gex': 'sum',
                'size': 'sum'
            }).reset_index()
            
            strike_gex.rename(columns={'option_gex': 'total_gex'}, inplace=True)
            
            # Calculate summary metrics
            spot_price = df['spot'].iloc[0]
            total_gex = strike_gex['total_gex'].sum()
            
            # Find strikes with highest positive and negative GEX
            sorted_gex = strike_gex.sort_values('total_gex', ascending=False)
            
            major_pos_vol = float(sorted_gex.iloc[0]['strike']) if len(sorted_gex) > 0 else 0
            minor_pos_vol = float(sorted_gex.iloc[1]['strike']) if len(sorted_gex) > 1 else 0
            major_neg_vol = float(sorted_gex.iloc[-1]['strike']) if len(sorted_gex) > 0 else 0
            minor_neg_vol = float(sorted_gex.iloc[-2]['strike']) if len(sorted_gex) > 1 else 0
            
            # Calculate zero gamma (simplified approach)
            zero_gamma = self.find_zero_gamma(strike_gex, spot_price)
            
            # Prepare strikes data for JSON storage
            strikes_data = []
            for _, row in strike_gex.iterrows():
                strikes_data.append([
                    float(row['strike']),
                    float(row['total_gex']),
                    int(row['size'])
                ])
            
            return {
                'spot': float(spot_price),
                'zero_gamma': float(zero_gamma),
                'major_pos_vol': major_pos_vol,
                'major_neg_vol': major_neg_vol,
                'sum_gex_vol': float(total_gex),
                'minor_pos_vol': minor_pos_vol,
                'minor_neg_vol': minor_neg_vol,
                'strikes_data': strikes_data,
                'trade_count': len(trades),
                'total_volume': int(df['size'].sum())
            }
            
        except Exception as e:
            logger.exception(f"Error calculating GEX: {e}")
            return None

    def find_zero_gamma(self, strike_gex: pd.DataFrame, spot_price: float) -> float:
        """Find zero gamma point using cumulative GEX."""
        try:
            if len(strike_gex) < 2:
                return spot_price
                
            # Sort by strike and calculate cumulative GEX
            sorted_gex = strike_gex.sort_values('strike')
            sorted_gex['cumulative_gex'] = sorted_gex['total_gex'].cumsum()
            
            strikes = sorted_gex['strike'].values
            cum_gex = sorted_gex['cumulative_gex'].values
            
            # Find zero crossing
            if np.all(cum_gex >= 0) or np.all(cum_gex <= 0):
                return spot_price
                
            zero_cross_idx = np.where(np.diff(np.sign(cum_gex)))[0]
            if len(zero_cross_idx) == 0:
                return spot_price
                
            # Linear interpolation at first zero crossing
            idx = zero_cross_idx[0]
            x1, y1 = strikes[idx], cum_gex[idx]
            x2, y2 = strikes[idx + 1], cum_gex[idx + 1]
            
            if abs(y2 - y1) < 1e-10:
                zero_gamma = (x1 + x2) / 2
            else:
                zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)
                
            return zero_gamma
            
        except Exception as e:
            logger.exception(f"Error finding zero gamma: {e}")
            return spot_price

    async def insert_to_database(self, data: Dict):
        """Insert GEX data into the inferred_gexray3 table."""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO inferred_gexray3 (
                        time, msg_type, ticker, expiration, spot, zero_gamma,
                        major_pos_vol, major_neg_vol, sum_gex_vol, minor_pos_vol,
                        minor_neg_vol, trades, strikes, trade_count, total_volume
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (time, ticker, expiration) DO UPDATE SET
                        spot = EXCLUDED.spot,
                        zero_gamma = EXCLUDED.zero_gamma,
                        major_pos_vol = EXCLUDED.major_pos_vol,
                        major_neg_vol = EXCLUDED.major_neg_vol,
                        sum_gex_vol = EXCLUDED.sum_gex_vol,
                        minor_pos_vol = EXCLUDED.minor_pos_vol,
                        minor_neg_vol = EXCLUDED.minor_neg_vol,
                        trades = EXCLUDED.trades,
                        strikes = EXCLUDED.strikes,
                        trade_count = EXCLUDED.trade_count,
                        total_volume = EXCLUDED.total_volume
                    """,
                    data["timestamp"],
                    data["msg_type"],
                    data["ticker"],
                    data["expiration"],
                    data["spot"],
                    data["zero_gamma"],
                    data["major_pos_vol"],
                    data["major_neg_vol"],
                    data["sum_gex_vol"],
                    data["minor_pos_vol"],
                    data["minor_neg_vol"],
                    json.dumps(data["trades"]),
                    json.dumps(data["strikes"]),
                    data["trade_count"],
                    data["total_volume"]
                )
                logger.info(f"Inserted GEX data for {data['ticker']} expiration {data['expiration']}")
                
        except Exception as e:
            logger.exception(f"Error inserting to database: {e}")

    async def publish_to_redis(self, data: Dict):
        """Publish GEX data to Redis channel."""
        try:
            message = {
                "msg_type": "inferred_gex",
                "ticker": data["ticker"],
                "data": {
                    "timestamp": data["timestamp"].isoformat(),
                    "ticker": data["ticker"],
                    "expiration": data["expiration"],
                    "spot": data["spot"],
                    "zero_gamma": data["zero_gamma"],
                    "major_pos_vol": data["major_pos_vol"],
                    "major_neg_vol": data["major_neg_vol"],
                    "sum_gex_vol": data["sum_gex_vol"],
                    "minor_pos_vol": data["minor_pos_vol"],
                    "minor_neg_vol": data["minor_neg_vol"],
                    "trade_count": data["trade_count"],
                    "total_volume": data["total_volume"]
                },
                "trades": data["trades"],
                "strikes": data["strikes"],
                "event_id": f"{REDIS_PUBLISH_CHANNEL}:{datetime.now().isoformat()}"
            }
            
            await self.redis_conn.publish(REDIS_PUBLISH_CHANNEL, json.dumps(message))
            logger.info(f"Published GEX data for {data['ticker']} to Redis")
            
        except Exception as e:
            logger.exception(f"Error publishing to Redis: {e}")

    async def process_buffered_trades(self):
        """Process buffered trades every second and calculate GEX."""
        while self.running:
            try:
                await asyncio.sleep(AGGREGATION_INTERVAL)
                
                if not self.trade_buffer:
                    continue
                    
                # Process each ticker/expiration combination
                buffer_copy = dict(self.trade_buffer)
                self.trade_buffer.clear()
                
                for buffer_key, trades in buffer_copy.items():
                    if not trades:
                        continue
                        
                    ticker, expiration = buffer_key.split('_', 1)
                    dte = self.calculate_dte(expiration)
                    
                    # Calculate GEX
                    gex_result = self.calculate_gex_from_trades(trades, dte)
                    if not gex_result:
                        continue
                    
                    # Prepare data for storage
                    data = {
                        "timestamp": datetime.now(NY_TIMEZONE),
                        "msg_type": "inferred_gex",
                        "ticker": ticker,
                        "expiration": expiration,
                        "spot": gex_result["spot"],
                        "zero_gamma": gex_result["zero_gamma"],
                        "major_pos_vol": gex_result["major_pos_vol"],
                        "major_neg_vol": gex_result["major_neg_vol"],
                        "sum_gex_vol": gex_result["sum_gex_vol"],
                        "minor_pos_vol": gex_result["minor_pos_vol"],
                        "minor_neg_vol": gex_result["minor_neg_vol"],
                        "trades": trades,  # Store original trade data
                        "strikes": gex_result["strikes_data"],
                        "trade_count": gex_result["trade_count"],
                        "total_volume": gex_result["total_volume"]
                    }
                    
                    # Store in database and publish to Redis
                    await self.insert_to_database(data)
                    await self.publish_to_redis(data)
                    
            except Exception as e:
                logger.exception(f"Error processing buffered trades: {e}")

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down inferred GEX processor...")
        self.running = False
        
        if self.db_pool:
            await self.db_pool.close()
        if self.redis_conn:
            await self.redis_conn.aclose()
        
        logger.info("Shutdown complete")

    async def main(self):
        """Main execution function."""
        try:
            # Setup signal handlers
            loop = asyncio.get_running_loop()
            for sig_name in ("SIGINT", "SIGTERM"):
                loop.add_signal_handler(
                    getattr(signal, sig_name), lambda: asyncio.create_task(self.shutdown())
                )
            
            # Connect to services
            await self.connect_to_database()
            await self.connect_to_redis()
            
            # Start processing tasks
            tasks = [
                asyncio.create_task(self.subscribe_to_inferred_trades()),
                asyncio.create_task(self.process_buffered_trades())
            ]
            
            logger.info("Inferred GEX processor started")
            await asyncio.gather(*tasks)
            
        except asyncio.CancelledError:
            logger.info("Process was cancelled")
        except Exception as e:
            logger.exception(f"Error in main process: {e}")
        finally:
            await self.shutdown()


async def main():
    """Entry point."""
    processor = InferredGexProcessor()
    await processor.main()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.exception(f"Error in main: {e}")
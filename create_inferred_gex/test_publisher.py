#!/usr/bin/env python3
"""
Test publisher for inferred_trades Redis channel.
Publishes sample trade data for testing the inferred GEX processor.
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
import random

import redis.asyncio as redis
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class TestTradePublisher:
    """Publishes test trade data to inferred_trades channel."""
    
    def __init__(self):
        self.redis_conn = None
        
    async def connect_to_redis(self):
        """Connect to Redis."""
        try:
            pool = redis.ConnectionPool(
                host=os.getenv("REDIS_REMOTE", os.getenv("REDIS_HOST", "localhost")),
                port=int(os.getenv("REDIS_PORT", "6379")),
                db=int(os.getenv("REDIS_DB", "0")),
                socket_keepalive=True,
                socket_timeout=5,
            )
            self.redis_conn = redis.Redis(connection_pool=pool, decode_responses=True)
            await self.redis_conn.ping()
            logger.info("Connected to Redis")
            return self.redis_conn
        except Exception as e:
            logger.exception(f"Error connecting to Redis: {e}")
            raise
    
    def generate_sample_trade(self, ticker="SPY", base_spot=455.0):
        """Generate a sample trade message."""
        # Random expiration date (within next 30 days)
        exp_date = datetime.now() + timedelta(days=random.randint(1, 30))
        
        # Generate strike around spot price
        strike_offset = random.uniform(-0.1, 0.1)  # ±10%
        strike = round(base_spot * (1 + strike_offset), 1)
        
        # Random option type
        right = random.choice(["C", "P"])
        
        # Random size (1-500 contracts)
        size = random.randint(1, 500)
        
        # Random price based on option type and strike
        if right == "C":
            # Call price decreases as strike increases above spot
            price = max(0.1, base_spot - strike + random.uniform(0, 10))
        else:
            # Put price increases as strike increases above spot
            price = max(0.1, strike - base_spot + random.uniform(0, 10))
        
        price = round(price, 2)
        
        # Random aggressor
        aggressor = random.choice([-1, 1])
        
        # Calculate signed premium
        signed_premium = price * size * 100 * aggressor
        
        trade = {
            "root": ticker,
            "strike": float(strike),
            "right": right,
            "size": size,
            "price": price,
            "spot": base_spot,
            "aggressor": aggressor,
            "expiration": exp_date.strftime("%Y-%m-%d"),
            "condition": 0,
            "trade_datetime": datetime.now().isoformat() + "Z",
            "signed_premium": signed_premium
        }
        
        return trade
    
    async def publish_test_trades(self, num_trades=100, interval=0.1):
        """Publish a series of test trades."""
        tickers = ["SPY", "QQQ", "TSLA", "NVDA", "SPXW"]
        spot_prices = {
            "SPY": 455.0,
            "QQQ": 380.0,
            "TSLA": 250.0,
            "NVDA": 120.0,
            "SPXW": 4550.0
        }
        
        logger.info(f"Publishing {num_trades} test trades...")
        
        for i in range(num_trades):
            ticker = random.choice(tickers)
            spot = spot_prices[ticker]
            
            trade = self.generate_sample_trade(ticker, spot)
            
            await self.redis_conn.publish(
                "inferred_trades", 
                json.dumps(trade)
            )
            
            if (i + 1) % 10 == 0:
                logger.info(f"Published {i + 1} trades")
            
            await asyncio.sleep(interval)
        
        logger.info(f"Finished publishing {num_trades} test trades")
    
    async def publish_continuous_trades(self, trades_per_second=5):
        """Publish trades continuously."""
        tickers = ["SPY", "QQQ", "TSLA", "NVDA", "SPXW"]
        spot_prices = {
            "SPY": 455.0,
            "QQQ": 380.0,
            "TSLA": 250.0,
            "NVDA": 120.0,
            "SPXW": 4550.0
        }
        
        interval = 1.0 / trades_per_second
        logger.info(f"Publishing trades continuously at {trades_per_second}/sec...")
        
        count = 0
        while True:
            ticker = random.choice(tickers)
            spot = spot_prices[ticker]
            
            trade = self.generate_sample_trade(ticker, spot)
            
            await self.redis_conn.publish(
                "inferred_trades", 
                json.dumps(trade)
            )
            
            count += 1
            if count % 50 == 0:
                logger.info(f"Published {count} trades")
            
            await asyncio.sleep(interval)
    
    async def cleanup(self):
        """Clean up resources."""
        if self.redis_conn:
            await self.redis_conn.aclose()


async def main():
    """Main function."""
    import sys
    
    publisher = TestTradePublisher()
    
    try:
        await publisher.connect_to_redis()
        
        if len(sys.argv) > 1 and sys.argv[1] == "continuous":
            await publisher.publish_continuous_trades()
        else:
            await publisher.publish_test_trades()
            
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.exception(f"Error: {e}")
    finally:
        await publisher.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
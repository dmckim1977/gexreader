import asyncio
import random
import datetime
import json
import redis.asyncio as redis
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

async def connect_to_redis():
    """Establish connection to Redis using redis.asyncio."""
    try:
        redis_conn = await redis.Redis(host='localhost', port=6379, decode_responses=True)
        print("Successfully connected to Redis")
        return redis_conn
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        raise

async def generate_fake_data():
    """Generate fake data with jittered values, include msg_type, and publish to Redis Pub/Sub."""
    redis_conn = await connect_to_redis()

    try:
        # Base values for the data (as per your example)
        base_values = {
            "spot": 6000,
            "zero_gamma": 5980,
            "major_pos_vol": 6100,
            "major_neg_vol": 5970,
            "sum_gex_vol": 10
        }

        while True:
            # Generate timestamp in the specified format (e.g., "2025-02-21T19:19:46+00:00")
            current_time = datetime.datetime.now(datetime.UTC).isoformat() + "+00:00"

            # Jitter the values randomly around ±10 of their base values
            jittered_values = {
                key: value + random.uniform(-10, 10) for key, value in base_values.items()
            }

            # Create the fake data message with msg_type
            fake_data = {
                "msg_type": "gex1",  # Add message type to indicate fake data
                "data": {"timestamp": current_time,
                    "ticker": "SPX",
                    "expiration": "zero",
                    "spot": jittered_values["spot"],
                    "zero_gamma": jittered_values["zero_gamma"],
                    "major_pos_vol": jittered_values["major_pos_vol"],
                    "major_neg_vol": jittered_values["major_neg_vol"],
                    "sum_gex_vol": jittered_values["sum_gex_vol"]
                },
                "trades": {},
                "strikes": {},
            }

            # Publish to Redis Pub/Sub channel 'gex2'
            message_data = json.dumps(fake_data)
            await redis_conn.publish('gex2', message_data)
            print(f"Published fake data at {current_time}: {fake_data}")

            # Wait 10 seconds before sending the next message
            await asyncio.sleep(10)

    except Exception as e:
        print(f"Error in generate_fake_data: {e}")
    finally:
        await redis_conn.aclose()

async def main():
    """Main function to run the fake data generator."""
    try:
        await generate_fake_data()
    except Exception as e:
        print(f"Main error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
import io
import json
import logging
import os
import pytz
import time
import asyncio
import signal
from datetime import datetime, timedelta
import asyncpg
import httpx
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import py_vollib.black_scholes_merton.implied_volatility
import py_vollib_vectorized
import redis.asyncio as redis
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

base_url = os.getenv("THETADATA_URL")


# Make HTTP requests asynchronous
async def get_snapshot(root: str, exp: int) -> pd.DataFrame:
    async with httpx.AsyncClient() as client:
        params = {
            "root": root,
            "exp": exp,
            "rate": "SOFR",
            "use_csv": True
        }
        res = await client.get(f"{base_url}/v2/bulk_snapshot/option/greeks", params=params)
        try:
            next_page = res.headers.get('next-page', 'null')
        except Exception as e:
            print(f'####### Error getting next page: {res.headers} Error: {e}')
        if next_page != 'null':
            logger.error('Error: next page exists but not implemented')
        else:
            df = pd.read_csv(io.StringIO(res.text))
            filt = df[['root', 'strike', 'right', 'bid', 'ask', 'implied_vol', 'iv_error', 'underlying_price',
                       'delta', 'theta', 'vega']].copy()
            return filt


async def get_ohlc(root: str, exp: int) -> pd.DataFrame:
    async with httpx.AsyncClient() as client:
        params = {
            "root": root,
            "exp": exp,
            "use_csv": True
        }
        res = await client.get(f"{base_url}/v2/bulk_snapshot/option/ohlc", params=params)
        next_page = res.headers.get('next-page', 'null')
        if next_page != 'null':
            logger.error('Error: next page exists but not implemented')
        else:
            df = pd.read_csv(io.StringIO(res.text))
            filt = df[['root', 'strike', 'right', 'volume']].copy()
            return filt


def calculate_iv(dataframe: pd.DataFrame, dte: float, risk_free_rate: float = 0.05) -> pd.DataFrame:
    # Create a copy to avoid modifying the original
    df = dataframe.copy()

    # Calculate midpoint price
    df['mid'] = (df['bid'] + df['ask']) / 2

    # Convert option type to lowercase for py_vollib
    df['flag'] = df['right'].str.lower()

    # IMPORTANT: Scale the strike price to match the underlying price scale
    # It appears strikes are multiplied by 10000, so we need to divide
    df['normalized_strike'] = df['strike'] / 1000

    # Handle zero or very small prices
    # For deep OTM options with very small prices, adjust to a minimum value
    min_price = 0.01
    df['adjusted_mid'] = df['mid'].apply(lambda x: max(x, min_price))

    # Calculate IV with error handling
    try:
        df['iv'] = py_vollib_vectorized.vectorized_implied_volatility(
            df['adjusted_mid'], df['underlying_price'], df['normalized_strike'],
            dte, risk_free_rate, df['flag'],
            q=0, model='black_scholes', return_as='numpy', on_error='ignore')
    except Exception as e:
        logger.error(f"IV calculation error: {e}")
        df['iv'] = float('nan')

    return df


def calculate_greeks(dataframe: pd.DataFrame, dte: float, risk_free_rate: float = 0.05) -> pd.DataFrame:
    df = dataframe.copy()

    # Skip rows with NaN IV values
    valid_mask = df['iv'].notna()

    if valid_mask.any():
        try:
            df.loc[valid_mask, 'gamma'] = py_vollib_vectorized.vectorized_gamma(
                df.loc[valid_mask, 'flag'],
                df.loc[valid_mask, 'underlying_price'],
                df.loc[valid_mask, 'normalized_strike'],
                dte, risk_free_rate,
                df.loc[valid_mask, 'iv'],
                model='black_scholes', return_as='numpy')
        except Exception as e:
            logger.error(f"Gamma calculation error: {e}")
            df['gamma'] = float('nan')
    else:
        df['gamma'] = float('nan')

    return df


def calculate_gex(dataframe, volume_column='volume'):
    """
    Calculate Gamma Exposure (GEX) for options and summarize by strike.
    Assumes only one option per strike/right combination.

    Parameters:
    dataframe (pd.DataFrame): DataFrame containing option data with columns:
                             'right', 'gamma', 'underlying_price', and volume column
    volume_column (str): Name of the column containing volume data

    Returns:
    pd.DataFrame: Summarized GEX by strike with call_gex, put_gex, and total_gex columns
    """
    # Create a copy to avoid modifying the original
    df = dataframe.copy()

    # Calculate GEX for each row
    df['option_gex'] = (
            df['gamma'] *
            100 *
            df[volume_column] *
            df['underlying_price']
    )

    # Multiply put GEX by -1
    puts_mask = df['right'] == 'P'
    df.loc[puts_mask, 'option_gex'] *= -1

    # Create separate dataframes for calls and puts
    calls_df = df[df['right'] == 'C'][['strike', 'option_gex']].rename(columns={'option_gex': 'call_gex'})
    puts_df = df[df['right'] == 'P'][['strike', 'option_gex']].rename(columns={'option_gex': 'put_gex'})

    # Merge call and put GEX by strike
    gex_summary = pd.merge(
        calls_df,
        puts_df,
        on='strike',
        how='outer'
    ).fillna(0)

    # Calculate total GEX
    gex_summary['total_gex'] = gex_summary['call_gex'] + gex_summary['put_gex']

    return gex_summary


def calculate_gamma_at_levels(dataframe, levels, dte, risk_free_rate=0.05):
    """
    Calculate gamma at different price levels using py_vollib_vectorized.

    Parameters:
    dataframe (pd.DataFrame): DataFrame with option data (must have necessary columns)
    levels (np.array): Array of price levels to evaluate
    dte (float): Days to expiration (annualized)
    risk_free_rate (float): Risk-free interest rate

    Returns:
    pd.DataFrame: DataFrame with gamma values at each price level
    """
    # Create a copy to avoid modifying the original
    df = dataframe.copy()

    # Filter out invalid rows (no IV)
    valid_mask = df['iv'].notna()
    df_valid = df[valid_mask].copy()

    if len(df_valid) == 0:
        logger.warning("Warning: No valid IV values found in dataframe")
        return None

    # Initialize a DataFrame to store results
    results = []

    # For each price level, calculate gamma
    for level in levels:
        # Create a new dataframe for this price level
        level_df = df_valid.copy()

        # Update underlying price to the current level
        level_df['underlying_price'] = level

        # Calculate gamma at this price level using py_vollib_vectorized
        try:
            level_df['gamma'] = py_vollib_vectorized.vectorized_gamma(
                level_df['flag'],
                level_df['underlying_price'],
                level_df['normalized_strike'],
                dte, risk_free_rate,
                level_df['iv'],
                model='black_scholes', return_as='numpy')

            # Calculate GEX for calls and puts
            level_df['option_gex'] = level_df['gamma'] * level_df['volume'] * level * 100

            # Negate GEX for puts (standard convention for GEX)
            put_mask = level_df['right'] == 'P'
            level_df.loc[put_mask, 'option_gex'] *= -1

            # Sum GEX for this level
            call_gex = level_df[level_df['right'] == 'C']['option_gex'].sum()
            put_gex = level_df[level_df['right'] == 'P']['option_gex'].sum()
            total_gex = call_gex + put_gex

            # Store the result
            results.append({
                'level': level,
                'call_gex': call_gex,
                'put_gex': put_gex,
                'total_gex': total_gex
            })

        except Exception as e:
            logger.error(f"Error calculating gamma at level {level}: {e}")
            results.append({
                'level': level,
                'call_gex': 0,
                'put_gex': 0,
                'total_gex': 0
            })

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)

    return results_df


def find_zero_gamma(gamma_df):
    """
    Find the zero gamma (gamma flip) point from the gamma profile.

    Parameters:
    gamma_df (pd.DataFrame): DataFrame with level and total_gex columns

    Returns:
    float or None: Price level where gamma flips from negative to positive, or None if not found
    """
    if gamma_df is None or len(gamma_df) < 2:
        return None

    # Get arrays for calculation
    levels = gamma_df['level'].values
    gex = gamma_df['total_gex'].values

    # Check if all values are of the same sign
    if np.all(gex >= 0) or np.all(gex <= 0):
        logger.warning("Warning: No zero crossing found in gamma profile")
        return None

    # Find zero crossings
    zero_cross_idx = np.where(np.diff(np.sign(gex)))[0]

    if len(zero_cross_idx) == 0:
        return None

    # Get values on either side of the crossing
    idx = zero_cross_idx[0]
    x1, y1 = levels[idx], gex[idx]
    x2, y2 = levels[idx + 1], gex[idx + 1]

    # Linear interpolation to find zero crossing
    zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)

    return zero_gamma


def calculate_gamma_profile(dataframe, dte, risk_free_rate=0.05,
                            strike_range=0.05, num_levels=100,
                            title="Gamma Exposure Profile"):
    """
    Calculate gamma profile and plot it.

    Parameters:
    dataframe (pd.DataFrame): DataFrame with option data
    dte (float): Days to expiration (annualized)
    risk_free_rate (float): Risk-free interest rate
    strike_range (float): Range around current price to analyze (as percentage)
    num_levels (int): Number of price levels to evaluate
    title (str): Title for the plot

    Returns:
    tuple: (figure, zero_gamma_point)
    """
    # Get current underlying price
    underlying_price = dataframe['underlying_price'].iloc[0]

    # Generate price levels
    min_price = underlying_price * (1 - strike_range)
    max_price = underlying_price * (1 + strike_range)
    levels = np.linspace(min_price, max_price, num_levels)

    # Calculate gamma at each level
    gamma_df = calculate_gamma_at_levels(dataframe, levels, dte, risk_free_rate)

    if gamma_df is None:
        logger.error("Failed to calculate gamma profile")
        return None, None, None, None

    # Find zero gamma point
    zero_gamma = find_zero_gamma(gamma_df)

    return zero_gamma, underlying_price, min_price, max_price


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


async def insert_to_database(pool, data):
    """Insert data into PostgreSQL database."""
    try:
        async with pool.acquire() as conn:

            # Insert the data
            await conn.execute('''
                               INSERT INTO gexray3 (msg_type, timestamp, ticker, expiration, spot,
                                                              zero_gamma, major_pos_vol, major_neg_vol, sum_gex_vol,
                                                              minor_pos_vol, minor_neg_vol, trades, strikes)
                               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (ticker, expiration, timestamp) 
                DO NOTHING
                               ''',
                               data["msg_type"],
                               datetime.now(),
                               data["data"]["ticker"],
                               data["data"]["expiration"],
                               data["data"]["spot"],
                               data["data"]["zero_gamma"],
                               data["data"]["major_pos_vol"],
                               data["data"]["major_neg_vol"],
                               data["data"]["sum_gex_vol"],
                               data["data"]["minor_pos_vol"],
                               data["data"]["minor_neg_vol"],
                               json.dumps(data["trades"]),
                               json.dumps(data["strikes"])
                               )
            logger.info(f"Inserted data for {data['data']['ticker']} into database")
    except Exception as e:
        logger.error(f"Error inserting into database: {e}")
        raise


async def run(
        ticker: str,
        risk_free_rate: float,
        t: float,
        exp_to_use: datetime,
        pool,
        redis_conn = None,
):
    try:
        # Get option data
        df = await get_snapshot(ticker, int(exp_to_use.strftime("%Y%m%d")))
        if df is None or df.empty:
            logger.error(f"Failed to get snapshot data for {ticker}")
            return

        # Calculate IVs
        df_iv = calculate_iv(df, t, risk_free_rate)

        # Calculate Greeks
        df_greeks = calculate_greeks(df_iv, t, risk_free_rate)

        # Get volume data
        volume = await get_ohlc(ticker, int(exp_to_use.strftime("%Y%m%d")))
        if volume is None or volume.empty:
            logger.error(f"Failed to get OHLC data for {ticker}")
            return

        # Merge data
        merged = df_greeks.merge(volume, on=['root', 'right', 'strike'], how='left')
        merged.fillna(value=0, axis=1, inplace=True)

        # Calculate GEX
        gex = calculate_gex(merged)
        sorted_gex = gex.sort_values(by="total_gex", ascending=False)
        sorted_gex['strike'] = sorted_gex['strike'] / 1000
        sorted_gex.to_csv('sorted_gex.csv')

        # Get current NY time
        ny_timezone = pytz.timezone('America/New_York')
        ny_now = datetime.now(ny_timezone)

        # Calculate and plot gamma profile
        title = f"{ticker} Gamma Exposure Profile, {ny_now.strftime('%d %b %Y')}"
        zero_gamma, underlying_price, min_price, max_price = calculate_gamma_profile(
            merged,
            dte=t,
            risk_free_rate=risk_free_rate,
            strike_range=0.05,  # ±5% from current price
            title=title
        )

        if zero_gamma is None or underlying_price is None:
            logger.error(f"Failed to calculate gamma profile for {ticker}")
            return

        # Filter strikes within range
        strikes = sorted_gex[(sorted_gex['strike'] > min_price) & (sorted_gex['strike'] < max_price)]
        strikes_list = strikes.values.tolist()

        # Standardize ticker
        root = "SPX" if ticker == "SPXW" else ticker
        root = "VIX" if ticker == "VIXW" else ticker

        # Prepare data for insertion
        data = {
            "msg_type": "gex3",
            "data": {
                "timestamp": ny_now.isoformat(),
                "ticker": root,
                "expiration": "zero",
                "spot": float(underlying_price),
                "zero_gamma": float(zero_gamma),
                "major_pos_vol": float(sorted_gex['strike'].iloc[0]) if not sorted_gex.empty else 0,
                "major_neg_vol": float(sorted_gex['strike'].iloc[-1]) if not sorted_gex.empty else 0,
                "sum_gex_vol": float(gex['total_gex'].sum()) if not gex.empty else 0,
                "minor_pos_vol": float(sorted_gex['strike'].iloc[1]) if len(sorted_gex) > 1 else 0,
                "minor_neg_vol": float(sorted_gex['strike'].iloc[-2]) if len(sorted_gex) > 1 else 0,
            },
            "trades": [],
            "strikes": strikes_list,
        }

        # Save to database
        await insert_to_database(pool, data)

        # # Publish to Redis
        # if redis_conn is not None:
        #     await publish_to_redis(redis_conn, data, ticker)

        logger.info(f"Successfully processed data for {ticker}")

        return data

    except Exception as e:
        logger.error(f"Error in run function: {e}")
        return None


async def main():
    try:
        # Setup initial connections
        pool = await connect_to_database()

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig_name in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, sig_name),
                lambda: asyncio.create_task(shutdown(pool))
            )

        # Register signal handlers for graceful shutdown - FIXED THIS PART
        loop = asyncio.get_running_loop()
        for sig_name in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, sig_name),  # Use signal module directly
                lambda: asyncio.create_task(shutdown(pool))
            )

        # Get current NY time
        ny_timezone = pytz.timezone('America/New_York')
        ny_now = datetime.now(ny_timezone)
        expo_to_use = ny_now

        # Calculate time until next market close (4:00 PM)
        next_close = expo_to_use.replace(hour=16, minute=0, second=0, microsecond=0)
        if ny_now > next_close:
            # If we're past closing time, use next business day
            next_close += timedelta(days=1)
            # Simple business day check (not accounting for holidays)
            if next_close.weekday() >= 5:  # Saturday (5) or Sunday (6)
                next_close += timedelta(days=7 - next_close.weekday())

        dte_days = (next_close - expo_to_use).total_seconds() / (24 * 60 * 60)
        time_to_expiration = dte_days / 262  # Annualized based on ~262 trading days per year

        logger.info(f'Time to expiration: {time_to_expiration:.6f} years ({dte_days:.2f} days)')

        rfr = 0.05  # Risk-free rate
        # ticker = "SPXW"  # Default ticker
        ticker_list = ['SPXW', 'QQQ', 'SPY', 'IWM']

        # Run continuously until stopped
        while True:
            start_time = time.time()

            # Update time to expiration based on current time
            ny_now = datetime.now(ny_timezone)
            dte_days = (next_close - ny_now).total_seconds() / (24 * 60 * 60)
            if dte_days < 0:
                # We've passed the market close, recalculate for next day
                expo_to_use = ny_now
                next_close = expo_to_use.replace(hour=16, minute=0, second=0, microsecond=0)
                if ny_now > next_close:
                    next_close += timedelta(days=1)
                    if next_close.weekday() >= 5:
                        next_close += timedelta(days=7 - next_close.weekday())
                dte_days = (next_close - expo_to_use).total_seconds() / (24 * 60 * 60)

            time_to_expiration = max(0, dte_days) / 262  # Ensure non-negative

            for ticker in ticker_list:
                logger.info(f"Starting data collection cycle for {ticker}")
                data = await run(
                    ticker=ticker,
                    risk_free_rate=rfr,
                    t=time_to_expiration,
                    exp_to_use=expo_to_use,
                    pool=pool,
                )

            # Calculate processing time and adjust sleep
            processing_time = time.time() - start_time
            sleep_time = max(0, 5 - processing_time)  # Target 5-second cycle
            logger.info(f"Cycle completed in {processing_time:.2f}s, sleeping for {sleep_time:.2f}s")

            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("Main task was cancelled, shutting down")
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
    finally:
        # Ensure connections are closed
        if 'pool' in locals():
            await pool.close()
        # if 'redis_conn' in locals():
        #     await redis_conn.aclose()


async def shutdown(pool):
    """Handle graceful shutdown."""
    logger.info("Shutdown initiated, closing connections...")

    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)

    # Close connections
    if pool:
        await pool.close()
    # if redis_conn:
    #     await redis_conn.aclose()

    logger.info("Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
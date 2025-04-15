import io
import json
import logging
import os
from typing import Optional

import pytz
import time
import asyncio
import signal
from datetime import datetime, timedelta
import asyncpg
import httpx
import numpy as np
import pandas as pd
import py_vollib_vectorized
from dotenv import load_dotenv
from pandas_market_calendars import get_calendar

# Configure logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

load_dotenv()

base_url = os.getenv("THETADATA_URL")

TICKER_LIST: list = ['AAPL', 'MSFT', 'META', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'VXX', 'PLTR', 'COIN', 'AMD', 'BABA', 'MSTR', 'SMCI', 'TQQQ']
EXPIRATION_TYPE: str = 'friday'  # options 'friday', or 'zero'
SLEEP_TIME: int = 15
RISK_FREE_RATE: float = 0.025
STRIKE_RANGE: float = 0.1
STRIKE_LEVELS: int = 50
DAYS_IN_YEAR_DTE: int = 262
NY_TIMEZONE = pytz.timezone('America/New_York')
EXPIRATION_DATETIME: Optional[str] = None
EXPIRATION_INT: Optional[int] = None


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


from pandas_market_calendars import get_calendar
from datetime import datetime, timedelta
import pytz

def get_next_options_expiration(current_date: datetime, ticker: str = None) -> datetime:
    nyse = get_calendar('NYSE')
    NY_TIMEZONE = pytz.timezone('America/New_York')

    if current_date.tzinfo is None:
        current_date = NY_TIMEZONE.localize(current_date)

    start_date = current_date.date()
    end_date = (current_date + timedelta(days=14)).date()
    schedule = nyse.schedule(start_date=start_date, end_date=end_date)

    target_weekday = 2 if ticker in ['VIX', 'VIXW'] else 4
    days_ahead = target_weekday - current_date.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    next_target_day = current_date + timedelta(days=days_ahead)
    next_target_date = next_target_day.date()

    # Default closing time
    close_hour = 16  # 4:00 PM

    # Check for early close
    if next_target_date in schedule.index:
        market_close = schedule.loc[next_target_date, 'market_close']
        close_hour = market_close.hour
        close_minute = market_close.minute
    else:
        # Find previous trading day
        valid_days = schedule.index
        next_target_date = valid_days[valid_days < next_target_date][-1]
        market_close = schedule.loc[next_target_date, 'market_close']
        close_hour = market_close.hour
        close_minute = market_close.minute

    expiration = NY_TIMEZONE.localize(
        datetime.combine(next_target_date, datetime.min.time())
    ).replace(hour=close_hour, minute=close_minute, second=0, microsecond=0)

    logger.info(f"Selected expiration for {ticker}: {expiration}")
    return expiration


async def configure():
    global EXPIRATION_INT
    global EXPIRATION_DATETIME

    # Setup initial connections
    pool = await connect_to_database()

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig_name in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, sig_name),
            lambda: asyncio.create_task(shutdown(pool))
        )

    ny_now = datetime.now(NY_TIMEZONE)

    # Check if today is a holiday and skip if it is
    nyse = get_calendar('NYSE')
    holidays = nyse.holidays().holidays
    if ny_now.date() in holidays:
        logger.info(f"Today ({ny_now.date()}) is a market holiday. Skipping execution.")
        await shutdown(pool)
        return

    # Get next options expiration
    try:
        if EXPIRATION_TYPE == 'friday':
            next_expiration = get_next_options_expiration(ny_now)
            logger.info(f"Next expiration: {next_expiration}")
        elif EXPIRATION_TYPE == 'zero':
            next_expiration = ny_now
            logger.info(f"Next expiration: {next_expiration}")
        else:
            raise ValueError(f"Unsupported EXPIRATION_TYPE: {EXPIRATION_TYPE}")
    except Exception as e:
        logger.error(f"Error calculating expiration: {e}")
        raise

    # Set expiration variables
    EXPIRATION_DATETIME = next_expiration
    try:
        EXPIRATION_INT = int(next_expiration.strftime("%Y%m%d"))
    except ValueError as e:
        logger.error(f"Error converting expiration to int: {e}")
        raise

    return next_expiration, ny_now, pool, loop


async def get_ohlc(root: str, exp: int = EXPIRATION_INT) -> pd.DataFrame:
    logger.info(f"Fetching OHLC for {root} with expiration {exp}")
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
            df['strike'] = df['strike'] / 1000
            filt = df[['root', 'strike', 'right', 'volume']].copy()
            return filt


async def get_snapshot(root: str, exp: int = EXPIRATION_INT) -> pd.DataFrame:
    logger.info(f"Fetching snapshot for {root} with expiration {exp}")
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
            df['strike'] = df['strike'] / 1000
            filt = df[['root', 'strike', 'right', 'bid', 'ask', 'implied_vol', 'iv_error', 'underlying_price',
                       'delta', 'theta', 'vega']].copy()
            return filt


def update_dte(next_close):
    ny_timezone = pytz.timezone('America/New_York')
    # Update time to expiration based on current time
    ny_now = datetime.now(ny_timezone)
    dte_days = (next_close - ny_now).total_seconds() / (24 * 60 * 60)

    return max(0, dte_days) / DAYS_IN_YEAR_DTE  # Ensure non-negative


def calculate_iv(dataframe: pd.DataFrame, dte: float, risk_free_rate: float = 0.05) -> pd.DataFrame:
    # Create a copy to avoid modifying the original
    df = dataframe.copy()

    # Calculate midpoint price
    df['mid'] = (df['bid'] + df['ask']) / 2

    # Convert option type to lowercase for py_vollib
    df['flag'] = df['right'].str.lower()

    # Calculate IV with error handling
    try:
        df['iv'] = py_vollib_vectorized.vectorized_implied_volatility(
            df['mid'], df['underlying_price'], df['strike'],
            dte, risk_free_rate, df['flag'],
            q=0, model='black_scholes', return_as='numpy', on_error='ignore')
    except Exception as e:
        logger.error(f"IV calculation error: {e}")
        df['iv'] = float('nan')

    return df


def calculate_greeks(dataframe: pd.DataFrame, dte: float, risk_free_rate: float = RISK_FREE_RATE) -> pd.DataFrame:
    df = dataframe.copy()

    # Skip rows with NaN IV values
    valid_mask = df['iv'].notna()

    if valid_mask.any():
        try:
            df.loc[valid_mask, 'gamma'] = py_vollib_vectorized.vectorized_gamma(
                df.loc[valid_mask, 'flag'],
                df.loc[valid_mask, 'underlying_price'],
                df.loc[valid_mask, 'strike'],
                dte, risk_free_rate,
                df.loc[valid_mask, 'iv'],
                model='black_scholes', return_as='numpy')
        except Exception as e:
            logger.error(f"Gamma calculation error: {e}")
            df['gamma'] = float('nan')
    else:
        df['gamma'] = float('nan')

    return df


def calculate_gex(dataframe):
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
            df['volume'] *
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


def find_zero_gamma_from_gex(gex_df, underlying_price):
    """
    Find the zero gamma point by calculating the cumulative GEX from the GEX by strike data.

    Parameters:
    gex_df (pd.DataFrame): DataFrame with 'strike' and 'total_gex' columns (output of calculate_gex)
    underlying_price (float): Current spot price of the underlying

    Returns:
    float: The strike price where cumulative GEX crosses zero (zero gamma point)
    """
    if gex_df is None or len(gex_df) < 2:
        logger.warning("Not enough data to calculate zero gamma")
        return underlying_price

    # Sort by strike
    gex_df = gex_df.sort_values('strike').copy()

    # Calculate cumulative GEX
    gex_df['cumulative_gex'] = gex_df['total_gex'].cumsum()

    # Extract strikes and cumulative GEX
    strikes = gex_df['strike'].values
    cumulative_gex = gex_df['cumulative_gex'].values

    # Check if there's a zero crossing
    if np.all(cumulative_gex >= 0) or np.all(cumulative_gex <= 0):
        logger.warning("No zero crossing found in cumulative GEX")
        return underlying_price

    # Find where cumulative GEX crosses zero
    zero_cross_idx = np.where(np.diff(np.sign(cumulative_gex)))[0]
    if len(zero_cross_idx) == 0:
        logger.warning("No zero crossing found in cumulative GEX")
        return underlying_price

    # Take the crossing closest to the underlying price
    idx = zero_cross_idx[np.argmin(np.abs(strikes[zero_cross_idx] - underlying_price))]
    x1, y1 = strikes[idx], cumulative_gex[idx]
    x2, y2 = strikes[idx + 1], cumulative_gex[idx + 1]

    # Linear interpolation to find the exact zero crossing
    if abs(y2 - y1) < 1e-10:  # Avoid division by near-zero
        zero_gamma = (x1 + x2) / 2
    else:
        zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)

    # Ensure the result is within bounds
    if not (min(x1, x2) <= zero_gamma <= max(x1, x2)):
        logger.warning(f"Interpolated zero gamma {zero_gamma} outside bounds [{x1}, {x2}]")
        zero_gamma = (x1 + x2) / 2

    return zero_gamma


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
                level_df['strike'],
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


def calculate_gamma_profile(dataframe, dte, risk_free_rate=RISK_FREE_RATE,
                            strike_range=STRIKE_RANGE, num_levels=100):
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
    min_price = underlying_price * (1 - STRIKE_RANGE)
    max_price = underlying_price * (1 + STRIKE_RANGE)
    levels = np.linspace(min_price, max_price, num_levels)

    # Calculate gamma at each level
    gamma_df = calculate_gamma_at_levels(dataframe, levels, dte, risk_free_rate)

    if gamma_df is None:
        logger.error("Failed to calculate gamma profile")
        return None, None, None, None

    # Find zero gamma point
    zero_gamma = find_zero_gamma(gamma_df)

    return zero_gamma, underlying_price, min_price, max_price


def find_zero_gamma_from_gex(gex_df, underlying_price):
    """
    Find the zero gamma point by calculating the cumulative GEX from the GEX by strike data.

    Parameters:
    gex_df (pd.DataFrame): DataFrame with 'strike' and 'total_gex' columns (output of calculate_gex)
    underlying_price (float): Current spot price of the underlying

    Returns:
    float: The strike price where cumulative GEX crosses zero (zero gamma point)
    """
    if gex_df is None or len(gex_df) < 2:
        logger.warning("Not enough data to calculate zero gamma")
        return underlying_price

    # Sort by strike
    gex_df = gex_df.sort_values('strike').copy()

    # Calculate cumulative GEX
    gex_df['cumulative_gex'] = gex_df['total_gex'].cumsum()

    # Extract strikes and cumulative GEX
    strikes = gex_df['strike'].values
    cumulative_gex = gex_df['cumulative_gex'].values

    # Check if there's a zero crossing
    if np.all(cumulative_gex >= 0) or np.all(cumulative_gex <= 0):
        logger.warning("No zero crossing found in cumulative GEX")
        return underlying_price

    # Find where cumulative GEX crosses zero
    zero_cross_idx = np.where(np.diff(np.sign(cumulative_gex)))[0]
    if len(zero_cross_idx) == 0:
        logger.warning("No zero crossing found in cumulative GEX")
        return underlying_price

    # Take the crossing closest to the underlying price
    idx = zero_cross_idx[np.argmin(np.abs(strikes[zero_cross_idx] - underlying_price))]
    x1, y1 = strikes[idx], cumulative_gex[idx]
    x2, y2 = strikes[idx + 1], cumulative_gex[idx + 1]

    # Linear interpolation to find the exact zero crossing
    if abs(y2 - y1) < 1e-10:  # Avoid division by near-zero
        zero_gamma = (x1 + x2) / 2
    else:
        zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)

    # Ensure the result is within bounds
    if not (min(x1, x2) <= zero_gamma <= max(x1, x2)):
        logger.warning(f"Interpolated zero gamma {zero_gamma} outside bounds [{x1}, {x2}]")
        zero_gamma = (x1 + x2) / 2

    return zero_gamma


def calculate_zero_gamma(dataframe, dte, risk_free_rate=0.05, strike_range=STRIKE_RANGE):
    """
    Calculate zero gamma using GEX at the current spot price.

    Parameters:
    dataframe (pd.DataFrame): Merged DataFrame with option data
    dte (float): Days to expiration (annualized)
    risk_free_rate (float): Risk-free interest rate
    strike_range (float): Range around current price to analyze (as percentage)

    Returns:
    tuple: (zero_gamma, underlying_price, min_price, max_price)
    """
    # Calculate GEX by strike at the current spot price
    gex = calculate_gex(dataframe)

    # Get underlying price
    underlying_price = dataframe['underlying_price'].iloc[0]

    # Find zero gamma
    zero_gamma = find_zero_gamma_from_gex(gex, underlying_price)

    # Calculate min and max price for strike filtering
    min_price = underlying_price * (1 - STRIKE_RANGE)
    max_price = underlying_price * (1 + STRIKE_RANGE)

    return zero_gamma, underlying_price, min_price, max_price


async def run(
        ticker: str,
        risk_free_rate: float,
        t: float,
        exp_to_use: datetime,
        pool,
):
    try:
        # Get option data
        df = await get_snapshot(ticker, EXPIRATION_INT)
        if df is None or df.empty:
            logger.error(f"Failed to get snapshot data for {ticker}")
            return

        # Calculate IVs
        df_iv = calculate_iv(df, t, RISK_FREE_RATE)

        # Calculate Greeks
        df_greeks = calculate_greeks(df_iv, t, RISK_FREE_RATE)

        # Get volume data
        volume = await get_ohlc(ticker, EXPIRATION_INT)
        if volume is None or volume.empty:
            logger.error(f"Failed to get OHLC data for {ticker}")
            return

        # Merge data
        merged = df_greeks.merge(volume, on=['root', 'right', 'strike'], how='left')
        merged.fillna(value=0, axis=1, inplace=True)

        # Calculate GEX
        gex = calculate_gex(merged)
        sorted_gex = gex.sort_values(by="total_gex", ascending=False)
        sorted_gex['strike'] = sorted_gex['strike']
        # sorted_gex.to_csv('sorted_gex.csv')

        # Get current NY time
        ny_now = datetime.now(NY_TIMEZONE)

        # Calculate and plot gamma profile
        zero_gamma, underlying_price, min_price, max_price = calculate_zero_gamma(
            merged,
            dte=t,
            risk_free_rate=risk_free_rate,
            strike_range=STRIKE_RANGE,
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
        logger.info(f"Successfully processed data for {ticker}")

        return data

    except Exception as e:
        logger.error(f"Error in run function: {e}")
        return None


async def main():
    try:
        # Setup initial connections
        next_close, ny_now, pool, loop = await configure()
        ticker_list = TICKER_LIST

        # Run continuously until stopped
        while True:
            start_time = time.time()
            time_to_expiration = update_dte(next_close)
            logger.info(f'Time to expiration: {time_to_expiration:.6f} years')

            for ticker in ticker_list:
                logger.info(f"Starting data collection cycle for {ticker}")
                data = await run(
                    ticker=ticker,
                    risk_free_rate=RISK_FREE_RATE,
                    t=time_to_expiration,
                    exp_to_use=EXPIRATION_INT,
                    pool=pool,
                )

            # Calculate processing time and adjust sleep
            processing_time = time.time() - start_time
            sleep_time = max(0, SLEEP_TIME - processing_time)  # Target 5-second cycle
            logger.info(f"Cycle completed in {processing_time:.2f}s, sleeping for {SLEEP_TIME:.2f}s")

            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("Main task was cancelled, shutting down")
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
    finally:
        # Ensure connections are closed
        if 'pool' in locals():
            await pool.close()


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

    logger.info("Shutdown complete")


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error in main process: {e}")
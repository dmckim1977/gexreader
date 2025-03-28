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
import numpy as np
import pandas as pd
import py_vollib_vectorized
from dotenv import load_dotenv
from pandas_market_calendars import get_calendar
from scipy.interpolate import InterpolatedUnivariateSpline
from scipy.optimize import root_scalar

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

load_dotenv()

base_url = os.getenv("THETADATA_URL")

TICKER_LIST: list = ['AAPL', 'MSFT', 'META', 'NVDA', 'TSLA', 'GOOGL', 'AMZN', 'VXX', 'PLTR', 'COIN']
# TICKER_LIST: list = ['NVDA']
EXPIRATION_TYPE: str = 'friday'  # options 'friday', or 'zero'
SLEEP_TIME: int = 30
RISK_FREE_RATE: float = 0.05
STRIKE_RANGE: float = 0.2
STRIKE_LEVELS: int = 100  # for exposure calculations
ZERO_GAMMA_WINDOW_SIZE: int = 10  # window for averaging zero gamma values by ticker
DAYS_IN_YEAR_DTE: int = 262


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


def get_next_options_expiration(current_date: datetime) -> datetime:
    """Returns the next stock options expiration date (Friday at 4:00 PM ET), adjusting for holidays."""
    ny_timezone = pytz.timezone('America/New_York')
    nyse = get_calendar('NYSE')  # NYSE calendar for U.S. stock/options holidays

    # Ensure current_date is timezone-aware
    if current_date.tzinfo is None:
        current_date = ny_timezone.localize(current_date)

    # Get NYSE trading days (valid schedule) for a reasonable range
    start_date = current_date.date()
    end_date = (current_date + timedelta(days=14)).date()  # Look 2 weeks ahead
    schedule = nyse.valid_days(start_date=start_date, end_date=end_date)
    holidays = nyse.holidays().holidays

    # If today is Friday and not a holiday, use today
    if current_date.weekday() == 4:  # Friday
        if current_date.date() not in holidays:
            return current_date.replace(hour=16, minute=0, second=0, microsecond=0)

    # Find the next Friday
    days_ahead = 4 - current_date.weekday()  # 4 is Friday (Monday=0, Sunday=6)
    if days_ahead <= 0:  # If today is Friday or after, go to next week
        days_ahead += 7
    next_friday = current_date + timedelta(days=days_ahead)
    next_friday = next_friday.replace(hour=16, minute=0, second=0, microsecond=0)
    if next_friday.tzinfo is None:
        next_friday = ny_timezone.localize(next_friday)

    # Adjust if next Friday is a holiday (e.g., Good Friday)
    while next_friday.date() in holidays:
        # Move to the previous trading day (typically Thursday)
        next_friday -= timedelta(days=1)
        # Ensure it’s a valid trading day
        while next_friday.date() not in schedule:
            next_friday -= timedelta(days=1)

    return next_friday


async def configure():
    # Setup initial connections
    pool = await connect_to_database()

    # Register signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    for sig_name in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, sig_name),
            lambda: asyncio.create_task(shutdown(pool))
        )

    # Get current NY time
    ny_timezone = pytz.timezone('America/New_York')
    ny_now = datetime.now(ny_timezone)

    # Check if today is a holiday and skip if it is
    nyse = get_calendar('NYSE')
    holidays = nyse.holidays().holidays
    if ny_now.date() in holidays:
        logger.info(f"Today ({ny_now.date()}) is a market holiday. Skipping execution.")
        await shutdown(pool)
        return

    # Get next options expiration
    next_expiration = get_next_options_expiration(ny_now)
    logger.info(f"Next expiration: {next_expiration}")

    # Calculate time until next market close (4:00 PM)
    next_close = next_expiration.replace(hour=16, minute=0, second=0, microsecond=0)
    if ny_now > next_close:
        # If we're past closing time, use next business day
        next_close += timedelta(days=1)
        # Simple business day check (not accounting for holidays)
        if next_close.weekday() >= 5:  # Saturday (5) or Sunday (6)
            next_close += timedelta(days=7 - next_close.weekday())

    expo_to_use = next_close.strftime("%Y%m%d")

    return next_close, ny_timezone, expo_to_use, pool, loop


def update_dte(next_close):
    ny_timezone = pytz.timezone('America/New_York')
    # Update time to expiration based on current time
    ny_now = datetime.now(ny_timezone)
    dte_days = (next_close - ny_now).total_seconds() / (24 * 60 * 60)

    return max(0, dte_days) / DAYS_IN_YEAR_DTE  # Ensure non-negative


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
            df['strike'] = df['strike'] / 1000
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
            df['strike'] = df['strike'] / 1000
            filt = df[['root', 'strike', 'right', 'volume']].copy()
            return filt


def calculate_iv(dataframe: pd.DataFrame, dte: float, risk_free_rate: float = RISK_FREE_RATE) -> pd.DataFrame:
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
    valid_mask = df[(df['iv'] != 0) & (df['iv'] != float('nan'))]

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


def calculate_gamma_at_levels(dataframe, levels, dte, risk_free_rate=RISK_FREE_RATE):
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
    Improved version that handles multiple crossings more intelligently.
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

    # Find all zero crossings
    zero_cross_idx = np.where(np.diff(np.sign(gex)))[0]

    if len(zero_cross_idx) == 0:
        return None

    # If there are multiple crossings, find the one with the strongest slope
    # This is generally the most significant crossing
    if len(zero_cross_idx) > 1:
        slopes = []
        for idx in zero_cross_idx:
            # Calculate absolute slope at crossing
            slope = abs((gex[idx + 1] - gex[idx]) / (levels[idx + 1] - levels[idx]))
            slopes.append(slope)

        # Get the crossing with steepest slope
        max_slope_idx = np.argmax(slopes)
        idx = zero_cross_idx[max_slope_idx]
    else:
        idx = zero_cross_idx[0]

    # Get values on either side of the crossing
    x1, y1 = levels[idx], gex[idx]
    x2, y2 = levels[idx + 1], gex[idx + 1]

    # Additional safety check for division
    if abs(y2 - y1) < 1e-10:  # Prevent near-zero division
        return (x1 + x2) / 2  # Just use midpoint if values are too close

    # Linear interpolation to find zero crossing
    zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)

    # Sanity check that result is between x1 and x2
    if not (min(x1, x2) <= zero_gamma <= max(x1, x2)):
        logger.warning(f"Interpolated zero gamma {zero_gamma} outside bounds [{x1}, {x2}]")
        return (x1 + x2) / 2  # Fallback to midpoint

    return zero_gamma

    # # Get values on either side of the crossing
    # idx = zero_cross_idx[0]
    # x1, y1 = levels[idx], gex[idx]
    # x2, y2 = levels[idx + 1], gex[idx + 1]
    #
    # # Linear interpolation to find zero crossing
    # zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)
    #
    # return zero_gamma


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

    return zero_gamma, underlying_price, min_price, max_price, gamma_df


def find_zero_gamma_from_gex(gex_df):
    """
    Find the zero gamma point directly from the GEX by strike data.

    Parameters:
    gex_df (pd.DataFrame): DataFrame with 'strike' and 'total_gex' columns (output of calculate_gex)

    Returns:
    float: The strike price where cumulative GEX crosses zero (zero gamma point)
    """
    if gex_df is None or len(gex_df) < 2:
        logger.warning("Not enough data to calculate zero gamma")
        return None

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
        return None

    # Find where cumulative GEX crosses zero
    zero_cross_idx = np.where(np.diff(np.sign(cumulative_gex)))[0]
    if len(zero_cross_idx) == 0:
        logger.warning("No zero crossing found in cumulative GEX")
        return None

    # Take the first crossing (or you can add logic for multiple crossings as in your original code)
    idx = zero_cross_idx[0]
    x1, y1 = strikes[idx], cumulative_gex[idx]
    x2, y2 = strikes[idx + 1], cumulative_gex[idx + 1]

    # Linear interpolation to find the exact zero crossing
    if abs(y2 - y1) < 1e-10:  # Avoid division by near-zero
        return (x1 + x2) / 2
    zero_gamma = x1 - y1 * (x2 - x1) / (y2 - y1)

    # Ensure the result is within bounds
    if not (min(x1, x2) <= zero_gamma <= max(x1, x2)):
        logger.warning(f"Interpolated zero gamma {zero_gamma} outside bounds [{x1}, {x2}]")
        return (x1 + x2) / 2

    return zero_gamma


def calculate_zero_gamma_from_gex(dataframe, dte, risk_free_rate=RISK_FREE_RATE):
    """
    Calculate zero gamma directly from GEX data, skipping the gamma profile calculation.

    Parameters:
    dataframe (pd.DataFrame): Merged DataFrame with option data (output of merging snapshot and OHLC)
    dte (float): Days to expiration (annualized)
    risk_free_rate (float): Risk-free interest rate

    Returns:
    tuple: (zero_gamma, underlying_price)
    """
    # Calculate GEX by strike
    gex = calculate_gex(dataframe)

    # Find zero gamma
    zero_gamma = find_zero_gamma_from_gex(gex)

    # Get underlying price
    underlying_price = dataframe['underlying_price'].iloc[0]

    return zero_gamma, underlying_price


async def run(
        ticker: str,
        risk_free_rate: float,
        t: float,
        exp_to_use: datetime,
        pool,
):
    logger.info('Entering run function')
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

        # Get IV data by strike for calls and puts
        iv_by_strike = {}
        for right in ['C', 'P']:
            for _, row in merged[merged['right'] == right].iterrows():
                strike = row['strike']
                if strike not in iv_by_strike:
                    iv_by_strike[strike] = {'C': None, 'P': None}
                iv_by_strike[strike][right] = row['iv']

        # Get current NY time
        ny_timezone = pytz.timezone('America/New_York')
        ny_now = datetime.now(ny_timezone)

        zero_gamma, underlying_price, min_price, max_price, gamma_df = calculate_gamma_profile(
            merged,
            dte=t,
            risk_free_rate=risk_free_rate,
            strike_range=STRIKE_RANGE,
        )
        print(ticker)
        print(zero_gamma)

        zero_gamma, underlying_price = calculate_zero_gamma_from_gex(
            merged,
            dte=t,
            risk_free_rate=risk_free_rate,
        )
        print(zero_gamma)

        # In your run function, after calculating zero_gamma:
        if 'prev_zero_gammas' not in globals():
            globals()['prev_zero_gammas'] = []


        # Create exposure lookup dictionary if gamma_df exists
        exposure_by_level = {}
        if gamma_df is not None:
            for _, row in gamma_df.iterrows():
                exposure_by_level[row['level']] = row['total_gex']

        # Extend each strike's data with IV and exposure
        enhanced_strikes = []
        for _, row in sorted_gex.iterrows():
            strike = row['strike']

            # Base data (what you currently have)
            strike_data = [
                float(strike),
                float(row['call_gex']),
                float(row['put_gex']),
                float(row['total_gex'])
            ]

            # Add call IV
            call_iv = iv_by_strike.get(strike, {}).get('C', None)
            strike_data.append(float(call_iv) if call_iv is not None else 0)

            # Add put IV
            put_iv = iv_by_strike.get(strike, {}).get('P', None)
            strike_data.append(float(put_iv) if put_iv is not None else 0)

            # Add exposure
            if gamma_df is not None:
                # Find closest level
                closest_level = min(exposure_by_level.keys(), key=lambda x: abs(x - strike))
                exposure = exposure_by_level[closest_level]
                strike_data.append(float(exposure))
            else:
                strike_data.append(0)

            enhanced_strikes.append(strike_data)

        # Sort by strike price for consistency
        enhanced_strikes.sort(key=lambda x: x[0])

        # Standardize ticker
        root = ticker

        # Prepare data for insertion
        data = {
            "msg_type": "gex3",
            "data": {
                "timestamp": ny_now.isoformat(),
                "ticker": root,
                "expiration": 'zero',
                "spot": float(underlying_price),
                "zero_gamma": float(zero_gamma) if zero_gamma is not None else None,  # Fallback to current price
                "major_pos_vol": float(sorted_gex['strike'].iloc[0]) if not sorted_gex.empty else 0,
                "major_neg_vol": float(sorted_gex['strike'].iloc[-1]) if not sorted_gex.empty else 0,
                "sum_gex_vol": float(gex['total_gex'].sum()) if not gex.empty else 0,
                "minor_pos_vol": float(sorted_gex['strike'].iloc[1]) if len(sorted_gex) > 1 else 0,
                "minor_neg_vol": float(sorted_gex['strike'].iloc[-2]) if len(sorted_gex) > 1 else 0,
            },
            "trades": [],
            "strikes": enhanced_strikes,
        }

        # Save to database
        # await insert_to_database(pool, data)  # TODO uncomment for prod

        logger.info(f"Successfully processed data for {ticker}")

        return {
            'spot': float(underlying_price),
            'zero_gamma': float(zero_gamma) if zero_gamma is not None else None,
            'major_pos_vol': float(sorted_gex['strike'].iloc[0]) if not sorted_gex.empty else 0,
            'major_neg_vol': float(sorted_gex['strike'].iloc[-1]) if not sorted_gex.empty else 0,
            'data': data  # Return full data as well if needed
        }

    except Exception as e:
        logger.error(f"Error in run function: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


async def main():
    try:
        next_close, ny_timezone, expo_to_use, pool, loop = await configure()
        rfr = RISK_FREE_RATE
        ticker_list = TICKER_LIST

        # Run continuously until stopped
        while True:
            start_time = time.time()

            time_to_expiration = update_dte(next_close)

            for ticker in ticker_list:
                logger.info(f"Starting data collection cycle for {ticker}")
                data = await run(
                    ticker=ticker,
                    risk_free_rate=rfr,
                    t=time_to_expiration,
                    exp_to_use=next_close,
                    pool=pool,
                )

                if data:
                    # Extract values for plotting
                    spot = data['spot']
                    zero_gamma = data['zero_gamma'] if data['zero_gamma'] is not None else spot
                    major_pos_vol = data['major_pos_vol']
                    major_neg_vol = data['major_neg_vol']

                    # Set breakpoint here to inspect after plotting
                    logger.info(f"Completed processing {ticker}")

            # Calculate processing time and adjust sleep
            processing_time = time.time() - start_time
            sleep_time = max(0, SLEEP_TIME - processing_time)  # Target 5-second cycle

            # loop_counter += 1  # TODO uncomment

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
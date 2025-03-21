import asyncio
import asyncpg
import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.gridspec import GridSpec
import pandas as pd
import argparse
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import numpy as np

load_dotenv(verbose=True)

# Database connection details from .env
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

# Main query for premium data
PREMIUM_QUERY = """
SELECT
    time_bucket(%s, trade_datetime) AS bucket_time,
    SUM(CASE WHEN side = 'C' AND aggressor = 1 THEN premium ELSE 0 END) AS call_premium_bought,
    SUM(CASE WHEN side = 'C' AND aggressor = -1 THEN premium ELSE 0 END) AS call_premium_sold,
    SUM(CASE WHEN side = 'P' AND aggressor = 1 THEN premium ELSE 0 END) AS put_premium_bought,
    SUM(CASE WHEN side = 'P' AND aggressor = -1 THEN premium ELSE 0 END) AS put_premium_sold,
    AVG(spot) AS avg_spot_price
FROM options_trades
WHERE trade_datetime >= $1
  AND trade_datetime < $2
  AND root = $3
  AND ($4::integer[] IS NULL OR condition = ANY($4::integer[]))
GROUP BY bucket_time
ORDER BY bucket_time;
"""


async def fetch_historical_data(date_str, ticker, bucket_interval='1 minute', conditions=None, time_window=None,
                                calc_window=10):
    """
    Fetches historical options premium data for a specific date and ticker.

    Args:
        date_str (str): Date in 'YYYY-MM-DD' format
        ticker (str): Ticker symbol (e.g., 'SPXW')
        bucket_interval (str): Time bucket interval (default: '1 minute')
        conditions (list): List of condition codes to filter (default: None)
        time_window (tuple): Optional start and end time in 'HH:MM' format (default: None for full day)
        calc_window (int): Number of periods to use for rolling calculations

    Returns:
        pandas.DataFrame: DataFrame with time-bucketed premium data
    """
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')

        # Set the time window
        if time_window:
            start_time, end_time = time_window
            start_dt = datetime.strptime(f"{date_str} {start_time}", '%Y-%m-%d %H:%M')
            end_dt = datetime.strptime(f"{date_str} {end_time}", '%Y-%m-%d %H:%M')
        else:
            # Default to market hours (9:30 AM to 4:00 PM)
            start_dt = datetime.strptime(f"{date_str} 09:30", '%Y-%m-%d %H:%M')
            end_dt = datetime.strptime(f"{date_str} 16:00", '%Y-%m-%d %H:%M')

        print(
            f"Fetching data for {ticker} on {date_str} from {start_dt.strftime('%H:%M')} to {end_dt.strftime('%H:%M')}")
        print(f"Using bucket interval: {bucket_interval}")

        # Connect to database
        conn = await asyncpg.connect(**DB_CONFIG)

        # Execute query with proper SQL string interpolation for the interval
        # Note: We're using the raw SQL format for the time_bucket function
        query = PREMIUM_QUERY.replace("%s", f"'{bucket_interval}'")

        print(f"Executing query with parameters: start={start_dt}, end={end_dt}, ticker={ticker}")

        try:
            rows = await conn.fetch(
                query,
                start_dt,
                end_dt,
                ticker,
                conditions
            )
            print(f"Query returned {len(rows)} rows")

            # If no rows returned, try a more permissive query for debugging
            if len(rows) == 0:
                print("No data found. Trying a test query to see if any data exists for this ticker today...")
                test_query = """
                SELECT MIN(trade_datetime) as first_trade, 
                       MAX(trade_datetime) as last_trade, 
                       COUNT(*) as trade_count 
                FROM options_trades 
                WHERE trade_datetime >= $1 
                AND trade_datetime < $2 
                AND root = $3
                """
                test_result = await conn.fetchrow(test_query, start_dt, end_dt, ticker)

                if test_result and test_result['trade_count'] > 0:
                    print(f"Found {test_result['trade_count']} trades for {ticker}")
                    print(f"First trade at: {test_result['first_trade']}")
                    print(f"Last trade at: {test_result['last_trade']}")
                else:
                    print(f"No trades found for {ticker} on {date_str}")
        except Exception as e:
            print(f"Query execution error: {e}")
            raise

        # Convert to pandas DataFrame
        df = pd.DataFrame(rows, columns=[
            'bucket_time', 'call_premium_bought', 'call_premium_sold',
            'put_premium_bought', 'put_premium_sold', 'avg_spot_price'
        ])

        await conn.close()

        try:
            # Convert to DataFrame and handle null values
            if not df.empty:
                df['bucket_time'] = pd.to_datetime(df['bucket_time'])
                # Fill NaN values with 0 for premium columns
                premium_cols = ['call_premium_bought', 'call_premium_sold', 'put_premium_bought', 'put_premium_sold']
                df[premium_cols] = df[premium_cols].fillna(0)

                # Convert Decimal objects to float to avoid Decimal-specific errors
                for col in premium_cols:
                    df[col] = df[col].astype(float)

                # Calculate derived metrics
                df['net_call_premium'] = df['call_premium_bought'] - df['call_premium_sold']
                df['net_put_premium'] = df['put_premium_bought'] - df['put_premium_sold']
                df['net_premium'] = df['net_call_premium'] - df['net_put_premium']

                # Calculate bullish ratio with safe division
                df['bullish_actions'] = df['call_premium_bought'] + df['put_premium_sold']
                df['bearish_actions'] = df['call_premium_sold'] + df['put_premium_bought']

                # Safe division - replace zeros with NaN before division to avoid errors
                bearish_safe = df['bearish_actions'].replace(0, np.nan)
                df['bullish_ratio'] = df['bullish_actions'] / bearish_safe

                # Add rolling calculations with safe division
                rolling_window = calc_window  # Use the provided calculation window

                df['rolling_net_premium'] = df['net_premium'].rolling(window=rolling_window, min_periods=1).sum()

                # Safe rolling calculation for bullish ratio
                rolling_bullish = df['bullish_actions'].rolling(window=rolling_window, min_periods=1).sum()
                rolling_bearish = df['bearish_actions'].rolling(window=rolling_window, min_periods=1).sum()

                # Replace zeros with NaN to avoid division by zero
                rolling_bearish_safe = rolling_bearish.replace(0, np.nan)
                df['rolling_bullish_ratio'] = rolling_bullish / rolling_bearish_safe

            return df

        except Exception as e:
            print(f"Error processing data: {e}")
            import traceback
            traceback.print_exc()
            return None

    except Exception as e:
        print(f"Error fetching data: {e}")
        raise


def plot_historical_data(df, ticker, date_str, time_window=None, save_path=None, calc_window=30):
    """
    Creates visualizations of historical options premium data in vertically stacked panes.

    Args:
        df (pandas.DataFrame): DataFrame with premium data
        ticker (str): Ticker symbol
        date_str (str): Date in 'YYYY-MM-DD' format
        time_window (tuple): Optional time window used for the query
        save_path (str): Path to save the plot
        calc_window (int): Number of periods in the rolling calculation window
    """
    if df is None or df.empty:
        print("No data to plot")
        return

    # Print summary of the data we're plotting
    print(f"\nData Summary for {ticker} on {date_str}:")
    print(f"Time range: {df['bucket_time'].min()} to {df['bucket_time'].max()}")
    print(f"Number of data points: {len(df)}")
    print(f"Total call premium bought: ${df['call_premium_bought'].sum():,.2f}")
    print(f"Total call premium sold: ${df['call_premium_sold'].sum():,.2f}")
    print(f"Total put premium bought: ${df['put_premium_bought'].sum():,.2f}")
    print(f"Total put premium sold: ${df['put_premium_sold'].sum():,.2f}")
    if 'avg_spot_price' in df.columns and not df['avg_spot_price'].isna().all():
        print(f"Average spot price: ${df['avg_spot_price'].mean():,.2f}")
    print("-------------------------------------")

    # Set matplotlib backend to Agg for non-interactive environments
    import matplotlib
    matplotlib.use('Agg')

    # Create a figure with 7 vertically stacked subplots
    fig, axes = plt.subplots(7, 1, figsize=(20, 24), sharex=True, gridspec_kw={'hspace': 0.3})

    # Time window description for title
    time_desc = f" ({time_window[0]} to {time_window[1]})" if time_window else ""

    # Main title
    fig.suptitle(f"{ticker} Options Premium Analysis - {date_str}{time_desc}", fontsize=20, y=0.99)

    # 1. Spot Price (Top)
    ax_spot = axes[0]
    if 'avg_spot_price' in df.columns and not df['avg_spot_price'].isna().all():
        ax_spot.plot(df['bucket_time'], df['avg_spot_price'], color='black', linewidth=1.5)
        ax_spot.set_title(f'{ticker} Spot Price', fontsize=14)
        ax_spot.set_ylabel('Price ($)', fontsize=12)
    else:
        ax_spot.text(0.5, 0.5, 'No spot price data available', horizontalalignment='center',
                     verticalalignment='center', transform=ax_spot.transAxes)
        ax_spot.set_title('Spot Price (No Data)', fontsize=14)
    ax_spot.grid(True, alpha=0.3)

    # 7. Bullish Ratio (Bottom)
    ax_ratio = axes[1]
    ax_ratio.plot(df['bucket_time'], df['bullish_ratio'], color='green', label='Bullish Ratio', linewidth=1.5)
    ax_ratio.plot(df['bucket_time'], df['rolling_bullish_ratio'], color='orange',
                  label=f'Rolling Bullish Ratio ({calc_window} periods)', linewidth=2)
    ax_ratio.axhline(y=1, color='black', linestyle='-', alpha=0.3, label='Neutral Line')
    ax_ratio.set_title('Bullish Ratio (>1 is Bullish)', fontsize=14)
    ax_ratio.set_ylabel('Ratio', fontsize=12)
    ax_ratio.set_ylim(bottom=0)  # Assuming ratio doesn't go below 0
    ax_ratio.legend(loc='upper right')
    ax_ratio.grid(True, alpha=0.3)

    # 2. Net Premium
    ax_net = axes[2]
    ax_net.plot(df['bucket_time'], df['net_premium'], color='blue', label='Net Premium', linewidth=1.5)
    ax_net.plot(df['bucket_time'], df['rolling_net_premium'], color='red',
                label=f'Rolling Net Premium ({calc_window} periods)', linewidth=2, linestyle='-')
    ax_net.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax_net.set_title('Net Premium (Calls - Puts)', fontsize=14)
    ax_net.set_ylabel('Net Premium ($)', fontsize=12)
    ax_net.legend(loc='upper right')
    ax_net.grid(True, alpha=0.3)

    # 3. Call Premium Bought
    ax_call_bought = axes[3]
    ax_call_bought.plot(df['bucket_time'], df['call_premium_bought'], color='green', linewidth=1.5)
    ax_call_bought.set_title('Call Premium Bought', fontsize=14)
    ax_call_bought.set_ylabel('Premium ($)', fontsize=12)
    ax_call_bought.grid(True, alpha=0.3)

    # 4. Call Premium Sold
    ax_call_sold = axes[4]
    ax_call_sold.plot(df['bucket_time'], df['call_premium_sold'], color='red', linewidth=1.5)
    ax_call_sold.set_title('Call Premium Sold', fontsize=14)
    ax_call_sold.set_ylabel('Premium ($)', fontsize=12)
    ax_call_sold.grid(True, alpha=0.3)

    # 5. Put Premium Bought
    ax_put_bought = axes[5]
    ax_put_bought.plot(df['bucket_time'], df['put_premium_bought'], color='blue', linewidth=1.5)
    ax_put_bought.set_title('Put Premium Bought', fontsize=14)
    ax_put_bought.set_ylabel('Premium ($)', fontsize=12)
    ax_put_bought.grid(True, alpha=0.3)

    # 6. Put Premium Sold
    ax_put_sold = axes[6]
    ax_put_sold.plot(df['bucket_time'], df['put_premium_sold'], color='orange', linewidth=1.5)
    ax_put_sold.set_title('Put Premium Sold', fontsize=14)
    ax_put_sold.set_ylabel('Premium ($)', fontsize=12)
    ax_put_sold.grid(True, alpha=0.3)

    # Add x-axis label only to the bottom subplot
    axes[-1].set_xlabel('Time', fontsize=12)

    # Format x-axis for all subplots and make y-axis more readable
    for i, ax in enumerate(axes):
        # X-axis formatting
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator())

        # Only show x-axis labels on the bottom subplot
        if i < len(axes) - 1:
            plt.setp(ax.get_xticklabels(), visible=False)
        else:
            for label in ax.get_xticklabels():
                label.set_rotation(45)
                label.set_ha('right')
                label.set_fontsize(10)

        # Y-axis formatting
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: f"${x:,.0f}" if abs(x) >= 1000 else f"${x:,.2f}"))
        ax.tick_params(axis='y', labelsize=10)

        # Add gridlines
        ax.grid(True, alpha=0.3, which='both')

    # Add more space for the bottom subplot's x-axis labels
    plt.subplots_adjust(bottom=0.05)

    # Always save the plot
    try:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"Plot saved to: {save_path}")
    except Exception as e:
        print(f"Error saving plot: {e}")
        # Try a fallback location
        fallback_path = f"{ticker}_{date_str.replace('-', '')}_plot.png"
        try:
            plt.savefig(fallback_path, dpi=300, bbox_inches='tight')
            print(f"Plot saved to fallback location: {fallback_path}")
        except Exception as e2:
            print(f"Error saving to fallback location: {e2}")

    # Don't try to show the plot in non-interactive environments
    plt.close(fig)


async def main():
    parser = argparse.ArgumentParser(description='Plot historical options premium data')
    parser.add_argument('--date', help='Date in YYYY-MM-DD format (defaults to today)')
    parser.add_argument('--ticker', required=True, help='Ticker symbol (e.g., SPXW)')
    parser.add_argument('--interval', default='1 second',
                        help='Time bucket interval (e.g., "1 second", "5 seconds", "1 minute")')
    parser.add_argument('--start-time', default='09:30', help='Start time in HH:MM format')
    parser.add_argument('--end-time', default='16:00', help='End time in HH:MM format')
    parser.add_argument('--conditions', nargs='+', type=int, help='List of condition codes to filter')
    parser.add_argument('--calc-window', type=int, default=30,
                        help='Number of periods to use for rolling calculations')
    parser.add_argument('--save', help='Path to save the plot (if not provided, plot will be displayed)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode with more verbose output')

    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Use today's date if not specified
    if args.date:
        date_str = args.date
    else:
        date_str = datetime.now().strftime('%Y-%m-%d')
        print(f"No date specified, using today: {date_str}")

    time_window = (args.start_time, args.end_time)

    # Fetch the data
    df = await fetch_historical_data(
        date_str,
        args.ticker,
        args.interval,
        args.conditions,
        time_window,
        args.calc_window
    )

    # Plot the data
    plot_historical_data(df, args.ticker, args.date, time_window, args.save)


if __name__ == "__main__":
    asyncio.run(main())
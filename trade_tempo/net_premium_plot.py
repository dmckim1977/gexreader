import asyncio
import asyncpg
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime
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
WHERE DATE(trade_datetime) = DATE($1)
  AND trade_datetime >= $1
  AND trade_datetime < $2
  AND root = $3
  AND ($4::integer[] IS NULL OR condition = ANY($4::integer[]))
GROUP BY bucket_time
ORDER BY bucket_time;
"""


async def fetch_data(date_str, ticker, bucket_interval='1 second', conditions=None, time_window=None):
    """
    Fetches options premium data from database
    """
    try:
        # Set the time window
        if time_window:
            start_time, end_time = time_window
            start_dt = datetime.strptime(f"{date_str} {start_time}", '%Y-%m-%d %H:%M')
            end_dt = datetime.strptime(f"{date_str} {end_time}", '%Y-%m-%d %H:%M')
        else:
            # Default to market hours
            start_dt = datetime.strptime(f"{date_str} 09:30", '%Y-%m-%d %H:%M')
            end_dt = datetime.strptime(f"{date_str} 16:00", '%Y-%m-%d %H:%M')

        print(
            f"Fetching data for {ticker} on {date_str} from {start_dt.strftime('%H:%M')} to {end_dt.strftime('%H:%M')}")

        # Connect to database
        conn = await asyncpg.connect(**DB_CONFIG)

        # Execute query with proper SQL string interpolation for the interval
        query = PREMIUM_QUERY.replace("%s", f"'{bucket_interval}'")

        rows = await conn.fetch(
            query,
            start_dt,
            end_dt,
            ticker,
            conditions
        )

        print(f"Query returned {len(rows)} rows")

        # Convert to DataFrame and process data
        df = pd.DataFrame(rows, columns=[
            'bucket_time', 'call_premium_bought', 'call_premium_sold',
            'put_premium_bought', 'put_premium_sold', 'avg_spot_price'
        ])

        await conn.close()

        if not df.empty:
            # Convert time and ensure numeric data types
            df['bucket_time'] = pd.to_datetime(df['bucket_time'])
            premium_cols = ['call_premium_bought', 'call_premium_sold', 'put_premium_bought', 'put_premium_sold']
            df[premium_cols] = df[premium_cols].fillna(0).astype(float)

            # Calculate net values
            df['net_call_premium'] = df['call_premium_bought'] - df['call_premium_sold']
            df['net_put_premium'] = df['put_premium_bought'] - df['put_premium_sold']
            df['net_premium'] = df['net_call_premium'] - df['net_put_premium']

            # Calculate cumulative sums
            df['cum_net_call_premium'] = df['net_call_premium'].cumsum()
            df['cum_net_put_premium'] = df['net_put_premium'].cumsum()
            df['cum_total_net_premium'] = df['net_premium'].cumsum()

        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def plot_cumulative_data(df, ticker, date_str, output_file=None):
    """
    Creates a 3-panel visualization:
    1. Spot price
    2. Cumulative net call premium and cumulative net put premium (same panel)
    3. Cumulative total net premium
    """
    if df.empty:
        print("No data to plot")
        return

    # Print summary of the data
    print(f"\nData Summary for {ticker} on {date_str}:")
    print(f"Time range: {df['bucket_time'].min()} to {df['bucket_time'].max()}")
    print(f"Number of data points: {len(df)}")
    print(f"Final cumulative net call premium: ${df['cum_net_call_premium'].iloc[-1]:,.2f}")
    print(f"Final cumulative net put premium: ${df['cum_net_put_premium'].iloc[-1]:,.2f}")
    print(f"Final cumulative total net premium: ${df['cum_total_net_premium'].iloc[-1]:,.2f}")
    if 'avg_spot_price' in df.columns and not df['avg_spot_price'].isna().all():
        print(f"Average spot price: ${df['avg_spot_price'].mean():,.2f}")

    # Set matplotlib backend
    import matplotlib
    matplotlib.use('Agg')

    # Create figure with 3 vertically stacked subplots
    fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True, gridspec_kw={'hspace': 0.3})

    # Main title
    fig.suptitle(f"{ticker} Cumulative Options Premium - {date_str}", fontsize=18, y=0.98)

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

    # 2. Cumulative Net Call and Put Premium
    ax_call_put = axes[1]
    ax_call_put.plot(df['bucket_time'], df['cum_net_call_premium'], color='green',
                     label='Cumulative Net Call Premium', linewidth=1.5)
    ax_call_put.plot(df['bucket_time'], df['cum_net_put_premium'], color='red',
                     label='Cumulative Net Put Premium', linewidth=1.5)
    ax_call_put.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax_call_put.set_title('Cumulative Net Call and Put Premium', fontsize=14)
    ax_call_put.set_ylabel('Cumulative Premium ($)', fontsize=12)
    ax_call_put.legend(loc='upper left')
    ax_call_put.grid(True, alpha=0.3)

    # 3. Cumulative Total Net Premium
    ax_total = axes[2]
    ax_total.plot(df['bucket_time'], df['cum_total_net_premium'], color='blue',
                  label='Cumulative Total Net Premium', linewidth=2)
    ax_total.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax_total.set_title('Cumulative Total Net Premium', fontsize=14)
    ax_total.set_ylabel('Cumulative Premium ($)', fontsize=12)
    ax_total.legend(loc='upper left')
    ax_total.grid(True, alpha=0.3)

    # Add x-axis label to the bottom subplot
    ax_total.set_xlabel('Time', fontsize=12)

    # Format axes
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

        # Y-axis formatting for dollar values
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, loc: f"${x:,.0f}" if abs(x) >= 1000 else f"${x:,.2f}"))
        ax.tick_params(axis='y', labelsize=10)

        # Add gridlines
        ax.grid(True, alpha=0.3, which='both')

    # Add market open/close lines
    try:
        market_open = datetime.strptime(f"{date_str} 09:30", '%Y-%m-%d %H:%M')
        market_close = datetime.strptime(f"{date_str} 16:00", '%Y-%m-%d %H:%M')

        for ax in axes:
            ax.axvline(x=market_open, color='green', linestyle='--', alpha=0.5, label='Market Open')
            ax.axvline(x=market_close, color='red', linestyle='--', alpha=0.5, label='Market Close')
    except Exception as e:
        print(f"Could not add market event lines: {e}")

    # Layout adjustments
    plt.tight_layout()
    plt.subplots_adjust(top=0.94)

    # Save the plot
    if output_file:
        output_path = output_file
    else:
        output_path = f"{ticker}_{date_str.replace('-', '')}_cumulative.png"

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")
    plt.close(fig)


async def main():
    # Change these parameters as needed
    date = "2025-03-18"  # YYYY-MM-DD format
    ticker = "SPXW"
    interval = "1 second"
    time_window = ("0:30", "16:00")  # (start_time, end_time) in HH:MM format
    output_file = f"{ticker}_{date.replace('-', '')}_cumulative.png"

    # Fetch data
    df = await fetch_data(date, ticker, interval, None, time_window)

    # Generate plot
    if not df.empty:
        plot_cumulative_data(df, ticker, date, output_file)
    else:
        print(f"No data available for {ticker} on {date}")


if __name__ == "__main__":
    asyncio.run(main())
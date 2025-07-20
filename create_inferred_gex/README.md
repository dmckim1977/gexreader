# Inferred GEX Processor

Real-time Gamma Exposure (GEX) calculation service that processes inferred trades from Redis and calculates aggregated GEX data.

## Overview

This service subscribes to the `inferred_trades` Redis channel, aggregates options trades in real-time, calculates GEX using Black-Scholes pricing, and stores results in a TimescaleDB table while publishing to Redis.

## Features

- **Real-time Processing**: Subscribes to `inferred_trades` Redis channel
- **1-Second Aggregation**: Processes trades every second for near real-time GEX updates
- **All Strikes**: Uses all available strikes (no filtering like the naive GEX service)
- **Configurable Tickers**: Filter processing by ticker list or process all tickers
- **TimescaleDB Storage**: Stores results in `inferred_gexray3` hypertable
- **Redis Publishing**: Publishes results to `inferred_gex` channel

## Data Flow

1. **Input**: Subscribes to `inferred_trades` Redis channel
2. **Buffering**: Aggregates trades by ticker/expiration in 1-second windows
3. **Calculation**: Calculates IV, Greeks, and GEX for all strikes
4. **Storage**: Inserts aggregated data into `inferred_gexray3` table
5. **Publishing**: Publishes results to `inferred_gex` Redis channel

## Database Schema

The service requires the `inferred_gexray3` TimescaleDB table:

```sql
CREATE TABLE IF NOT EXISTS inferred_gexray3 (
    time TIMESTAMPTZ NOT NULL,
    msg_type TEXT NOT NULL DEFAULT 'inferred_gex',
    ticker TEXT NOT NULL,
    expiration TEXT NOT NULL,
    spot DOUBLE PRECISION NOT NULL,
    zero_gamma DOUBLE PRECISION,
    major_pos_vol DOUBLE PRECISION,
    major_neg_vol DOUBLE PRECISION, 
    sum_gex_vol DOUBLE PRECISION,
    minor_pos_vol DOUBLE PRECISION,
    minor_neg_vol DOUBLE PRECISION,
    trades JSONB,
    strikes JSONB,
    trade_count INTEGER DEFAULT 0,
    total_volume INTEGER DEFAULT 0,
    CONSTRAINT inferred_gexray3_pkey PRIMARY KEY (time, ticker, expiration)
);

SELECT create_hypertable('inferred_gexray3', 'time', if_not_exists => TRUE);
```

## Configuration

### Environment Variables

- `POSTGRES_USER`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password  
- `POSTGRES_DB`: PostgreSQL database name
- `POSTGRES_HOST`: PostgreSQL host
- `REDIS_HOST`: Redis host (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `REDIS_DB`: Redis database (default: 0)

### Configuration Constants

- `TICKER_LIST`: List of tickers to process (default: ["SPXW", "SPY", "QQQ", "TSLA", "NVDA"])
- `AGGREGATION_INTERVAL`: Processing interval in seconds (default: 1.0)
- `RISK_FREE_RATE`: Risk-free rate for Greeks calculation (default: 0.025)

## Docker Deployment

```bash
# Build and run the service
cd /path/to/create_inferred_gex
docker compose build
docker compose up -d

# Check logs
docker logs inferred_gex_processor -f

# Stop service
docker compose stop inferred_gex_processor
```

## Input Data Format

Expects JSON messages on `inferred_trades` channel:

```json
{
    "root": "SPY",
    "strike": 455.0,
    "right": "C",
    "size": 100,
    "price": 12.50,
    "spot": 455.73,
    "aggressor": 1,
    "expiration": "2025-01-17",
    "condition": 0,
    "trade_datetime": "2025-07-19T15:30:45.123456Z",
    "signed_premium": 125000.00
}
```

## Output Data Format

Publishes to `inferred_gex` Redis channel:

```json
{
    "msg_type": "inferred_gex",
    "ticker": "SPY",
    "data": {
        "timestamp": "2025-07-20T15:30:45.123456+00:00",
        "ticker": "SPY",
        "expiration": "2025-01-17",
        "spot": 455.73,
        "zero_gamma": 456.25,
        "major_pos_vol": 460.0,
        "major_neg_vol": 450.0,
        "sum_gex_vol": 1234567.89,
        "minor_pos_vol": 458.0,
        "minor_neg_vol": 452.0,
        "trade_count": 25,
        "total_volume": 2500
    },
    "trades": [...],
    "strikes": [[strike, gex, volume], ...],
    "event_id": "inferred_gex:2025-07-20T15:30:45.123456"
}
```

## Monitoring

- **Logs**: Monitor Docker logs for processing status and errors
- **Redis**: Check `inferred_gex` channel for published data
- **Database**: Query `inferred_gexray3` table for stored results

## Testing

Use the included test publisher to verify the service works without live data:

```bash
# Publish 100 test trades
python test_publisher.py

# Publish trades continuously 
python test_publisher.py continuous
```

## Troubleshooting

### Redis Connection Issues

1. **Environment Variables**: Ensure `REDIS_REMOTE` is set correctly in your environment
2. **Network**: With `network_mode: "host"`, the container uses the host network
3. **Redis Service**: Verify Redis is running and accessible on the configured host/port

```bash
# Test Redis connection
redis-cli -h $REDIS_REMOTE ping

# Check Redis logs
docker logs redis-container-name
```

### Common Issues

- **Timeout errors**: Normal when no data is flowing on `inferred_trades` channel
- **Connection refused**: Check Redis host configuration and network connectivity
- **Database errors**: Ensure the `inferred_gexray3` table exists and user has permissions

### Monitoring

```bash
# Watch service logs
docker logs inferred_gex -f

# Check Redis channels
redis-cli monitor

# Query database
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT COUNT(*) FROM inferred_gexray3;"
```

## Performance Notes

- Processes high-volume trade streams in real-time
- Uses connection pooling for database efficiency
- Handles JSON serialization for complex data structures
- Implements proper error handling and graceful shutdown
- Auto-reconnects to Redis on connection loss
- Exponential backoff for connection retries
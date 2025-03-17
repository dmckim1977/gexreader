import asyncio
import asyncpg
import logging
from datetime import datetime, timezone, timedelta
from collections import deque
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
import json
import os
from dotenv import load_dotenv

load_dotenv(verbose=True)

logger = logging.getLogger(__name__)

# Database connection details from .env
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


QUERY = """
SELECT
    time_bucket('1 second', trade_datetime) AS utc_time,
    SUM(CASE WHEN side = 'C' AND aggressor = 1 THEN premium ELSE 0 END) AS call_premium_bought,
    SUM(CASE WHEN side = 'C' AND aggressor = -1 THEN premium ELSE 0 END) AS call_premium_sold,
    SUM(CASE WHEN side = 'P' AND aggressor = 1 THEN premium ELSE 0 END) AS put_premium_bought,
    SUM(CASE WHEN side = 'P' AND aggressor = -1 THEN premium ELSE 0 END) AS put_premium_sold
FROM options_trades
WHERE trade_datetime > $1
  AND trade_datetime < $2
  AND root = ANY($3)
  AND ($4::integer[] IS NULL OR condition = ANY($4::integer[]))
GROUP BY time_bucket('1 second', trade_datetime)
ORDER BY utc_time;
"""

class PremiumTracker:
    def __init__(self, window_seconds=30, roots=['QQQ'], conditions=None):
        self.window_seconds = window_seconds
        self.roots = roots
        self.conditions = conditions
        self.last_timestamp = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        self.data_queue = deque(maxlen=window_seconds)

    async def fetch_new_trades(self):
        conn = await asyncpg.connect(**DB_CONFIG)
        try:
            await conn.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            now_utc = datetime.now(timezone.utc)
            logger.info(f"Fetching trades from {self.last_timestamp} to {now_utc}")
            results = await conn.fetch(
                QUERY,
                self.last_timestamp,
                now_utc,
                self.roots,
                self.conditions
            )
            logger.info(f"Fetched {len(results)} new trade buckets")
            return results
        except Exception as e:
            logger.error(f"Database error: {e}")
            raise
        finally:
            await conn.close()

    async def update(self):
        new_data = await self.fetch_new_trades()
        if new_data:
            for row in new_data:
                self.data_queue.append(row)
                self.last_timestamp = max(self.last_timestamp, row['utc_time'])
        current_time = datetime.now(timezone.utc)
        while self.data_queue and (current_time - self.data_queue[0]['utc_time']).total_seconds() > self.window_seconds:
            self.data_queue.popleft()

    def get_current_window(self):
        return list(self.data_queue)

app = FastAPI()
tracker = PremiumTracker(window_seconds=30, roots=['SPXW'], conditions=None)

HTML_CONTENT = """
<!DOCTYPE html>
<html>
<head>
    <title>Premium Bar Charts with Bullish Gauge</title>
    <script src="https://code.highcharts.com/highcharts.js"></script>
    <script src="https://code.highcharts.com/highcharts-more.js"></script>
    <script src="https://code.highcharts.com/modules/solid-gauge.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <style>
        .chart-container {
            display: flex;
            justify-content: space-between;
            width: 90vw; /* Responsive width */
            margin: 20px auto;
        }
        #call-container, #put-container {
            height: 70vh; /* Responsive height */
            width: 45vw; /* Half of container for side-by-side */
        }
        #slider-container {
            text-align: center;
            margin: 20px;
        }
        #gauge-container {
            height: 40vh;
            width: 30vw;
            margin: 20px auto;
        }
        .dashboard {
            display: flex;
            flex-direction: column;
            align-items: center;
            background-color: #f8f8f8;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        .title {
            font-size: 24px;
            font-weight: bold;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <div class="dashboard">
        <div id="slider-container">
            <label for="bar-count">Number of Bars: <span id="bar-value">30</span></label>
            <input type="range" id="bar-count" min="10" max="60" value="30" step="1">
        </div>
        <div id="gauge-container"></div>
        <div class="chart-container">
            <div id="call-container"></div>
            <div id="put-container"></div>
        </div>
    </div>

    <script>
        // Call Chart
        const callChart = Highcharts.chart('call-container', {
            chart: { type: 'bar', animation: false },
            title: { text: 'Call Option Premiums' },
            xAxis: { title: { text: 'Seconds' } },
            yAxis: { title: { text: 'Premium' }, categories: [], reversed: false },
            plotOptions: { bar: { dataLabels: { enabled: true } } },
            series: [
                { name: 'Call Bought', data: [], color: '#00FF00' },
                { name: 'Call Sold', data: [], color: '#FF0000', negativeColor: '#FF0000' }
            ],
            legend: { enabled: true },
            responsive: {
                rules: [{
                    condition: { maxWidth: 500 },
                    chartOptions: { chart: { width: '100%' } }
                }]
            }
        });

        // Put Chart
        const putChart = Highcharts.chart('put-container', {
            chart: { type: 'bar', animation: false },
            title: { text: 'Put Option Premiums' },
            xAxis: { title: { text: 'Seconds' } },
            yAxis: { title: { text: 'Premium' }, categories: [], reversed: false },
            plotOptions: { bar: { dataLabels: { enabled: true } } },
            series: [
                { name: 'Put Bought', data: [], color: '#0000FF' },
                { name: 'Put Sold', data: [], color: '#FFA500', negativeColor: '#FFA500' }
            ],
            legend: { enabled: true },
            responsive: {
                rules: [{
                    condition: { maxWidth: 500 },
                    chartOptions: { chart: { width: '100%' } }
                }]
            }
        });

        // Define gauge options for the solidgauge style
        const gaugeOptions = {
            chart: {
                type: 'solidgauge'
            },
            title: {
                text: 'Bullish Premium Ratio',
                style: {
                    fontSize: '24px'
                }
            },
            pane: {
                center: ['50%', '85%'],
                size: '140%',
                startAngle: -90,
                endAngle: 90,
                background: {
                    backgroundColor: Highcharts.defaultOptions.legend.backgroundColor || '#fafafa',
                    borderRadius: 5,
                    innerRadius: '60%',
                    outerRadius: '100%',
                    shape: 'arc'
                }
            },
            exporting: {
                enabled: false
            },
            tooltip: {
                enabled: false
            },
            yAxis: {
                min: -20,
                max: +20,
                stops: [
                    [0.1, '#55BF3B'], // green - bullish (low values)
                    [0.5, '#DDDF0D'], // yellow - neutral (middle values)
                    [0.9, '#DF5353']  // red - bearish (high values)
                ],
                lineWidth: 0,
                tickWidth: 0,
                minorTickInterval: null,
                tickAmount: 2,
                title: {
                    y: -70,
                    text: 'Bullish Ratio',
                    style: {
                        fontWeight: 'bold'
                    }
                },
                labels: {
                    y: 16
                }
            },
            plotOptions: {
                solidgauge: {
                    borderRadius: 3,
                    dataLabels: {
                        y: 5,
                        borderWidth: 0,
                        useHTML: true
                    }
                }
            },
            credits: {
                enabled: false
            },
            series: [{
                name: 'Ratio',
                data: [0],
                dataLabels: {
                    format:
                        '<div style="text-align:center">' +
                        '<span style="font-size:25px">{y:.2f}</span><br/>' +
                        '<span style="font-size:12px;opacity:0.4">ratio</span>' +
                        '</div>'
                }
            }]
        };
        
        // Bullish Premium Ratio Gauge
        const gaugeChart = Highcharts.chart('gauge-container', gaugeOptions);

        // Data handling
        let maxBars = 30;
        const timestamps = [];
        const callData = { call_premium_bought: [], call_premium_sold: [] };
        const putData = { put_premium_bought: [], put_premium_sold: [] };
        
        // For rolling window of bullish premium ratio
        const ratioWindow = [];
        const ratioWindowSize = 5; // 10 second window

        // Function to calculate bullish premium ratio
        function calculateBullishPremiumRatio(callsBought, putsSold, putsBought, callsSold) {
            const numerator = (callsBought - putsSold) + (putsBought - callsSold);
            const denominator = (callsBought - putsSold);
            
            // Avoid division by zero
            if (denominator === 0) return 0;
            
            return numerator / denominator;
        }

        // Function to update ratio with a rolling window
        function updateRatioWindow(callsBought, putsSold, putsBought, callsSold, timestamp) {
            const ratio = calculateBullishPremiumRatio(callsBought, putsSold, putsBought, callsSold);
            
            // Add new data point
            ratioWindow.push({
                timestamp: new Date(timestamp).getTime(),
                ratio: ratio
            });
            
            // Remove data points older than window size (5 seconds)
            const now = new Date().getTime();
            const cutoffTime = now - (ratioWindowSize * 1000);
            while (ratioWindow.length > 0 && ratioWindow[0].timestamp < cutoffTime) {
                ratioWindow.shift();
            }
            
            // Calculate average ratio over the window
            if (ratioWindow.length === 0) return 0;
            
            const sum = ratioWindow.reduce((total, point) => total + point.ratio, 0);
            return sum / ratioWindow.length;
        }

        // WebSocket connection
        const ws = new WebSocket('ws://' + window.location.host + '/premium');
        ws.onopen = () => console.log('WebSocket connected');
        ws.onmessage = (event) => {
            try {
                const newData = JSON.parse(event.data);
                let latestCallsBought = 0;
                let latestPutsSold = 0;
                let latestPutsBought = 0;
                let latestCallsSold = 0;
                let latestTimestamp = null;

                newData.forEach(row => {
                    const time = row.time;
                    if (!timestamps.includes(time)) {
                        timestamps.unshift(time);
                        callData.call_premium_bought.unshift(row.call_premium_bought);
                        callData.call_premium_sold.unshift(-row.call_premium_sold); // Negative for sold
                        putData.put_premium_bought.unshift(row.put_premium_bought);
                        putData.put_premium_sold.unshift(-row.put_premium_sold); // Negative for sold

                        if (timestamps.length > maxBars) {
                            timestamps.pop();
                            callData.call_premium_bought.pop();
                            callData.call_premium_sold.pop();
                            putData.put_premium_bought.pop();
                            putData.put_premium_sold.pop();
                        }

                        // Get the latest values for ratio calculation
                        latestCallsBought = row.call_premium_bought;
                        latestPutsSold = row.put_premium_sold;
                        latestPutsBought = row.put_premium_bought;
                        latestCallsSold = row.call_premium_sold;
                        latestTimestamp = time;

                        callChart.yAxis[0].setCategories(timestamps);
                        callChart.series[0].setData(callData.call_premium_bought);
                        callChart.series[1].setData(callData.call_premium_sold);
                        putChart.yAxis[0].setCategories(timestamps);
                        putChart.series[0].setData(putData.put_premium_bought);
                        putChart.series[1].setData(putData.put_premium_sold);
                    }
                });

                // Update the gauge with latest values if we have new data
                if (latestTimestamp) {
                    const currentRatio = updateRatioWindow(
                        latestCallsBought,
                        latestPutsSold,
                        latestPutsBought,
                        latestCallsSold,
                        latestTimestamp
                    );
                    
                    // Update the gauge
                    if (gaugeChart && gaugeChart.series && gaugeChart.series[0]) {
                        const point = gaugeChart.series[0].points[0];
                        if (point) {
                            point.update(currentRatio);
                        }
                    }
                }
            } catch (e) {
                console.error(`Parse error: ${e.message}`);
            }
        };
        ws.onerror = (error) => console.error('WebSocket error:', error);
        ws.onclose = (event) => console.log(`WebSocket closed: code=${event.code}, reason=${event.reason}`);

        $('#bar-count').on('input', function() {
            maxBars = parseInt(this.value);
            $('#bar-value').text(maxBars);
            while (timestamps.length > maxBars) {
                timestamps.pop();
                callData.call_premium_bought.pop();
                callData.call_premium_sold.pop();
                putData.put_premium_bought.pop();
                putData.put_premium_sold.pop();
            }
            callChart.yAxis[0].setCategories(timestamps);
            callChart.series[0].setData(callData.call_premium_bought);
            callChart.series[1].setData(callData.call_premium_sold);
            putChart.yAxis[0].setCategories(timestamps);
            putChart.series[0].setData(putData.put_premium_bought);
            putChart.series[1].setData(putData.put_premium_sold);
        });
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return HTML_CONTENT

@app.websocket("/premium")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("WebSocket connection accepted")
    try:
        while True:
            await tracker.update()
            window_data = tracker.get_current_window()
            data = [
                {
                    "time": row["utc_time"].isoformat(),
                    "call_premium_bought": float(row["call_premium_bought"] or 0),
                    "call_premium_sold": float(row["call_premium_sold"] or 0),
                    "put_premium_bought": float(row["put_premium_bought"] or 0),
                    "put_premium_sold": float(row["put_premium_sold"] or 0)
                } for row in window_data
            ]
            await websocket.send_text(json.dumps(data))
            logger.info(f"Sent to WebSocket: {len(window_data)} seconds of data")
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        raise
    finally:
        logger.info("Closing WebSocket")
        await websocket.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
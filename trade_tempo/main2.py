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

# Import the HTML content from the artifact you created
# This is the HTML content for the new dashboard with VU gauges
HTML_CONTENT = """<!DOCTYPE html>
<html>
<head>
    <title>Options Premium Dashboard</title>
    <script src="https://code.highcharts.com/highcharts.js"></script>
    <script src="https://code.highcharts.com/highcharts-more.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
            margin: 0;
            padding: 20px;
        }

        .dashboard-container {
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            padding: 20px;
        }

        .title {
            text-align: center;
            font-size: 24px;
            margin-bottom: 20px;
            color: #333;
        }

        .gauges-container {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-around;
            margin-bottom: 20px;
        }

        .gauge {
            width: 300px;
            height: 240px;
            margin: 10px;
        }

        .charts-container {
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
        }

        .chart {
            width: 48%;
            height: 400px;
        }

        .data-table-container {
            width: 100%;
            overflow-x: auto;
            margin-top: 20px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }

        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: right;
        }

        th {
            background-color: #f2f2f2;
            position: sticky;
            top: 0;
        }

        tr:nth-child(even) {
            background-color: #f9f9f9;
        }

        tr:hover {
            background-color: #f1f7ff;
        }

        .timestamp-cell {
            text-align: left;
        }

        .window-control {
            text-align: center;
            margin: 20px 0;
        }

        .window-control label {
            margin-right: 10px;
        }
    </style>
</head>
<body>
    <div class="dashboard-container">
        <div class="title">Options Premium Analytics Dashboard</div>

        <div class="window-control">
            <label for="window-size">Rolling Window Size (seconds): <span id="window-value">10</span></label>
            <input type="range" id="window-size" min="1" max="30" value="10" step="1">
        </div>

        <div class="gauges-container">
            <div id="net-premium-gauge" class="gauge"></div>
            <div id="net-call-gauge" class="gauge"></div>
            <div id="net-put-gauge" class="gauge"></div>
            <div id="bullish-ratio-gauge" class="gauge"></div>
        </div>

        <div class="charts-container">
            <div id="call-container" class="chart"></div>
            <div id="put-container" class="chart"></div>
        </div>

        <div class="data-table-container">
            <h3>Premium Data & Calculations (Most Recent First)</h3>
            <table id="premium-table">
                <thead>
                    <tr>
                        <th class="timestamp-cell">Timestamp</th>
                        <th>Calls Bought</th>
                        <th>Calls Sold</th>
                        <th>Puts Bought</th>
                        <th>Puts Sold</th>
                        <th>Net Call</th>
                        <th>Net Put</th>
                        <th>Net Premium</th>
                        <th>Bullish Ratio</th>
                    </tr>
                </thead>
                <tbody id="premium-table-body">
                    <!-- Data rows will be inserted here -->
                </tbody>
            </table>
        </div>
    </div>

    <script>
        // Initialize data structures
        let windowSize = 10; // Default 10 second window
        const premiumData = [];
        const timestamps = [];
        const callData = { call_premium_bought: [], call_premium_sold: [] };
        const putData = { put_premium_bought: [], put_premium_sold: [] };

        // Create the gauge charts
        function createGauges() {
            // Common options for all gauges
            const gaugeBaseOptions = {
                chart: {
                    type: 'gauge',
                    plotBorderWidth: 1,
                    plotBackgroundColor: {
                        linearGradient: { x1: 0, y1: 0, x2: 0, y2: 1 },
                        stops: [
                            [0, '#FFF4C6'],
                            [0.3, '#FFFFFF'],
                            [1, '#FFF4C6']
                        ]
                    },
                    plotBackgroundImage: null,
                    height: 240
                },
                exporting: { enabled: false },
                tooltip: { enabled: false },
                plotOptions: {
                    gauge: {
                        dial: {
                            radius: '100%',
                            backgroundColor: 'black',
                            baseWidth: 4,
                            topWidth: 1,
                            baseLength: '75%',
                            rearLength: '15%'
                        },
                        dataLabels: {
                            enabled: true,
                            format: '{y:,.0f}',
                            style: { fontSize: '12px' }
                        },
                        pivot: {
                            backgroundColor: 'black',
                            radius: 6
                        }
                    }
                }
            };

            // Net Premium Gauge
            Highcharts.chart('net-premium-gauge', Highcharts.merge(gaugeBaseOptions, {
                title: { text: 'Net Premium (Calls - Puts)' },
                pane: {
                    startAngle: -45,
                    endAngle: 45,
                    background: null,
                    center: ['50%', '145%'],
                    size: 300
                },
                yAxis: {
                    min: -1000000,
                    max: 1000000,
                    tickPosition: 'outside',
                    minorTickPosition: 'outside',
                    labels: { distance: 20, rotation: 'auto' },
                    plotBands: [{
                        from: 0,
                        to: 1000000,
                        color: '#55BF3B', // green - bullish
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }, {
                        from: -1000000,
                        to: 0,
                        color: '#DF5353', // red - bearish
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }],
                    title: { text: 'Net Premium', y: -40 }
                },
                series: [{
                    name: 'Net Premium',
                    data: [0]
                }]
            }));

            // Net Call Premium Gauge
            Highcharts.chart('net-call-gauge', Highcharts.merge(gaugeBaseOptions, {
                title: { text: 'Net Call Premium (Bought - Sold)' },
                pane: {
                    startAngle: -45,
                    endAngle: 45,
                    background: null,
                    center: ['50%', '145%'],
                    size: 300
                },
                yAxis: {
                    min: -500000,
                    max: 500000,
                    tickPosition: 'outside',
                    minorTickPosition: 'outside',
                    labels: { distance: 20, rotation: 'auto' },
                    plotBands: [{
                        from: 0,
                        to: 500000,
                        color: '#55BF3B', // green - bullish
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }, {
                        from: -500000,
                        to: 0,
                        color: '#DF5353', // red - bearish
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }],
                    title: { text: 'Net Call Premium', y: -40 }
                },
                series: [{
                    name: 'Net Call Premium',
                    data: [0]
                }]
            }));

            // Net Put Premium Gauge
            Highcharts.chart('net-put-gauge', Highcharts.merge(gaugeBaseOptions, {
                title: { text: 'Net Put Premium (Bought - Sold)' },
                pane: {
                    startAngle: -45,
                    endAngle: 45,
                    background: null,
                    center: ['50%', '145%'],
                    size: 300
                },
                yAxis: {
                    min: -500000,
                    max: 500000,
                    tickPosition: 'outside',
                    minorTickPosition: 'outside',
                    labels: { distance: 20, rotation: 'auto' },
                    plotBands: [{
                        from: 0,
                        to: 500000,
                        color: '#DF5353', // red - bearish for puts
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }, {
                        from: -500000,
                        to: 0,
                        color: '#55BF3B', // green - bullish for puts
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }],
                    title: { text: 'Net Put Premium', y: -40 }
                },
                series: [{
                    name: 'Net Put Premium',
                    data: [0]
                }]
            }));

            // Bullish Ratio Gauge
            Highcharts.chart('bullish-ratio-gauge', Highcharts.merge(gaugeBaseOptions, {
                title: { text: 'Bullish Premium Ratio' },
                pane: {
                    startAngle: -45,
                    endAngle: 45,
                    background: null,
                    center: ['50%', '145%'],
                    size: 300
                },
                yAxis: {
                    min: 0,
                    max: 3,
                    tickPosition: 'outside',
                    minorTickPosition: 'outside',
                    labels: { distance: 20, rotation: 'auto' },
                    plotBands: [{
                        from: 0,
                        to: 1,
                        color: '#DF5353', // red - bearish (ratio < 1)
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }, {
                        from: 1,
                        to: 1.5,
                        color: '#DDDF0D', // yellow - neutral (ratio = 1)
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }, {
                        from: 1.5,
                        to: 3,
                        color: '#55BF3B', // green - bullish (ratio > 1)
                        innerRadius: '100%',
                        outerRadius: '105%'
                    }],
                    title: { text: 'Bullish Ratio', y: -40 }
                },
                series: [{
                    name: 'Bullish Ratio',
                    data: [0]
                }]
            }));
        }

        // Create the bar charts for calls and puts
        function createBarCharts() {
            // Call Chart
            Highcharts.chart('call-container', {
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
            });

            // Put Chart
            Highcharts.chart('put-container', {
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
            });
        }

        // Function to calculate all metrics
        function calculateMetrics(callsBought, callsSold, putsBought, putsSold) {
            const netCallPremium = callsBought - callsSold;
            const netPutPremium = putsBought - putsSold;

            // For net premium calculation:
            // Net premium = Net Call Premium + Put Premium Sold - Put Premium Bought
            // This treats put selling as bullish (adding to bullish sentiment)
            // and put buying as bearish (subtracting from bullish sentiment)
            const netPremium = netCallPremium + (putsSold - putsBought);

            // Calculate bullish ratio - a better representation of market sentiment
            // Bullish actions: Call buying and Put selling
            // Bearish actions: Call selling and Put buying
            const bullishActions = callsBought + putsSold;
            const bearishActions = callsSold + putsBought;

            // Avoid division by zero
            let bullishRatio = 0;
            if (bearishActions !== 0) {
                bullishRatio = bullishActions / bearishActions;
            }

            return {
                netCallPremium,
                netPutPremium,
                netPremium,
                bullishRatio
            };
        }

        // Calculate rolling window metrics
        function calculateRollingMetrics() {
            if (premiumData.length === 0) return { netCallPremium: 0, netPutPremium: 0, netPremium: 0, bullishRatio: 0 };

            // Slice to just use the window size
            const windowData = premiumData.slice(0, windowSize);

            let totalCallsBought = 0;
            let totalCallsSold = 0;
            let totalPutsBought = 0;
            let totalPutsSold = 0;

            windowData.forEach(data => {
                totalCallsBought += data.callsBought;
                totalCallsSold += data.callsSold;
                totalPutsBought += data.putsBought;
                totalPutsSold += data.putsSold;
            });

            return calculateMetrics(totalCallsBought, totalCallsSold, totalPutsBought, totalPutsSold);
        }

        // Update gauge with new data
        function updateGauges() {
            const metrics = calculateRollingMetrics();

            // Get all charts
            const charts = Highcharts.charts || [];

            // Update Net Premium Gauge
            const netPremiumGauge = charts.find(chart => chart && chart.renderTo.id === 'net-premium-gauge');
            if (netPremiumGauge && netPremiumGauge.series && netPremiumGauge.series[0]) {
                netPremiumGauge.series[0].points[0].update(metrics.netPremium);
            }

            // Update Net Call Premium Gauge
            const netCallGauge = charts.find(chart => chart && chart.renderTo.id === 'net-call-gauge');
            if (netCallGauge && netCallGauge.series && netCallGauge.series[0]) {
                netCallGauge.series[0].points[0].update(metrics.netCallPremium);
            }

            // Update Net Put Premium Gauge
            const netPutGauge = charts.find(chart => chart && chart.renderTo.id === 'net-put-gauge');
            if (netPutGauge && netPutGauge.series && netPutGauge.series[0]) {
                netPutGauge.series[0].points[0].update(metrics.netPutPremium);
            }

            // Update Bullish Ratio Gauge
            const bullishRatioGauge = charts.find(chart => chart && chart.renderTo.id === 'bullish-ratio-gauge');
            if (bullishRatioGauge && bullishRatioGauge.series && bullishRatioGauge.series[0]) {
                bullishRatioGauge.series[0].points[0].update(metrics.bullishRatio);
            }
        }

        // Update bar charts with new data
        function updateBarCharts() {
            // Get all charts
            const charts = Highcharts.charts || [];

            // Update Call Chart
            const callChart = charts.find(chart => chart && chart.renderTo.id === 'call-container');
            if (callChart) {
                callChart.yAxis[0].setCategories(timestamps);
                callChart.series[0].setData(callData.call_premium_bought);
                callChart.series[1].setData(callData.call_premium_sold);
            }

            // Update Put Chart
            const putChart = charts.find(chart => chart && chart.renderTo.id === 'put-container');
            if (putChart) {
                putChart.yAxis[0].setCategories(timestamps);
                putChart.series[0].setData(putData.put_premium_bought);
                putChart.series[1].setData(putData.put_premium_sold);
            }
        }

        // Update data table
        function updateDataTable() {
            const tableBody = document.getElementById('premium-table-body');
            tableBody.innerHTML = '';

            // Show the most recent data first (up to 20 rows to avoid overloading the page)
            const displayData = premiumData.slice(0, 20);

            displayData.forEach(data => {
                const row = document.createElement('tr');

                // Calculate metrics for each individual row
                const metrics = calculateMetrics(
                    data.callsBought, 
                    data.callsSold, 
                    data.putsBought, 
                    data.putsSold
                );

                // Format the timestamp for display
                const timestamp = new Date(data.timestamp);
                const formattedTime = timestamp.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit', second:'2-digit'});

                row.innerHTML = `
                    <td class="timestamp-cell">${formattedTime}</td>
                    <td>${data.callsBought.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${data.callsSold.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${data.putsBought.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${data.putsSold.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${metrics.netCallPremium.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${metrics.netPutPremium.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${metrics.netPremium.toLocaleString(undefined, {maximumFractionDigits: 0})}</td>
                    <td>${metrics.bullishRatio.toLocaleString(undefined, {maximumFractionDigits: 2})}</td>
                `;

                tableBody.appendChild(row);
            });
        }

        // Initialize WebSocket connection
        function initWebSocket() {
            const ws = new WebSocket('ws://' + window.location.host + '/premium');

            ws.onopen = () => console.log('WebSocket connected');

            ws.onmessage = (event) => {
                try {
                    const newData = JSON.parse(event.data);

                    newData.forEach(row => {
                        const time = row.time;
                        if (!timestamps.includes(time)) {
                            // Add to timestamps and chart data arrays
                            timestamps.unshift(time);
                            callData.call_premium_bought.unshift(row.call_premium_bought);
                            callData.call_premium_sold.unshift(-row.call_premium_sold); // Negative for sold
                            putData.put_premium_bought.unshift(row.put_premium_bought);
                            putData.put_premium_sold.unshift(-row.put_premium_sold); // Negative for sold

                            // Add to premium data array for calculations
                            premiumData.unshift({
                                timestamp: time,
                                callsBought: row.call_premium_bought,
                                callsSold: row.call_premium_sold,
                                putsBought: row.put_premium_bought,
                                putsSold: row.put_premium_sold
                            });

                            // Trim arrays to maintain max length
                            while (timestamps.length > 30) {
                                timestamps.pop();
                                callData.call_premium_bought.pop();
                                callData.call_premium_sold.pop();
                                putData.put_premium_bought.pop();
                                putData.put_premium_sold.pop();
                            }

                            // Trim premium data array too, but keep more history
                            while (premiumData.length > 100) {
                                premiumData.pop();
                            }

                            // Update all visualizations
                            updateBarCharts();
                            updateGauges();
                            updateDataTable();
                        }
                    });
                } catch (e) {
                    console.error(`Parse error: ${e.message}`);
                }
            };

            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
                setTimeout(initWebSocket, 3000); // Try to reconnect after 3 seconds
            };

            ws.onclose = (event) => {
                console.log(`WebSocket closed: code=${event.code}, reason=${event.reason}`);
                setTimeout(initWebSocket, 3000); // Try to reconnect after 3 seconds
            };

            return ws;
        }

        // Initialize everything when the document is ready
        $(document).ready(function() {
            createGauges();
            createBarCharts();
            initWebSocket();

            // Window size slider
            $('#window-size').on('input', function() {
                windowSize = parseInt(this.value);
                $('#window-value').text(windowSize);
                updateGauges(); // Recalculate metrics with new window size
            });
        });
    </script>
</body>
</html>"""


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
            await asyncio.sleep(1)  # 1 second update frequency
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        raise
    finally:
        logger.info("Closing WebSocket")
        await websocket.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
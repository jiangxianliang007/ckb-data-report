import requests
import json
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import plotly.io as pio
from discord_webhook import DiscordWebhook
import io
import pytz
import pandas as pd
from prometheus_api_client import PrometheusConnect

# Load environment variables from .env file
load_dotenv()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
PROMETHEUS_API_URL = os.getenv("PROMETHEUS_API_URL")
API_KEY = os.getenv("X_API_KEY")
if not DISCORD_WEBHOOK_URL:
    raise ValueError("DISCORD_WEBHOOK_URL not set in .env file")
if not PROMETHEUS_API_URL:
    raise ValueError("PROMETHEUS_API_URL not set in .env file")

# Set Beijing timezone
beijing_tz = pytz.timezone("Asia/Shanghai")

# API configuration for different data types
API_CONFIG = {
    "avg_hash_rate": {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/avg_hash_rate",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    "knowledge_size": {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/knowledge_size",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    "total_depositors_count_total_dao_deposit": {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/total_depositors_count-total_dao_deposit",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    "transactions_count": {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/transactions_count",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    "uncle_rate": {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/uncle_rate",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        },
        "params": {
        "limit": 30
        }
    },
    "redis_key_not_expired_total": {
        "prometheus_url": PROMETHEUS_API_URL,
        "query": "redis_key_not_expired_total"
    }
}

def fetch_data(endpoint_config):
    """Fetch data from the specified API endpoint"""
    if "headers" in endpoint_config:
        try:
            response = requests.get(endpoint_config["url"], headers=endpoint_config["headers"], params=endpoint_config.get("params"))
            response.raise_for_status()
            print(f"Fetched data from {endpoint_config['url']} - Status: {response.status_code}")
            return response.json()
        except requests.RequestException as e:
            print(f"Failed to fetch data from {endpoint_config['url']}: {e}")
            return None
    elif "prometheus_url" in endpoint_config:
        try:
            prom = PrometheusConnect(url=endpoint_config["prometheus_url"], disable_ssl=True)
            end_time = datetime.now(timezone.utc).astimezone(beijing_tz).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
            start_time = end_time - timedelta(days=29)  # 30 days back from yesterday
            print(f"Fetching Prometheus data from {start_time} to {end_time}")
            data = prom.custom_query_range(
                query=endpoint_config["query"],
                start_time=start_time,
                end_time=end_time,
                step="1d"
            )
            return data
        except Exception as e:
            print(f"Failed to fetch data from Prometheus {endpoint_config['prometheus_url']}: {e}")
            return None

def convert_hash_rate(hash_rate):
    """Convert hash rate from KH/s to PH/s"""
    try:
        return float(hash_rate) / 1_000_000_000_000  # Convert from KH/s to PH/s
    except (TypeError, ValueError):
        return None

def convert_to_million_ckbytes(value):
    """Convert Shannons to million CKBytes (M CKBytes)"""
    try:
        return float(value) / 100_000_000 / 1_000_000  # Shannons to CKBytes (10^8) to M (10^6)
    except (TypeError, ValueError):
        return None

def convert_to_billion_ckb(value):
    """Convert Shannons to billion CKB (B, 10^9 CKB)"""
    try:
        return float(value) / 100_000_000 / 1_000_000_000  # Shannons to CKB (10^8) to B (10^9)
    except (TypeError, ValueError):
        return None

def process_data(data, data_type):
    """Process data for the given data type, take last 30 points in Beijing time"""
    if not data or ("data" not in data and data_type != "redis_key_not_expired_total"):
        print(f"No data found for {data_type}: {data}")
        return [], []

    dates = []
    values = []

    if data_type in [
        "avg_hash_rate",
        "knowledge_size",
        "total_depositors_count_total_dao_deposit",
        "transactions_count",
        "uncle_rate"
    ]:
        for item in data["data"]:
            timestamp = int(item["attributes"]["created_at_unixtimestamp"])
            utc_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            beijing_dt = utc_dt.astimezone(beijing_tz)
            date = beijing_dt.date()
            if data_type == "avg_hash_rate":
                value = convert_hash_rate(item["attributes"]["avg_hash_rate"])
            elif data_type == "knowledge_size":
                value = convert_to_million_ckbytes(item["attributes"]["knowledge_size"])
            elif data_type == "total_depositors_count_total_dao_deposit":
                value = convert_to_billion_ckb(item["attributes"]["total_dao_deposit"])
            elif data_type == "transactions_count":
                value = int(item["attributes"]["transactions_count"])
            elif data_type == "uncle_rate":
                value = float(item["attributes"]["uncle_rate"]) * 100
            else:
                value = None
            if value is not None:
                dates.append(date)
                values.append(value)
        # Sort by date in descending order and take last 30 days
        sorted_pairs = sorted(zip(dates, values), key=lambda x: x[0], reverse=True)
        if len(sorted_pairs) > 30:
            sorted_pairs = sorted_pairs[:30]
        dates, values = zip(*sorted_pairs) if sorted_pairs else ([], [])
        print(f"Sample data for {data_type}: {list(zip(dates[:5], values[:5]))}")  # Debug: Print first 5 pairs
    elif data_type == "redis_key_not_expired_total":
        for series in data:
            for point in series["values"]:
                timestamp, value = point
                dt = datetime.fromtimestamp(float(timestamp), tz=timezone.utc).astimezone(beijing_tz)
                date = dt.date()
                if date not in dates:  # Take the first value of each day (closest to 0:00)
                    dates.append(date)
                    values.append(float(value) if value else None)
        # Sort by date and take last 30 days
        sorted_pairs = sorted(zip(dates, values), key=lambda x: x[0], reverse=True)
        if len(sorted_pairs) > 30:
            sorted_pairs = sorted_pairs[:30]
        dates, values = zip(*sorted_pairs) if sorted_pairs else ([], [])
        print(f"Sample data for {data_type}: {list(zip(dates[:5], values[:5]))}")  # Debug: Print first 5 pairs

    # Reverse to ascending order for display
    dates, values = list(dates)[::-1], list(values)[::-1]
    return dates, values

def create_chart(dates, values, title, yaxis_title):
    """Create a line chart using Plotly with data point labels and aligned ticks"""
    # Format values to 3 decimal places for labels (or integer for transactions_count)
    text_labels = [
        f"{value:.2f}%" if yaxis_title.lower().startswith("uncle") else
        (f"{value:.3f}" if isinstance(value, float) else str(value))
        for value in values
    ]

    # Generate continuous date range for X-axis ticks
    if dates:
        date_range = pd.date_range(start=dates[0], end=dates[-1], freq='D')
        tick_labels = [date.strftime("%Y-%m-%d") for date in date_range]

        # Align data with date range, fill missing values with None
        date_dict = dict(zip(dates, values))
        aligned_values = [date_dict.get(date.date(), None) for date in date_range]
    else:
        date_range = []
        tick_labels = []
        aligned_values = []

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=date_range,
            y=aligned_values,
            mode="lines+markers+text",
            name=yaxis_title,
            text=[label if value is not None else "" for label, value in zip(text_labels, aligned_values)],
            textposition="top center",
            textfont=dict(size=14),
            hovertemplate="Date: %{x}<br>Value: %{y:.3f}<extra></extra>" if any(v is not None and isinstance(v, float) for v in aligned_values) else "Date: %{x}<br>Value: %{y}<extra></extra>"
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=yaxis_title,
        template="plotly_dark",
        xaxis=dict(
            tickmode="array",
            tickvals=date_range,
            ticktext=tick_labels,
            tickformat="%Y-%m-%d"
        ),
        width=1600,
        height=800,
        title_font_size=20,
        xaxis_title_font_size=16,
        yaxis_title_font_size=16,
        margin=dict(t=50, b=50, l=50, r=50)
    )

    # Save chart as PNG bytes with higher resolution
    img_bytes = pio.to_image(fig, format="png", scale=3)
    return img_bytes

def send_to_discord(img_bytes, chart_name, content):
    """Send chart to Discord via webhook"""
    webhook = DiscordWebhook(
        url=DISCORD_WEBHOOK_URL,
        content=content,
        files={chart_name: io.BytesIO(img_bytes)}
    )

    try:
        response = webhook.execute()
        if response.status_code == 200:
            print(f"Chart {chart_name} sent to Discord successfully")
        else:
            print(f"Failed to send {chart_name} to Discord: {response.status_code}")
    except Exception as e:
        print(f"Error sending {chart_name} to Discord: {e}")

def main():
    """Main function to fetch data, create charts, and send to Discord"""
    # Process each data type
    for data_type, config in API_CONFIG.items():
        # Fetch and process data
        data = fetch_data(config)
        dates, values = process_data(data, data_type)
        print(f"Processed {data_type}: {len(dates)} dates, {len(values)} values")

        if not dates or not values:
            print(f"No data to chart for {data_type}")
            continue

        # Create chart
        if data_type == "avg_hash_rate":
            title = "Nervos Network Average Hash Rate (Last 30 Days)"
            yaxis_title = "Average Hash Rate (PH/s)"
            chart_name = "avg_hash_rate.png"
        elif data_type == "knowledge_size":
            title = "Nervos Network Knowledge Size (Last 30 Days)"
            yaxis_title = "Knowledge Size (M CKBytes)"
            chart_name = "knowledge_size.png"
        elif data_type == "total_depositors_count_total_dao_deposit":
            title = "Total Nervos DAO Deposit (Last 30 Days)"
            yaxis_title = "Nervos DAO Deposit (Billion CKB)"
            chart_name = "nervos_dao_deposit.png"
        elif data_type == "transactions_count":
            title = "Nervos Network Transactions Count (Last 30 Days)"
            yaxis_title = "Transactions Count"
            chart_name = "transactions_count.png"
        elif data_type == "redis_key_not_expired_total":
            title = "Nervos Network Active Node (Last 30 Days)"
            yaxis_title = "Number of active nodes"
            chart_name = "active_node.png"
        elif data_type == "uncle_rate":
            title = "Nervos Network Uncle Rate (Last 30 Days)"
            yaxis_title = "Uncle Rate (%)"
            chart_name = "uncle_rate.png"

        img_bytes = create_chart(dates, values, title, yaxis_title)
        send_to_discord(img_bytes, chart_name, content=title)

if __name__ == "__main__":
    main()

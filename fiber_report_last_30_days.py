import os
import requests
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pytz

# Function to get data from Fiber API
def get_fiber_data():
    url = "https://api-dashboard.fiber.channel/analysis"
    payload = {
        "range": "1M",
        "interval": "day",
        "fields": ["nodes", "channels", "capacity"],
        "net": "mainnet"
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

# Function to create charts
def create_charts(data):
    dates = [datetime.fromtimestamp(entry['timestamp']).strftime('%Y-%m-%d') for entry in data]
    nodes = [entry['nodes'] for entry in data]
    channels = [entry['channels'] for entry in data]
    capacity = [entry['capacity'] / 10**8 for entry in data]  # Convert to CKB

    # Total Active Nodes chart
    fig_nodes = go.Figure(data=[go.Bar(x=dates, y=nodes, name='Total Active Nodes')])
    fig_nodes.update_layout(title='TOTAL ACTIVE NODES', xaxis_title='Date', yaxis_title='Nodes')
    fig_nodes.write_image("total_active_nodes.png")

    # Total Channels chart
    fig_channels = go.Figure(data=[go.Bar(x=dates, y=channels, name='Total Channels (CKB)')])
    fig_channels.update_layout(title='TOTAL CHANNELS', xaxis_title='Date', yaxis_title='Channels')
    fig_channels.write_image("total_channels.png")

    # CKB Liquidity chart
    fig_capacity = go.Figure(data=[go.Bar(x=dates, y=capacity, name='CKB Liquidity (Total)')])
    fig_capacity.update_layout(title='CKB LIQUIDITY', xaxis_title='Date', yaxis_title='CKB')
    fig_capacity.write_image("ckb_liquidity.png")

# Function to send images to Discord
def send_to_discord(image_path):
    webhook_url = os.getenv('FIBER_DISCORD_WEBHOOK_URL')
    with open(image_path, 'rb') as image:
        requests.post(webhook_url, files={'file': image})

if __name__ == "__main__":
    data = get_fiber_data()
    create_charts(data)
    send_to_discord("total_active_nodes.png")
    send_to_discord("total_channels.png")
    send_to_discord("ckb_liquidity.png")

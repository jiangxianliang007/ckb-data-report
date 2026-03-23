import requests
import plotly.graph_objs as go

# Your existing code logic to get data from Fiber API

# Assuming `data` contains the API response with 'series' and 'points'

# Sample data structure assumption: 
# data = { 'series': { 'Nodes': [...], 'Channels': [...], 'Capacity': [...] }, 'points': [...] }

# Convert Capacity hex to int and then divide by 1e8
capacity_total = int(data['series']['Capacity'][0], 16) / 1e8

# Create plots
fig_nodes = go.Figure(data=[go.Bar(x=data['series']['Nodes'], y=data['points']['Nodes'])])
fig_channels = go.Figure(data=[go.Bar(x=data['series']['Channels'], y=data['points']['Channels'])])
fig_capacity = go.Figure(data=[go.Bar(x=['Total Capacity'], y=[capacity_total])])

# Export plots as PNG
fig_nodes.write_image('nodes.png')
fig_channels.write_image('channels.png')
fig_capacity.write_image('capacity.png')

# Send images via Discord webhook
webhook_url = 'YOUR_DISCORD_WEBHOOK_URL'

with open('nodes.png', 'rb') as img:
    requests.post(webhook_url, files={'file': img})
with open('channels.png', 'rb') as img:
    requests.post(webhook_url, files={'file': img})
with open('capacity.png', 'rb') as img:
    requests.post(webhook_url, files={'file': img})

# Make sure to handle the response from Discord to check if the images were sent correctly.

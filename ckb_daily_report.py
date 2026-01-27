import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from collections import Counter

# Load environment variables from .env file
load_dotenv()

# Get Discord Webhook URL and Prometheus API URL from environment variables
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
PROMETHEUS_API_URL = os.getenv("PROMETHEUS_API_URL")
API_KEY = os.getenv("X_API_KEY")
if not DISCORD_WEBHOOK_URL:
    raise ValueError("DISCORD_WEBHOOK_URL not set in .env file")
if not PROMETHEUS_API_URL:
    raise ValueError("PROMETHEUS_API_URL not set in .env file")

# API endpoints
urls = [
    {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/blocks/ckb_node_versions",
        "headers": {
            "Accept": "application/json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/addresses_count",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/total_tx_fee",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/distribution_data/miner_address_distribution",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/circulating_supply-liquidity",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    {
        "url": "https://mainnet-staging-api.explorer.app5.org/api/v1/daily_statistics/live_cells_count-dead_cells_count",
        "headers": {
            "Accept": "application/vnd.api+json",
            "Content-Type": "application/vnd.api+json",
            "X-API-Key": API_KEY
        }
    },
    {
        "url": "https://api-nodes.ckb.dev/",
        "headers": {"Accept": "application/json"}
    },
]

def fetch_data(url, headers):
    """Fetch data from a given API URL with specified headers"""
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None

def fetch_prometheus_data(query):
    """Fetch data from Prometheus API for a given query"""
    try:
        url = f"{PROMETHEUS_API_URL}/api/v1/query"
        params = {"query": query}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get("status") == "success" and data.get("data", {}).get("result"):
            result = data["data"]["result"]
            if result:
                return float(result[0]["value"][1])
        return None
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"Error fetching Prometheus data for query '{query}': {e}")
        return None

def convert_hash_rate(hash_rate):
    """Convert hash rate from H/s to TH/s"""
    try:
        return float(hash_rate) / 1_000_000_000_000  # Convert H/s to TH/s (10^12)
    except (TypeError, ValueError):
        return None

def convert_tx_fee(total_tx_fee):
    """Convert total transaction fee from Shannons to CKB"""
    try:
        return float(total_tx_fee) / 100_000_000  # Convert Shannons to CKB (10^8)
    except (TypeError, ValueError):
        return None

def convert_to_billion_ckb(value):
    """Convert Shannons to billion CKB (B, 10^9 CKB)"""
    try:
        return float(value) / 100_000_000 / 1_000_000_000  # Shannons to CKB (10^8) to B (10^9)
    except (TypeError, ValueError):
        return None

def convert_to_million(value):
    """Convert count to million (M)"""
    try:
        return float(value) / 1_000_000  # Convert to M (10^6)
    except (TypeError, ValueError):
        return None

def calculate_version_percentages(data):
    """Calculate percentage of blocks_count for each miner version, sorted by blocks_count descending"""
    if not data or 'data' not in data:
        return []
    total_blocks = sum(item['blocks_count'] for item in data['data'])
    if total_blocks == 0:
        return []
    versions = [
        {
            "version": item['version'],
            "blocks_count": item['blocks_count'],
            "percentage": (item['blocks_count'] / total_blocks) * 100
        }
        for item in data['data']
    ]
    versions.sort(key=lambda x: (-x['blocks_count'], x['version']))
    return versions

def calculate_miner_address_distribution(data):
    """Calculate blocks_count and percentage for each miner address"""
    if not data or 'data' not in data or 'attributes' not in data['data'] or 'miner_address_distribution' not in data['data']['attributes']:
        return []
    distribution = data['data']['attributes']['miner_address_distribution']
    total_blocks = sum(int(count) for count in distribution.values())
    if total_blocks == 0:
        return []
    return [
        {
            "address": address,
            "blocks_count": int(count),
            "percentage": (int(count) / total_blocks) * 100
        }
        for address, count in distribution.items()
    ]

def calculate_node_distribution(data):
    """Calculate total nodes and percentage for each version_short, sorted descending, with <10 nodes as 'other'"""
    if not data or not isinstance(data, list):
        return {'total_nodes': 0, 'versions': []}
    total_nodes = len(data)
    if total_nodes == 0:
        return {'total_nodes': 0, 'versions': []}

    # Count nodes by version_short
    version_counts = Counter(node['version_short'] for node in data if 'version_short' in node)

    # Separate versions with >=10 nodes and <10 nodes
    main_versions = []
    other_count = 0
    for version, count in version_counts.items():
        if count >= 10:
            main_versions.append({
                "version": version,
                "count": count,
                "percentage": (count / total_nodes) * 100
            })
        else:
            other_count += count

    # Add 'other' if any versions have <10 nodes
    if other_count > 0:
        main_versions.append({
            "version": "other",
            "count": other_count,
            "percentage": (other_count / total_nodes) * 100
        })

    # Sort by count (descending) and version (descending)
    main_versions.sort(key=lambda x: (-x['count'], x['version']))
    return {'total_nodes': total_nodes, 'versions': main_versions}

def get_unique_addresses(data):
    """Get the latest addresses_count from the data"""
    if not data or 'data' not in data or not data['data']:
        return 'N/A'
    latest_record = max(data['data'], key=lambda x: int(x['attributes']['created_at_unixtimestamp']))
    return latest_record['attributes']['addresses_count']

def get_total_tx_fee(data):
    """Get the latest total_tx_fee from the data"""
    if not data or 'data' not in data or not data['data']:
        return 'N/A'
    latest_record = max(data['data'], key=lambda x: int(x['attributes']['created_at_unixtimestamp']))
    total_tx_fee = latest_record['attributes']['total_tx_fee']
    return convert_tx_fee(total_tx_fee) or 'N/A'

def get_circulation_metrics(data):
    """Get the latest circulating_supply, liquidity, and Nervos DAO"""
    if not data or 'data' not in data or not data['data']:
        return {'circulating_supply': 'N/A', 'liquidity': 'N/A', 'nervos_dao': 'N/A'}
    latest_record = max(data['data'], key=lambda x: int(x['attributes']['created_at_unixtimestamp']))
    circulating_supply = latest_record['attributes']['circulating_supply']
    liquidity = latest_record['attributes']['liquidity']

    circulating_supply_b = convert_to_billion_ckb(circulating_supply)
    liquidity_b = convert_to_billion_ckb(liquidity)

    try:
        nervos_dao = float(circulating_supply) - float(liquidity)
        nervos_dao_b = convert_to_billion_ckb(nervos_dao)
    except (TypeError, ValueError):
        nervos_dao_b = 'N/A'

    return {
        'circulating_supply': circulating_supply_b if circulating_supply_b is not None else 'N/A',
        'liquidity': liquidity_b if liquidity_b is not None else 'N/A',
        'nervos_dao': nervos_dao_b if nervos_dao_b is not None else 'N/A'
    }

def get_cell_counts(data):
    """Get the latest live_cells_count and dead_cells_count"""
    if not data or 'data' not in data or not data['data']:
        return {'live_cells': 'N/A', 'dead_cells': 'N/A'}
    latest_record = max(data['data'], key=lambda x: int(x['attributes']['created_at_unixtimestamp']))
    live_cells = latest_record['attributes']['live_cells_count']
    dead_cells = latest_record['attributes']['dead_cells_count']

    live_cells_m = convert_to_million(live_cells)
    dead_cells_m = convert_to_million(dead_cells)

    return {
        'live_cells': live_cells_m if live_cells_m is not None else 'N/A',
        'dead_cells': dead_cells_m if dead_cells_m is not None else 'N/A'
    }

def get_mainnet_db_size():
    """Get the latest Mainnet DB size from Prometheus"""
    return fetch_prometheus_data('directory_size_gib{net="Mainnet"}') or 'N/A'

def split_message(content, max_length=1900):
    """Split long message into chunks under max_length"""
    lines = content.split('\n')
    chunks = []
    current_chunk = []
    current_length = 0

    for line in lines:
        line_length = len(line) + 1  # +1 for newline
        if current_length + line_length > max_length and current_chunk:
            chunks.append('\n'.join(current_chunk))
            current_chunk = [line]
            current_length = line_length
        else:
            current_chunk.append(line)
            current_length += line_length

    if current_chunk:
        chunks.append('\n'.join(current_chunk))

    return chunks

def generate_daily_report(versions_data, addresses_data, tx_fee_data, miner_address_data, circulation_data, cell_counts_data, nodes_data):
    """Generate a Markdown report from the fetched data"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    report = f"""# Nervos CKB Daily Report ({current_date})

**Data Source**: Explorer API

## Miner Versions Distribution (Last 7 Days)
"""
    # Process miner versions API data
    if not versions_data:
        report += "- Error: Unable to fetch miner versions data.\n"
    else:
        versions = calculate_version_percentages(versions_data)
        if not versions:
            report += "- No version data available.\n"
        else:
            for version in versions:
                report += f"- **Version {version['version']}**: {version['blocks_count']} blocks ({version['percentage']:.2f} %)\n"

    # Process miner address distribution API data
    report += "\n## Miner Address Distribution (Last 7 Days)\n"
    if not miner_address_data:
        report += "- Error: Unable to fetch miner address distribution data.\n"
    else:
        addresses = calculate_miner_address_distribution(miner_address_data)
        if not addresses:
            report += "- No miner address data available.\n"
        else:
            for addr in addresses:
                report += f"- **Address {addr['address']}**: {addr['blocks_count']} blocks ({addr['percentage']:.2f} %)\n"

    # Process unique addresses API data
    report += "\n## Unique Addresses\n"
    addresses_count = get_unique_addresses(addresses_data)
    report += f"- **Unique addresses used**: {addresses_count}\n"

    total_tx_fee = get_total_tx_fee(tx_fee_data)
    report += f"- **Daily transaction fees**: {total_tx_fee:.2f} CKB\n" if total_tx_fee != 'N/A' else f"- **Daily transaction fees**: {total_tx_fee}\n"

    # Process circulation
    report += "\n## Circulation\n"
    circulation_metrics = get_circulation_metrics(circulation_data)
    report += f"- **Circulating Supply**: {circulation_metrics['circulating_supply']:.4f} CKB (B)\n" if circulation_metrics['circulating_supply'] != 'N/A' else f"- **Circulating Supply**: {circulation_metrics['circulating_supply']}\n"
    report += f"- **Liquidity**: {circulation_metrics['liquidity']:.4f} CKB (B)\n" if circulation_metrics['liquidity'] != 'N/A' else f"- **Liquidity**: {circulation_metrics['liquidity']}\n"
    report += f"- **Nervos DAO**: {circulation_metrics['nervos_dao']:.4f} CKB (B)\n" if circulation_metrics['nervos_dao'] != 'N/A' else f"- **Nervos DAO**: {circulation_metrics['nervos_dao']}\n"

    # Process Mainnet DB size
    report += "\n## Mainnet DB Size\n"
    mainnet_db_size = get_mainnet_db_size()
    report += f"- **Mainnet DB Size**: {mainnet_db_size:.2f} GiB\n" if mainnet_db_size != 'N/A' else f"- **Mainnet DB Size**: {mainnet_db_size}\n"

    # Process live and dead cell counts
    report += "\n## Live & Dead Cell Counts\n"
    cell_counts = get_cell_counts(cell_counts_data)
    report += f"- **Live Cells**: {cell_counts['live_cells']:.6f} M\n" if cell_counts['live_cells'] != 'N/A' else f"- **Live Cells**: {cell_counts['live_cells']}\n"
    report += f"- **Dead Cells**: {cell_counts['dead_cells']:.6f} M\n" if cell_counts['dead_cells'] != 'N/A' else f"- **Dead Cells**: {cell_counts['dead_cells']}\n"

    # Process active nodes
    report += "\n## CKB Active Nodes\n"
    nodes_info = calculate_node_distribution(nodes_data)
    report += f"- **Total Nodes**: {nodes_info['total_nodes']}\n"
    if not nodes_info['versions']:
        report += "- No node version data available.\n"
    else:
        for version in nodes_info['versions']:
            report += f"- **Version {version['version']}**: {version['count']} nodes ({version['percentage']:.2f} %)\n"

    return report

def save_report(report_content, filename="ckb_daily_report.md"):
    """Save the report to a file"""
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(report_content)
    print(f"Report saved to {filename}")

def send_to_discord(report_content):
    """Send the report to a Discord channel via Webhook, handling long messages"""
    try:
        # Split content if too long
        chunks = split_message(report_content, max_length=1900)

        for i, chunk in enumerate(chunks):
            payload = {
                "content": chunk
            }
            response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
            try:
                response.raise_for_status()
                print(f"Report chunk {i+1}/{len(chunks)} successfully sent to Discord")
            except requests.HTTPError as e:
                print(f"Error sending report chunk {i+1}/{len(chunks)} to Discord: {e}, Response: {response.text}")
    except requests.RequestException as e:
        print(f"Error sending report to Discord: {e}")

def main():
    # Fetch data from APIs
    versions_data = fetch_data(urls[0]["url"], urls[0]["headers"])
    addresses_data = fetch_data(urls[1]["url"], urls[1]["headers"])
    tx_fee_data = fetch_data(urls[2]["url"], urls[2]["headers"])
    miner_address_data = fetch_data(urls[3]["url"], urls[3]["headers"])
    circulation_data = fetch_data(urls[4]["url"], urls[4]["headers"])
    cell_counts_data = fetch_data(urls[5]["url"], urls[5]["headers"])
    nodes_data = fetch_data(urls[6]["url"], urls[6]["headers"])

    # Generate and save the report
    report = generate_daily_report(versions_data, addresses_data, tx_fee_data, miner_address_data, circulation_data, cell_counts_data, nodes_data)
    save_report(report)

    # Send report to Discord
    send_to_discord(report)

if __name__ == "__main__":
    main()

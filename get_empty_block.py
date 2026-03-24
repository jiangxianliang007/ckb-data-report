import os

import requests
import discord
import pandas as pd
from dotenv import load_dotenv


# Get the latest block number from CKB RPC
def get_latest_block_number() -> int | None:
    rpc_url = "https://mainnet.ckb.dev"
    payload = {
        "id": 2,
        "jsonrpc": "2.0",
        "method": "get_tip_block_number",
        "params": [],
    }
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }

    try:
        response = requests.post(rpc_url, json=payload, headers=headers, timeout=30)
    except Exception as e:
        print(f"Failed to fetch latest block number: {e}")
        return None

    if response.status_code != 200:
        print(f"Failed to fetch latest block number, status code: {response.status_code}")
        print(response.text)
        return None

    # RPC returns hex string, e.g. "0x1234"
    return int(response.json()["result"], 16)


# Download blocks CSV from explorer API
def download_blocks_csv(start_block: int, end_block: int, file_name: str = "exported_transactions.csv") -> bool:
    download_url = (
        "https://mainnet-api.explorer.nervos.org/api/v1/blocks/download_csv"
        f"?start_number={start_block}&end_number={end_block}"
    )

    headers = {
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }

    try:
        response = requests.get(download_url, headers=headers, timeout=120)
    except Exception as e:
        print(f"CSV download failed: {e}")
        return False

    if response.status_code != 200:
        print(f"CSV download failed, status code: {response.status_code}")
        print(response.text)
        return False

    with open(file_name, "wb") as f:
        f.write(response.content)

    print(f"CSV downloaded and saved as {file_name}")
    return True


# Analyze CSV and send a message to Discord
async def analyze_blocks_and_send_message(
    client: discord.Client,
    csv_file: str,
    channel_id: str,
    latest_block: int,
) -> None:
    df = pd.read_csv(csv_file)

    # Total blocks mined by each miner
    miner_block_count = df["Miner"].value_counts()

    # Blocks that contain exactly 1 transaction, mined by each miner
    one_transaction_blocks = df[df["Transactions"] == 1]["Miner"].value_counts()

    # Merge stats
    miner_stats = pd.DataFrame(
        {
            "Miner": miner_block_count.index,
            "Total Blocks Mined": miner_block_count.values,
            "Blocks with One Transaction": one_transaction_blocks.reindex(miner_block_count.index, fill_value=0).values,
        }
    )

    # Ratio in percentage
    miner_stats["Transaction/Blocks Ratio"] = (
        miner_stats["Blocks with One Transaction"] / miner_stats["Total Blocks Mined"]
    ) * 100

    # Build message content
    message_content = ""
    for _, row in miner_stats.iterrows():
        message_content += (
            f"Miner: {row['Miner']}\n"
            f"Total Blocks Mined: {row['Total Blocks Mined']}\n"
            f"Blocks with One Transaction: {row['Blocks with One Transaction']}\n"
            f"Blocks Ratio: {row['Transaction/Blocks Ratio']:.2f}%\n"
            "----\n"
        )

    print(message_content)

    # Get channel (fallback to fetch if not in cache)
    channel = client.get_channel(int(channel_id))
    if channel is None:
        channel = await client.fetch_channel(int(channel_id))

    await channel.send(
        f"LatestBlock: {latest_block}\n"
        "Explorer: https://explorer.nervos.org/\n"
        f"```{message_content}```"
    )


def main() -> None:
    load_dotenv()
    discord_bot_channel = os.getenv("CHANNEL_ID")
    discord_bot_token = os.getenv("DISCORD_TOKEN")

    if not discord_bot_channel or not discord_bot_token:
        raise RuntimeError(
            "Missing env vars: CHANNEL_ID and/or DISCORD_TOKEN. "+
            "Please set them in your environment or in a .env file."
        )

    latest_block = get_latest_block_number()
    if latest_block is None:
        raise RuntimeError("Could not fetch latest block number.")

    print(f"Latest block number: {latest_block}")

    start_block = max(latest_block - 5000, 0)
    print(f"Start block number (latest - 5000): {start_block}")

    csv_file = "exported_transactions.csv"
    if not download_blocks_csv(start_block, latest_block, csv_file):
        raise RuntimeError("CSV download failed; aborting.")

    intents = discord.Intents.default()
    client = discord.Client(intents=intents)

    @client.event
    async def on_ready():
        print(f"Logged in as {client.user}")
        await analyze_blocks_and_send_message(client, csv_file, discord_bot_channel, latest_block)
        await client.close()

    client.run(discord_bot_token)


if __name__ == "__main__":
    main()
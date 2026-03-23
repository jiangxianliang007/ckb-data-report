import io
import os
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import requests
from discord_webhook import DiscordWebhook
from dotenv import load_dotenv

FIBER_API_URL = "https://api-dashboard.fiber.channel/analysis"

def _must_getenv(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise ValueError(f"{key} not set in .env")
    return v

def fetch_fiber_analysis():
    payload = {
        "range": "1M",
        "interval": "day",
        "fields": ["nodes", "channels", "capacity"],
        "net": "mainnet",
    }
    resp = requests.post(
        FIBER_API_URL,
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

def pick_series(data, name: str) -> dict:
    for s in data.get("series", []):
        if s.get("name") == name:
            return s
    raise KeyError(f"Series '{name}' not found in response")

def parse_nodes_series(series_nodes: dict) -> pd.DataFrame:
    rows = [(d, v) for d, v in series_nodes.get("points", [])]
    df = pd.DataFrame(rows, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df

def parse_channels_series(series_channels: dict) -> pd.DataFrame:
    rows = []
    for d, obj in series_channels.get("points", []):
        v = None
        if isinstance(obj, dict):
            v = obj.get("ckb")
        rows.append((d, v))
    df = pd.DataFrame(rows, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df

def parse_capacity_series(series_capacity: dict) -> pd.DataFrame:
    rows = []
    for d, arr in series_capacity.get("points", []):
        total_hex = None
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict) and item.get("name") == "ckb":
                    total_hex = item.get("total")
                    break

        v_ckb = None
        if isinstance(total_hex, str) and total_hex.startswith("0x"):
            # capacity total is in shannons, convert to CKB
            v_ckb = int(total_hex, 16) / 1e8

        rows.append((d, v_ckb))

    df = pd.DataFrame(rows, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df

def create_bar_chart(df: pd.DataFrame, title: str, yaxis_title: str) -> bytes:
    if df.empty:
        raise ValueError(f"No data for chart: {title}")

    date_range = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="D")
    date_to_value = dict(zip(df["date"].dt.date, df["value"]))
    aligned_values = [date_to_value.get(d.date(), None) for d in date_range]
    tick_labels = [d.strftime("%Y-%m-%d") for d in date_range]

    def _fmt(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""

        # Liquidity is large; show with 2 decimals and commas.
        if yaxis_title.strip().upper() == "CKB":
            return f"{float(v):,.2f}"

        # Nodes / Channels: prefer integer display.
        try:
            fv = float(v)
            if fv.is_integer():
                return str(int(fv))
            return f"{fv:.2f}"
        except Exception:
            return str(v)

    text_labels = [_fmt(v) for v in aligned_values]

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=date_range,
            y=aligned_values,
            name=yaxis_title,
            text=text_labels,
            textposition="outside",
            cliponaxis=False,
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
            tickformat="%Y-%m-%d",
        ),
        width=1600,
        height=900,
        margin=dict(t=80, b=60, l=60, r=60),
    )

    ys = [
        v
        for v in aligned_values
        if v is not None and not (isinstance(v, float) and pd.isna(v))
    ]
    if ys:
        y_max = max(ys)
        fig.update_yaxes(range=[0, y_max * 1.15])

    return pio.to_image(fig, format="png", scale=3)

def send_to_discord_image(webhook_url: str, content: str, filename: str, img_bytes: bytes):
    webhook = DiscordWebhook(url=webhook_url, content=content)
    webhook.add_file(file=io.BytesIO(img_bytes).read(), filename=filename)

    resp = webhook.execute()
    if getattr(resp, "status_code", None) not in (200, 204):
        raise RuntimeError(
            f"Discord webhook failed: {getattr(resp, 'status_code', None)} {getattr(resp, 'text', '')}"
        )

def main():
    load_dotenv()

    webhook_url = _must_getenv("FIBER_DISCORD_WEBHOOK_URL")

    data = fetch_fiber_analysis()

    nodes_df = parse_nodes_series(pick_series(data, "Nodes"))
    channels_df = parse_channels_series(pick_series(data, "Channels"))
    capacity_df = parse_capacity_series(pick_series(data, "Capacity"))

    today = datetime.utcnow().strftime("%Y-%m-%d")

    chart_specs = [
        {
            "df": nodes_df,
            "title": "TOTAL ACTIVE NODES (Last 30 Days)",
            "yaxis": "Nodes",
            "filename": "fiber_total_active_nodes.png",
        },
        {
            "df": channels_df,
            "title": "TOTAL CHANNELS (Last 30 Days)",
            "yaxis": "Channels (CKB)",
            "filename": "fiber_total_channels.png",
        },
        {
            "df": capacity_df,
            "title": "CKB LIQUIDITY (Last 30 Days)",
            "yaxis": "CKB",
            "filename": "fiber_ckb_liquidity.png",
        },
    ]

    for spec in chart_specs:
        img_bytes = create_bar_chart(spec["df"], spec["title"], spec["yaxis"])
        content = f"{spec['title']} • Generated at {today}"
        send_to_discord_image(
            webhook_url=webhook_url,
            content=content,
            filename=spec["filename"],
            img_bytes=img_bytes,
        )

    print("Fiber report sent to Discord successfully.")


if __name__ == "__main__":
    main()
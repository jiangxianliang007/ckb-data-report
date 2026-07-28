"""
从 Notion 页面拉取各国节假日表格，过滤出「下个月」有节假日的国家/地区，推送到 Discord。

存储方式: B —— 一个 Notion 页面里，每个国家一个 heading + 一个 table block。
区间规则: 方案② —— 节假日日期区间只要与下个月有交集就纳入。

执行时机由外部计划任务（cron / Actions）控制，本脚本不做月末判断。
"""

import os
import re
import datetime
import requests
from dotenv import load_dotenv

load_dotenv()  # 自动读取当前目录下的 .env 文件（CI 环境无 .env 时静默跳过）

# ----------------- 配置 -----------------
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_PAGE_ID = os.environ["NOTION_PAGE_ID"]
DISCORD_WEBHOOK = os.environ["DISCORD_WEBHOOK"]

# 通知期号年份（用于 “Administration Notice [YYYY]”），默认取下个月的年份
NOTICE_YEAR = None

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
}

MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}

# 列名（按需改成你表格里的真实表头）
COL_NAME = "Holiday Name"
COL_DATE_PREFIX = "Dates"      # 匹配 "Dates(2026)" 这种，自动提取年份
COL_DURATION = "Duration"
COL_REMARKS = "Remarks"


# ----------------- Notion 拉取 -----------------
def get_children(block_id):
    """取某 block/page 的所有子 block（自动翻页）。"""
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    results, cursor = [], None
    while True:
        params = {"page_size": 100}
        if cursor:
            params["start_cursor"] = cursor
        r = requests.get(url, headers=NOTION_HEADERS, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        results.extend(data["results"])
        if not data.get("has_more"):
            break
        cursor = data["next_cursor"]
    return results


def rich_text_to_plain(cell):
    return "".join(rt.get("plain_text", "") for rt in cell).strip()


def split_flag(raw):
    """'🇯🇵 Japan' -> ('🇯🇵', 'Japan')。"""
    flag = []
    i = 0
    while i < len(raw) and 0x1F1E6 <= ord(raw[i]) <= 0x1F1FF:
        flag.append(raw[i])
        i += 1
    return ("".join(flag).strip() or None, raw[i:].strip())


def parse_page_tables(page_id):
    """
    遍历页面 blocks：heading = 当前国家，table = 该国节假日表。
    返回 [ {"flag":.., "name":.., "header":[...], "rows":[dict,...]}, ... ]，保留页面顺序。
    """
    countries = []
    current = None

    for block in get_children(page_id):
        btype = block["type"]

        if btype.startswith("heading_"):
            text = "".join(rt.get("plain_text", "")
                           for rt in block[btype]["rich_text"]).strip()
            if not text:
                continue
            flag, name = split_flag(text)
            current = {"flag": flag, "name": name, "header": [], "rows": []}
            countries.append(current)

        elif btype == "table" and current is not None:
            table_rows = get_children(block["id"])
            matrix = [
                [rich_text_to_plain(c) for c in r["table_row"]["cells"]]
                for r in table_rows if r["type"] == "table_row"
            ]
            if not matrix:
                continue
            header, *body = matrix
            current["header"] = header
            current["rows"] = [dict(zip(header, row)) for row in body]

    return countries


# ----------------- 日期解析 -----------------
def find_date_column(header):
    """在表头里找以 'Dates' 开头的列，返回列名和年份（从括号里提取）。"""
    for h in header:
        if h.strip().lower().startswith(COL_DATE_PREFIX.lower()):
            m = re.search(r"(\d{4})", h)
            year = int(m.group(1)) if m else datetime.date.today().year
            return h, year
    return None, datetime.date.today().year


def parse_date_range(text, year):
    """'Jan 1' / 'Feb 15 - Feb 23' / 'Feb 15 - 23' -> (start_date, end_date) 或 None。"""
    text = (text or "").strip()
    if not text:
        return None
    parts = re.split(r"\s*[-–—]\s*", text)

    def one(s, fallback_month=None):
        m = re.search(r"([A-Za-z]{3})\s+(\d{1,2})", s)
        if m and m.group(1) in MONTHS:
            mo = MONTHS[m.group(1)]
            return datetime.date(year, mo, int(m.group(2))), mo
        m2 = re.match(r"^\s*(\d{1,2})\s*$", s)
        if m2 and fallback_month:
            return datetime.date(year, fallback_month, int(m2.group(1))), fallback_month
        return None, None

    start, smonth = one(parts[0])
    if start is None:
        return None
    if len(parts) == 1:
        return (start, start)
    end, _ = one(parts[1], fallback_month=smonth)
    return (start, end or start)


def month_bounds(year, month):
    first = datetime.date(year, month, 1)
    nxt_y = year + (1 if month == 12 else 0)
    nxt_m = 1 if month == 12 else month + 1
    last = datetime.date(nxt_y, nxt_m, 1) - datetime.timedelta(days=1)
    return first, last


def overlaps(rng, m_first, m_last):
    """方案②：区间与目标月有交集。"""
    start, end = rng
    return start <= m_last and end >= m_first


# ----------------- 过滤 & 渲染 -----------------
def build_filtered(countries, target_year, target_month):
    m_first, m_last = month_bounds(target_year, target_month)
    out = []
    for c in countries:
        date_col, col_year = find_date_column(c["header"])
        if not date_col:
            continue
        picked = []
        for row in c["rows"]:
            rng = parse_date_range(row.get(date_col, ""), col_year)
            if rng and overlaps(rng, m_first, m_last):
                picked.append((rng[0], row, date_col))
        if picked:
            picked.sort(key=lambda x: x[0])
            out.append({"flag": c["flag"], "name": c["name"], "rows": picked})
    return out


def render_message(filtered, target_year, target_month, today=None):
    """生成完整通知：抬头引言 + 各国节假日 + 结尾 Notes + 落款。"""
    today = today or datetime.date.today()
    month_name = datetime.date(target_year, target_month, 1).strftime("%B %Y")
    notice_year = NOTICE_YEAR or target_year

    lines = []

    # ---- 抬头 ----
    lines.append(f"# 📢 Administration Notice [{notice_year}]")
    lines.append("")
    lines.append(f"## {month_name} Public Holiday Reference (Global)")
    lines.append("")
    lines.append("Dear team,")
    lines.append("")
    lines.append(f"Below is a reference of public holidays across different "
                 f"regions for {month_name}.")
    lines.append("As a distributed team, we keep things flexible across regions "
                 "and time zones. The information below is shared for general "
                 "awareness.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- 正文：各国节假日 ----
    if not filtered:
        lines.append("No public holidays next month across tracked regions.")
    else:
        for c in filtered:
            flag = f"{c['flag']} " if c["flag"] else ""
            lines.append(f"### {flag}{c['name']}")
            for _, row, date_col in c["rows"]:
                name = row.get(COL_NAME, "").strip()
                date = row.get(date_col, "").strip()
                dur = row.get(COL_DURATION, "").strip()
                remarks = row.get(COL_REMARKS, "").strip()
                extra = " — ".join(x for x in [dur, remarks] if x)
                extra = f" — {extra}" if extra else ""
                lines.append(f"- {date}: {name}{extra}")
            lines.append("")

    # ---- 结尾 Notes ----
    lines.append("---")
    lines.append("")
    lines.append("## 🗒️ Notes")
    lines.append("")
    lines.append("- Please arrange your work accordingly ahead of the holidays.")
    lines.append("- Stay reachable during holidays if urgent matters arise.")
    lines.append("- For countries with **federal, state, provincial, territorial, "
                 "or canton-based holiday systems** (e.g. Australia, Canada, "
                 "Malaysia, Switzerland, and the United States), additional public "
                 "holidays or observances may apply depending on your local "
                 "jurisdiction. Please refer to your local arrangements where "
                 "applicable.")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ---- 落款（时间用脚本运行当天）----
    lines.append("Operations Team")
    lines.append("")
    lines.append(today.strftime("%B %-d, %Y"))

    return "\n".join(lines)


# ----------------- Discord 发送 -----------------
def send_discord(content):
    """按 2000 字符限制，尽量按段落切分后发送。"""
    chunks, buf = [], ""
    for line in content.split("\n"):
        if line.startswith(("## ", "### ", "---")) and len(buf) > 1500:
            chunks.append(buf)
            buf = ""
        if len(buf) + len(line) + 1 > 1900:
            chunks.append(buf)
            buf = ""
        buf += line + "\n"
    if buf.strip():
        chunks.append(buf)

    for chunk in chunks:
        r = requests.post(DISCORD_WEBHOOK, json={"content": chunk}, timeout=30)
        r.raise_for_status()


# ----------------- 主流程 -----------------
def next_month(day):
    y = day.year + (1 if day.month == 12 else 0)
    m = 1 if day.month == 12 else day.month + 1
    return y, m


def main():
    today = datetime.date.today()
    target_year, target_month = next_month(today)
    print(f"[run] 生成 {target_year}-{target_month:02d} 的节假日通知")

    countries = parse_page_tables(NOTION_PAGE_ID)
    filtered = build_filtered(countries, target_year, target_month)
    message = render_message(filtered, target_year, target_month, today=today)

    print("---- 预览 ----")
    print(message)
    print("--------------")

    send_discord(message)
    print("[done] 已发送到 Discord")


if __name__ == "__main__":
    main()

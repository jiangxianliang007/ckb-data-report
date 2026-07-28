# Monthly Holiday Notifier (Notion → Discord)

从 Notion 页面拉取各国节假日表格，把**下个月**有节假日的国家/地区推送到 Discord。

执行时机由外部计划任务（cron / GitHub Actions schedule / 系统定时任务）控制，本脚本本身不做月末判断。

## 工作原理

1. 读取一个 Notion 页面，页面里每个国家是一个 `heading` + 一个 `table` block（存储方式 B）。
2. 解析每张表的 `Dates(YYYY)` 列，支持单日 `Jan 1` 和区间 `Feb 15 - Feb 23`。
3. **区间只要与下个月有交集就纳入**（方案②，跨月节假日也会提醒）。
4. 生成 Markdown 消息，按 2000 字符限制分段发到 Discord Webhook。

## 准备工作

### 1. Notion Integration
- 创建 Integration，拿到 `NOTION_TOKEN`。
- 把目标页面 **Share → 邀请该 Integration**，否则 API 读不到。
- 页面 URL 末尾 32 位十六进制即 `NOTION_PAGE_ID`。

### 2. Discord Webhook
- 频道设置 → 整合 → 创建 Webhook，复制 URL 作为 `DISCORD_WEBHOOK`。

### 3. 环境变量
本地复制 `.env.example` 为 `.env` 并填入真实值：

```bash
cp .env.example .env
```

`.env` 已在 `.gitignore` 中忽略，不会提交。CI 环境可直接用 Secrets 注入同名环境变量。

## 本地运行

```bash
pip install -r requirements.txt
python notify_holidays.py
```

## 定时执行（每月最后一天）

cron 无法直接表达“每月最后一天”，用“明天是 1 号”判断法：

```bash
# 每天 23:00 检查，若明天是 1 号（即今天是月末）才执行
0 23 * * * [ "$(date -d tomorrow +\%d)" = "01" ] && /usr/bin/python3 /path/holiday_notify/notify_holidays.py
```

或者固定某一天（简单粗暴）：

```bash
# 每月 28 号执行
0 23 28 * * /usr/bin/python3 /path/holiday_notify/notify_holidays.py
```

## 表头假设

脚本默认列名如下，如与实际不同，改 `notify_holidays.py` 顶部常量即可：

| 常量 | 默认值 |
|------|--------|
| `COL_NAME` | `Holiday Name` |
| `COL_DATE_PREFIX` | `Dates`（匹配 `Dates(2026)` 并自动提取年份） |
| `COL_DURATION` | `Duration` |
| `COL_REMARKS` | `Remarks` |

# CKB TxPool Diagnostics / 交易池故障诊断

用于排查 CKB Testnet/Mainnet 节点 Pending 积压、交易依赖结构、手续费集中、矿工不 Proposal 等问题。默认检查本机 `http://127.0.0.1:8114`。

## 一键执行

在具有 Python 3 的 CKB 节点（或能访问该 RPC 的运维机）执行：

```bash
python3 txpool/diagnostics/report.py --rpc-url http://127.0.0.1:8114
```

数据量较大时，扫描根交易的 `get_pool_tx_detail_info` 会产生多批只读 RPC（每批 20 条），因此可能耗时数分钟。指定输出目录：

```bash
python3 txpool/diagnostics/report.py --rpc-url http://127.0.0.1:8114 --output-dir /tmp/ckb-txpool-report
```

快速模式（跳过根后代详情和交易结构样本）：

```bash
python3 txpool/diagnostics/report.py --skip-details --skip-samples
```

**离线模式**：如果现场已经清理，用之前保存的 `get_raw_tx_pool(true)` JSON 数据仍能统计祖先、费率、大小和入池时间，但无法查询真实后代或交易内容：

```bash
python3 txpool/diagnostics/report.py --snapshot /tmp/ckb-pool.json
```

## 单节点 Testnet 清理（可选）

`report.py` 默认仅执行只读诊断，不会清理任何交易池。需要清理时，在仓库根目录运行：

```bash
python3 txpool/diagnostics/report.py --rpc-url http://127.0.0.1:8114 --clear-tx-pool --execute --yes
```

该命令先保存 Markdown 报告和原始交易池 JSON 快照，再重新检查目标节点网络必须为 `ckb_testnet`，随后仅对指定 RPC 运行一次 `clear_tx_pool`。最后输出 Pending/Proposed 清理前后的数量，**不调用 Ansible、不清理其他节点**。

- `--clear-tx-pool` 必须同时提供 `--execute --yes`，避免误操作。
- 不允许通过 `--snapshot` 离线模式执行清理。
- 这是整池清理，包含正常未确认交易；可能影响 Faucet 等应用，并可能发生 P2P 重新广播。
- 清理前的报告和快照会写到 `--output-dir`（默认相对工作目录 `./txpool-reports`）。
- 如果诊断过程因 RPC 超时/异常失败，清理不会开始；大交易池建议先 `--skip-details --skip-samples` 以减少诊断耗时。

## 报告内容及含义

| 章节 | 内容 | 用途 |
|---|---|---|
| 交易池概况 | Pending、Proposed、最大祖先数、容量、基础费率 | 确认当前积压及压力 |
| Ancestors 分布 | 按包含自身的祖先计数统计每组交易数量 | 区分独立交易和依赖交易；**不等于链深度** |
| Fee Rate / Size | 高占比手续费率与交易大小 | 检查低费率、大量重复模板 |
| Cycles | 相同执行成本出现次数 | 对比交易运行成本 |
| 入池时间 | 各种 Size + Fee 组合首次/末次入池时间（北京时间） | 分析批量发送阶段 |
| 根交易 Descendants | 根交易后代数量及最大值 | 识别扇出依赖，不等于最长链 |
| 交易结构抽样 | Inputs、Outputs、Cell Deps、Lock 类型、Args、父交易依赖、重复 OutPoint | 寻找共同模板或可疑依赖 |
| 解读与下一步 | 说明判断边界与需要的矿工侧证据 | 避免把相关性误判成因果 |

## 输出文件

运行时同时打印中文 Markdown 报告，并写入：

- `txpool-report-YYYYMMDD-HHMMSS.md`：可直接保存/分享的中文诊断报告；
- `txpool-snapshot-YYYYMMDD-HHMMSS.json`：原始 `get_raw_tx_pool(true)` 交易池快照，用于后续离线复盘。

**时间与安全提示**：报告使用 UTC+8，北京时间。快照可能较大，并可能包含交易 Hash、费用、时间等数据；公开分享前请确认是否合适。如果是事故现场，最好在重启或清理交易池前保存数据。

## 需要特别注意

1. 交易结构默认抽取高频 Size 类别的前 10 笔，只能反映样本，不代表全量交易。
2. 相同 Lock Code Hash 或相同 Cell Dep 不能证明同一发送人；不同的输出 Lock Args 也不能直接证明不同发送人。
3. Sample 内重复消费同一 OutPoint 为 0，并不能排除全池其他交易发生冲突。
4. 本节点 Proposed 为 0 不代表矿工没有 Proposal。确认根因还需结合矿工 Block Template、区块 Proposal、节点日志。
5. `max_ancestors_count`、`max_descendants_count` 与实际最长连续依赖路径是三个不同的量，本工具不声称计算全池最大链深度。

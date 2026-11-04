# CKB TxPool tools

统一存放 Testnet 交易池日常清理和事故诊断工具。

## 目录

- `guard.py`：TxPool 阈值检查及 Ansible 执行清理（单次运行，适合 cron）
- `diagnostics/report.py`：采集并生成中文交易池诊断报告
- `diagnostics/README.md`：诊断报告具体说明

## 清理脚本（仅 Testnet）

在 Ansible 控制机执行，默认参考节点 `http://54.238.17.163:8224`，Inventory 分组 `ckb-testnet`，远端 RPC `http://127.0.0.1:18121`。

自动清理规则（只允许 `ckb_testnet`）：

- Pending **大于** `--pending-threshold`（默认 10000）：调用 `clear_tx_pool` 清理整个交易池。
- 任一 Pending 交易等待时间 **大于** `--max-age-seconds`（默认 300 秒）：同样清理整个交易池，不要求 Pending 数量同时超限。
- 只有祖先数量超出 `--max-ancestors`（默认 10）时，继续通过 `remove_transaction` 定向清理。
- `--targeted-only` 可以关闭自动整池清理，恢复只针对根交易删除的模式；`--clear-tx-pool` 是人工强制整池清理。
- 不带 `--execute` 一律为 DRY-RUN；`--clear-on-pending` 为兼容旧命令保留，现在无需显式指定。

```bash
# 先检查预期动作，不会删除任何交易
python3 txpool/guard.py --pending-threshold 10000 --max-age-seconds 300

# 正式执行：任一阈值超过就清理整池
python3 txpool/guard.py --pending-threshold 10000 --max-age-seconds 300 --execute
```

每 10 分钟运行一次：

```cron
*/10 * * * * /usr/bin/flock -n /tmp/ckb-tx-pool-guard.lock /usr/bin/python3 /etc/ansible/ckb-data-report/txpool/guard.py --pending-threshold 10000 --max-age-seconds 300 --execute >> /etc/ansible/ckb-data-report/ckb-tx-pool-guard.log 2>&1
```

**风险**：整池清理会移除所有 Pending/Proposed 未确认交易（不只是超时交易），可能影响 Faucet 或其他应用发送的正常交易，且可能被其他 P2P 节点重新广播。建议先在 DRY-RUN 验证。程序在参考节点和 Ansible 目标节点调用删除前都会检查目标链为 `ckb_testnet`；确保 Inventory 的 `ckb-testnet` 分组只包含测试网节点。若参考节点已清理但 Ansible 失败，会记录错误并非自动回滚。

## 诊断报告

```bash
python3 txpool/diagnostics/report.py --rpc-url http://127.0.0.1:8114
```

输出 Markdown 诊断报告与 JSON 快照，参见 [diagnostics/README.md](diagnostics/README.md)。

## 合并部署注意

此前目录 `txpool_diagnostics/` 已整理到 `txpool/diagnostics/`。合并 PR 后请更新所有引用原路径的命令和定时任务；原先功能分支根目录的 `ckb_tx_pool_guard.py` 对应这里的 `txpool/guard.py`。

## 为单台主机覆盖 RPC 端口

目标节点默认使用 `http://127.0.0.1:18121`。仅端口不同的主机在 `/etc/ansible/hosts` 中设置 `ckb_rpc_port`：

```ini
[ckb-testnet]
3.98.152.180
10.0.0.12 ckb_rpc_port=8114
10.0.0.13
```

未指定端口的主机仍使用 18121；指定了 `ckb_rpc_port=8114` 的主机使用本机 8114。`--local-rpc` 可以改变整个分组的默认地址和端口，但主机变量会覆盖端口。该设置对 `clear_tx_pool` 和 `remove_transaction` 都生效，查询参考节点的 `--rpc-url` 不受影响，cron 无需改动。

测试连接：

```bash
ansible ckb-testnet -i /etc/ansible/hosts -m ping
```

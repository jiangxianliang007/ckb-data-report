# CKB TxPool tools

统一存放 Testnet 交易池日常清理和事故诊断工具。

## 目录

- `guard.py`：TxPool 阈值检查及 Ansible 执行清理（单次运行，适合 cron）
- `diagnostics/report.py`：采集并生成中文交易池诊断报告
- `diagnostics/README.md`：诊断报告具体说明

## 清理脚本（仅 Testnet）

在 Ansible 控制机执行，默认参考节点 `http://54.238.17.163:8224`，Inventory 分组 `ckb-testnet`，远端 RPC `http://127.0.0.1:18121`。

```bash
# 仅打印检查结果
python3 txpool/guard.py --pending-threshold 10000 --max-age-seconds 300 --max-ancestors 10 --clear-on-pending

# 开启清理，Pending > 10000 时先清空参考节点，再通过 Ansible 清空目标组
# 其余情况，按交易年龄或祖先数量触发定向 remove_transaction
python3 txpool/guard.py --pending-threshold 10000 --max-age-seconds 300 --max-ancestors 10 --clear-on-pending --execute
```

每 3 分钟执行一次的示例（确认测试网节点清单、日志权限和 SSH 凭证后再启用）：

```cron
*/3 * * * * /usr/bin/flock -n /tmp/ckb-tx-pool-guard.lock /usr/bin/python3 /etc/ansible/ckb-data-report/txpool/guard.py --pending-threshold 10000 --max-age-seconds 300 --max-ancestors 10 --clear-on-pending --execute >> /etc/ansible/ckb-data-report/ckb-tx-pool-guard.log 2>&1
```

`--clear-on-pending` 是显式开关，不带此开关时超过 Pending 阈值仅进入定向清理候选选择。人工强制整池清理另有 `--clear-tx-pool --execute`。两种整池清理都会处理本地正常未确认交易，且清理不保证其他 P2P 节点不会重新广播。

**运行限制**：参考节点会校验 `ckb_testnet`，但远端仅通过 Ansible 直接调用 RPC，确保 `ckb-testnet` 分组只有测试网主机。不要在不具备 Ansible CLI 的普通 CKB 节点上加 `--execute` 运行，避免出现参考节点清理成功而远端未清理的部分成功状态。

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

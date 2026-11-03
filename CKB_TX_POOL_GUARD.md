# CKB Testnet TxPool Guard

One Python script runs **only on the Ansible control server**. It examines the reference CKB testnet node (`http://54.238.17.163:8224`), removes an abnormal root transaction from that node, then runs **Ansible ad-hoc shell** with `curl` on all targets to remove the same hash. No script is copied onto target machines, and target machines do not scan their tx pools.

The script calls `get_pool_tx_detail_info` in batches for root transactions to retrieve real `descendants_count` values. `get_raw_tx_pool(true)` does not include this field. Logs report `max_ancestors_count`, `max_descendants_count`, `roots_checked` and `roots_vanished` (transactions that left the pool during scanning). An RPC error aborts the run without silently treating missing counts as zero.

Detection triggers if either:
- Longest dependency chain is greater than `--max-chain-length 500`; or
- A root transaction has more than `--max-descendants 1000` descendants.

Only the **reference** node's chain identity is checked (`ckb-testnet`). Targets must be a trusted inventory group containing testnet nodes only. Any transactions removed affect local tx pools, not the network globally.

## Usage

Python 3 and the Ansible CLI are required on the control server. Target nodes need `curl` and a local CKB JSON-RPC endpoint with the `Pool` module enabled.

Example inventory:

```ini
[ckb-testnet]
3.98.152.180
```

Check SSH and the inventory first:

```bash
ansible ckb-testnet -i /etc/ansible/hosts -m ping
```

Dry run (default):

```bash
python3 ckb_tx_pool_guard.py --inventory /etc/ansible/hosts
```

Execute cleanup:

```bash
python3 ckb_tx_pool_guard.py --execute --inventory /etc/ansible/hosts --group ckb-testnet
```

The default RPC on each target is `http://127.0.0.1:18121`. Override with `--local-rpc` if needed.

Run every 10 minutes via the Ansible control server user's crontab:

```cron
*/10 * * * * /usr/bin/flock -n /tmp/ckb-tx-pool-guard.lock /usr/bin/python3 /opt/ckb-data-report/ckb_tx_pool_guard.py --execute --inventory /etc/ansible/hosts --group ckb-testnet >> /var/log/ckb-tx-pool-guard.log 2>&1
```

Use a writable log path, and ensure the cron user has the correct Ansible SSH key and inventory access. `flock` prevents overlap. Test dry run first, then enable `--execute`.

Note: the Python script records Ansible stdout/stderr. Ansible failure exits nonzero; if a particular remote RPC returns an error, inspect the logged output. This is an operational guard, not an automatic network-wide transaction ban.

## Current options

Run on the Ansible controller. The default detection thresholds are pending > 10000, transaction age > 300 seconds, or ancestors_count > 10. The script scans up to 500 qualifying candidates, sorts traced roots by descendants_count, and selects up to 20 roots with a combined estimated removal budget of 500 transactions. The estimated budget is not an exact count: some descendant sets may overlap or change during execution. The reference node is 54.238.17.163:8224, the remote inventory group is ckb-testnet and each target uses local RPC 127.0.0.1:18121. The command defaults to dry run; pass --execute to perform removal. Review the dry-run output before enabling a scheduled run, since ordinary pending transactions can remain unconfirmed for more than five minutes.

## Optional full pool cleanup

Use `--clear-on-pending` to enable full-pool cleanup only when reference Pending exceeds `--pending-threshold` (default 10000). Use `--clear-tx-pool` for a manual full-pool cleanup. Both require `--execute` to change state; without it they only log the intended action. Full-pool cleanup first calls `clear_tx_pool` on the reference Testnet node, then invokes a single Ansible ad-hoc command to call the same RPC on the `ckb-testnet` target group. Ordinary execution without those flags continues selective root cleanup. Clearing a pool removes all local unconfirmed transactions including legitimate ones; it does not undo committed blockchain transactions.

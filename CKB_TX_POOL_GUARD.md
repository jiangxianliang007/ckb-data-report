# CKB Testnet TxPool Guard

One Python script runs **only on the Ansible control server**. It examines the reference CKB testnet node (`http://35.79.78.62:8224`), removes an abnormal root transaction from that node, then runs **Ansible ad-hoc shell** with `curl` on all targets to remove the same hash. No script is copied onto target machines, and target machines do not scan their tx pools.

Detection triggers if either:
- Longest dependency chain is greater than `--max-chain-length 500`; or
- A root transaction has more than `--max-descendants 1000` descendants.

Only the **reference** node's chain identity is checked (`ckb_testnet`). Targets must be a trusted inventory group containing testnet nodes only. Any transactions removed affect local tx pools, not the network globally.

## Usage

Python 3 and the Ansible CLI are required on the control server. Target nodes need `curl` and a local CKB JSON-RPC endpoint with the `Pool` module enabled.

Example inventory:

```ini
[ckb_testnet]
testnet-1 ansible_host=10.0.0.11
testnet-2 ansible_host=10.0.0.12
```

Check SSH and the inventory first:

```bash
ansible ckb_testnet -i /etc/ansible/hosts -m ping
```

Dry run (default):

```bash
python3 ckb_tx_pool_guard.py --inventory /etc/ansible/hosts
```

Execute cleanup:

```bash
python3 ckb_tx_pool_guard.py --execute --inventory /etc/ansible/hosts --group ckb_testnet
```

If all CKB nodes use local port 8224 instead of 8114, set `--local-rpc http://127.0.0.1:8224`.

Run every 10 minutes via the Ansible control server user's crontab:

```cron
*/10 * * * * /usr/bin/flock -n /tmp/ckb-tx-pool-guard.lock /usr/bin/python3 /opt/ckb-data-report/ckb_tx_pool_guard.py --execute --inventory /etc/ansible/hosts --group ckb_testnet >> /var/log/ckb-tx-pool-guard.log 2>&1
```

Use a writable log path, and ensure the cron user has the correct Ansible SSH key and inventory access. `flock` prevents overlap. Test dry run first, then enable `--execute`.

Note: the Python script records Ansible stdout/stderr. Ansible failure exits nonzero; if a particular remote RPC returns an error, inspect the logged output. This is an operational guard, not an automatic network-wide transaction ban.

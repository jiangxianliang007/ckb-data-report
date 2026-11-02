#!/usr/bin/env python3
"""CKB testnet tx-pool guard: scan one RPC, clean it, then invoke Ansible remotely.

Runs ONCE per invocation, intended for cron every 10 minutes.
No script is copied to target hosts: Ansible runs curl against each host's local RPC.
"""
import argparse
import json
import logging
import re
import shlex
import subprocess
import sys
import urllib.request

LOG = logging.getLogger("ckb-txpool-guard")
TX_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
BATCH_SIZE = 20


def batch_rpc(url, calls):
    payload = [{"jsonrpc": "2.0", "id": i, "method": method, "params": params}
               for i, (method, params) in enumerate(calls)]
    request = urllib.request.Request(url, json.dumps(payload).encode(),
                                     {"Content-Type": "application/json", "User-Agent": "ckb-txpool-guard/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        replies = json.load(response)
    if not isinstance(replies, list) or len(replies) != len(calls):
        raise RuntimeError("Incomplete RPC batch response")
    by_id = {r["id"]: r for r in replies}
    if len(by_id) != len(calls):
        raise RuntimeError("Duplicate RPC batch IDs")
    results = []
    for i, (method, _) in enumerate(calls):
        reply = by_id[i]
        if reply.get("error"):
            raise RuntimeError(f"{method}: {reply['error']}")
        results.append(reply.get("result"))
    return results


def rpc(url, method, params=None):
    return batch_rpc(url, [(method, params or [])])[0]


def rpc_many(url, method, tx_hashes):
    output = []
    for index in range(0, len(tx_hashes), BATCH_SIZE):
        output.extend(batch_rpc(url, [(method, [h]) for h in tx_hashes[index:index+BATCH_SIZE]]))
    return output


def depth_roots(url, entries, threshold):
    """Trace dependencies of high-ancestor-count transactions, returning root->max depth.

    We never infer a root if a required transaction cannot be fetched.
    """
    candidates = [h for h, info in entries.items()
                  if int(info["ancestors_count"], 16) > threshold]
    if not candidates:
        return {}
    graph = {}
    frontier = set(candidates)
    while frontier:
        missing = sorted(h for h in frontier if h not in graph)
        frontier = set()
        fetch = []
        for h in missing:
            if int(entries[h]["ancestors_count"], 16) == 1:
                graph[h] = set()
            else:
                fetch.append(h)
        for h, result in zip(fetch, rpc_many(url, "get_transaction", fetch)):
            if not result or not result.get("transaction"):
                raise RuntimeError(f"Cannot resolve dependency for {h}; refusing cleanup")
            tx = result["transaction"]
            tx = tx.get("inner", tx)
            parents = {x["previous_output"]["tx_hash"] for x in tx.get("inputs", [])}
            parents.update(x["out_point"]["tx_hash"] for x in tx.get("cell_deps", []))
            graph[h] = parents.intersection(entries)
            frontier.update(graph[h] - graph.keys())

    paths = {}
    for h in sorted(graph, key=lambda v: int(entries[v]["ancestors_count"], 16)):
        parents = graph[h]
        if not parents:
            paths[h] = (1, h)
        else:
            if any(p not in paths for p in parents):
                raise RuntimeError(f"Unresolved dependency graph at {h}")
            depth, root = max((paths[p] for p in parents), key=lambda pair: pair[0])
            paths[h] = (depth + 1, root)
    roots = {}
    for h in candidates:
        depth, root = paths[h]
        if depth > threshold:
            roots[root] = max(depth, roots.get(root, 0))
    return roots


def remote_cleanup(args, tx_hash):
    # The hash is validated, and only the RPC cleanup call runs on target hosts.
    # No Python file or helper script is deployed to them.
    payload = json.dumps({"jsonrpc": "2.0", "id": 1,
                          "method": "remove_transaction", "params": [tx_hash]})
    command = ("curl --fail --silent --show-error --max-time 30 "
               "-H 'Content-Type: application/json' --data-binary "
               + shlex.quote(payload) + " " + shlex.quote(args.local_rpc))
    cmd = ["ansible", args.group, "-i", args.inventory, "-m", "shell", "-a", command]
    LOG.info("ansible cleanup group=%s tx=%s", args.group, tx_hash)
    result = subprocess.run(cmd, text=True, capture_output=True, timeout=180)
    if result.stdout.strip():
        LOG.info("ansible stdout: %s", result.stdout.strip())
    if result.stderr.strip():
        LOG.warning("ansible stderr: %s", result.stderr.strip())
    if result.returncode:
        raise RuntimeError(f"Ansible failed with exit code {result.returncode}")
    # Ansible shell rc=0 only shows curl succeeded; inspect RPC JSON result manually in logs.
    if '"error"' in result.stdout:
        raise RuntimeError("Ansible output contains JSON-RPC error; check logs")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rpc-url", default="http://54.238.17.163:8224")
    p.add_argument("--max-chain-length", type=int, default=500)
    p.add_argument("--max-descendants", type=int, default=1000)
    p.add_argument("--max-roots", type=int, default=2)
    p.add_argument("--inventory", default="/etc/ansible/hosts")
    p.add_argument("--group", default="ckb-testnet")
    p.add_argument("--local-rpc", default="http://127.0.0.1:18121")
    p.add_argument("--execute", action="store_true", help="Actually remove transactions (default: dry run)")
    args = p.parse_args()
    if min(args.max_chain_length, args.max_descendants, args.max_roots) < 1:
        p.error("thresholds and max-roots must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        info = rpc(args.rpc_url, "get_blockchain_info")
        chain = info["chain"]
        if chain != "ckb_testnet":
            raise RuntimeError(f"Refusing non-testnet reference chain: {chain}")
        pool = rpc(args.rpc_url, "get_raw_tx_pool", [True])
        entries = {**pool["pending"], **pool["proposed"]}
        LOG.info("chain=%s entries=%d execute=%s", chain, len(entries), args.execute)

        roots = {}
        for h, entry in entries.items():
            if int(entry["ancestors_count"], 16) == 1 and int(entry.get("descendants_count", "0x0"), 16) > args.max_descendants:
                roots[h] = {"depth": None, "descendants": int(entry["descendants_count"], 16)}
        for h, depth in depth_roots(args.rpc_url, entries, args.max_chain_length).items():
            roots.setdefault(h, {"depth": None, "descendants": int(entries[h].get("descendants_count", "0x0"), 16)})
            roots[h]["depth"] = depth

        selected = sorted(roots.items(), key=lambda x: max(x[1]["depth"] or 0, x[1]["descendants"]), reverse=True)[:args.max_roots]
        if not selected:
            LOG.info("no anomalous transaction chains found")
            return 0
        for h, metrics in selected:
            if not TX_RE.fullmatch(h):
                raise RuntimeError(f"Invalid transaction hash: {h}")
            LOG.warning("anomaly root=%s depth=%s descendants=%s", h, metrics["depth"], metrics["descendants"])
            if not args.execute:
                LOG.info("DRY_RUN: would remove root=%s on reference and Ansible group=%s", h, args.group)
                continue
            # Reference removal happens first; do not invoke Ansible if reference declined removal.
            removed = rpc(args.rpc_url, "remove_transaction", [h])
            LOG.warning("reference remove root=%s result=%s", h, removed)
            if removed is True:
                remote_cleanup(args, h)
            else:
                LOG.warning("skip Ansible for %s: reference transaction no longer present", h)
        return 0
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        LOG.exception("guard failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

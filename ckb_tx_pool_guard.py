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
import time
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


def trace_roots(url, entries, candidates):
    """Return roots for selected transactions using their in-pool dependencies."""
    graph = {}
    frontier = set(candidates)
    while frontier:
        hashes = sorted(frontier - graph.keys())
        frontier = set()
        fetch = []
        for h in hashes:
            if int(entries[h]["ancestors_count"], 16) == 1:
                graph[h] = set()
            else:
                fetch.append(h)
        for h, result in zip(fetch, rpc_many(url, "get_transaction", fetch)):
            if not result or not result.get("transaction"):
                graph[h] = None
                continue
            tx = result["transaction"]
            tx = tx.get("inner", tx)
            parents = {x["previous_output"]["tx_hash"] for x in tx.get("inputs", [])}
            parents.update(x["out_point"]["tx_hash"] for x in tx.get("cell_deps", []))
            graph[h] = parents.intersection(entries)
            frontier.update(graph[h] - graph.keys())
    roots = {}
    for h in sorted(graph, key=lambda k: int(entries[k]["ancestors_count"], 16)):
        parents = graph[h]
        if parents is None:
            roots[h] = set()
        elif not parents:
            roots[h] = {h}
        else:
            roots[h] = set().union(*(roots.get(p, set()) for p in parents))
    result = set()
    for h in candidates:
        result.update(roots.get(h, set()))
    return sorted(result)


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
    p.add_argument("--pending-threshold", type=int, default=10000)
    p.add_argument("--max-age-seconds", type=int, default=60)
    p.add_argument("--max-ancestors", type=int, default=10)
    p.add_argument("--max-candidates", type=int, default=100)
    p.add_argument("--max-roots", type=int, default=2)
    p.add_argument("--inventory", default="/etc/ansible/hosts")
    p.add_argument("--group", default="ckb-testnet")
    p.add_argument("--local-rpc", default="http://127.0.0.1:18121")
    p.add_argument("--execute", action="store_true", help="Actually remove transactions (default: dry run)")
    args = p.parse_args()
    if min(args.pending_threshold, args.max_age_seconds, args.max_ancestors, args.max_candidates, args.max_roots) < 1:
        p.error("thresholds and max-roots must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        info = rpc(args.rpc_url, "get_blockchain_info")
        chain = info["chain"]
        if chain != "ckb_testnet":
            raise RuntimeError(f"Refusing non-testnet reference chain: {chain}")
        pool = rpc(args.rpc_url, "get_raw_tx_pool", [True])
        entries = {**pool["pending"], **pool["proposed"]}
        max_ancestors = max((int(e["ancestors_count"], 16) for e in entries.values()), default=0)
        # Any of the three configurable conditions triggers targeted root removal.
        # Never remove the entire pool merely because its size is large.
        pending_count = len(pool["pending"])
        now_ms = int(time.time() * 1000)
        candidates = []
        over_age = 0
        over_ancestors = 0
        missing_ts = 0
        for h, entry in pool["pending"].items():
            ancestor_count = int(entry["ancestors_count"], 16)
            timestamp = entry.get("timestamp")
            if timestamp is None:
                missing_ts += 1
                age = 0
            else:
                age = max(0, (now_ms - int(timestamp, 16)) // 1000)
            age_bad = timestamp is not None and age > args.max_age_seconds
            ancestor_bad = ancestor_count > args.max_ancestors
            over_age += int(age_bad)
            over_ancestors += int(ancestor_bad)
            if age_bad or ancestor_bad or pending_count > args.pending_threshold:
                candidates.append((h, age, ancestor_count, age_bad, ancestor_bad))

        LOG.info(
            "chain=%s entries=%d pending=%d proposed=%d execute=%s",
            chain, len(entries), pending_count, len(pool["proposed"]), args.execute
        )
        LOG.info(
            "max_ancestors_count=%d over_age=%d over_ancestors=%d "
            "pending_threshold=%d missing_timestamp=%d",
            max_ancestors, over_age, over_ancestors,
            args.pending_threshold, missing_ts
        )
        if not candidates:
            LOG.info("no cleanup conditions matched")
            return 0
        # First choose high-ancestor and older transactions; limit the trace workload.
        candidates.sort(key=lambda v: (v[4], v[3], v[1]), reverse=True)
        selected_candidates = [x[0] for x in candidates[:args.max_candidates]]
        roots = trace_roots(args.rpc_url, entries, selected_candidates)[:args.max_roots]
        LOG.warning("cleanup candidates=%d traced=%d roots=%d", len(candidates), len(selected_candidates), len(roots))
        for h in roots:
            if not TX_RE.fullmatch(h):
                raise RuntimeError(f"Invalid transaction hash: {h}")
            LOG.warning("trigger root=%s pending=%d over_age=%d over_ancestors=%d", h, pending_count, over_age, over_ancestors)
            if not args.execute:
                LOG.info("DRY_RUN would remove root=%s on reference and Ansible group=%s", h, args.group)
                continue
            removed = rpc(args.rpc_url, "remove_transaction", [h])
            LOG.warning("reference remove root=%s result=%s", h, removed)
            if removed is True:
                remote_cleanup(args, h)
            else:
                LOG.warning("skip Ansible root=%s: reference transaction not found", h)
        return 0
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        LOG.exception("guard failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

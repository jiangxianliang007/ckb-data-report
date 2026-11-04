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
from urllib.parse import urlsplit

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


def remote_cleanup(args, tx_hash=None, clear=False):
    # The hash is validated, and only the RPC cleanup call runs on target hosts.
    # No Python file or helper script is deployed to them.
    method = "clear_tx_pool" if clear else "remove_transaction"
    params = [] if clear else [tx_hash]
    payload = json.dumps({"jsonrpc": "2.0", "id": 1,
                          "method": method, "params": params})
    # Ansible renders this Jinja expression independently for each inventory host.
    # Hosts without ckb_rpc_port use the port from --local-rpc (default 18121).
    parsed = urlsplit(args.local_rpc)
    if not parsed.hostname or not parsed.port:
        raise ValueError("--local-rpc must include a hostname and port")
    port = "{{ ckb_rpc_port | default(" + str(parsed.port) + ") }}"
    hostname = parsed.hostname
    if ":" in hostname:
        hostname = "[" + hostname + "]"
    target_rpc = parsed._replace(netloc=hostname + ":" + port).geturl()
    # Verify EACH target is a testnet node before sending a destructive RPC.
    probe = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "get_blockchain_info", "params": []})
    curl = "curl --fail --silent --show-error --max-time 30 -H 'Content-Type: application/json' --data-binary "
    pool_probe = json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tx_pool_info", "params": []})
    def post_rpc(body):
        return curl + shlex.quote(body) + " " + shlex.quote(target_rpc)
    # Echo raw JSON with labels: Ansible includes the inventory hostname in stdout.
    # This avoids requiring jq on every target host.
    command = ("chain_info=$(" + post_rpc(probe) + ")"
               + " && printf '%s\\n' \"$chain_info\" | grep -Eq "
               + shlex.quote(r'"chain"[[:space:]]*:[[:space:]]*"ckb_testnet"')
               + " && before=$(" + post_rpc(pool_probe) + ")"
               + " && printf 'CKB_TXPOOL_BEFORE %s\\n' \"$before\""
               + " && cleanup=$(" + post_rpc(payload) + ")"
               + " && printf 'CKB_TXPOOL_CLEANUP %s\\n' \"$cleanup\""
               + " && ! printf '%s\\n' \"$cleanup\" | grep -Eq "
               + shlex.quote(r'"error"[[:space:]]*:')
               + " && after=$(" + post_rpc(pool_probe) + ")"
               + " && printf 'CKB_TXPOOL_AFTER %s\\n' \"$after\"")
    cmd = ["ansible", args.group, "-i", args.inventory, "-m", "shell", "-a", command]
    LOG.info("ansible cleanup group=%s method=%s tx=%s", args.group, method, tx_hash)
    started = time.monotonic()
    result = subprocess.run(cmd, text=True, capture_output=True, timeout=180)
    LOG.info("ansible finished group=%s method=%s rc=%d duration_seconds=%.3f",
             args.group, method, result.returncode, time.monotonic() - started)
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
    p.add_argument("--max-age-seconds", type=int, default=300)
    p.add_argument("--max-ancestors", type=int, default=10)
    p.add_argument("--max-candidates", type=int, default=500)
    p.add_argument("--max-roots", type=int, default=20)
    p.add_argument("--clear-on-pending", action="store_true",
                   help="deprecated compatibility flag; threshold-based full clear is now the default")
    p.add_argument("--targeted-only", action="store_true",
                   help="disable automatic full-pool clears and use targeted root removal only")
    p.add_argument("--clear-tx-pool", action="store_true",
                   help="manually clear entire testnet pool; requires --execute")
    p.add_argument("--max-estimated-txs", type=int, default=500,
                   help="maximum estimated root+descendant transactions selected per run")
    p.add_argument("--inventory", default="/etc/ansible/hosts")
    p.add_argument("--group", default="ckb-testnet")
    p.add_argument("--local-rpc", default="http://127.0.0.1:18121")
    p.add_argument("--execute", action="store_true", help="Actually remove transactions (default: dry run)")
    args = p.parse_args()
    if min(args.pending_threshold, args.max_age_seconds, args.max_ancestors, args.max_candidates, args.max_roots, args.max_estimated_txs) < 1:
        p.error("thresholds and max-roots must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    LOG.info("guard start reference_rpc=%s inventory=%s group=%s target_rpc=%s execute=%s",
             args.rpc_url, args.inventory, args.group, args.local_rpc, args.execute)

    try:
        reference_started = time.monotonic()
        info = rpc(args.rpc_url, "get_blockchain_info")
        LOG.info("reference network_check rpc=%s chain=%s elapsed_seconds=%.3f",
                 args.rpc_url, info["chain"], time.monotonic() - reference_started)
        chain = info["chain"]
        if chain != "ckb_testnet":
            raise RuntimeError(f"Refusing non-testnet reference chain: {chain}")
        scan_started = time.monotonic()
        pool = rpc(args.rpc_url, "get_raw_tx_pool", [True])
        LOG.info("reference pool_scan rpc=%s elapsed_seconds=%.3f",
                 args.rpc_url, time.monotonic() - scan_started)
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
        max_age_seconds = 0
        for h, entry in pool["pending"].items():
            ancestor_count = int(entry["ancestors_count"], 16)
            timestamp = entry.get("timestamp")
            if timestamp is None:
                missing_ts += 1
                age = 0
            else:
                age = max(0, (now_ms - int(timestamp, 16)) // 1000)
                max_age_seconds = max(max_age_seconds, age)
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
        LOG.info("pending=%d pending_threshold=%d", pending_count, args.pending_threshold)
        LOG.info(
            "max_age_seconds=%d age_threshold=%d over_age=%d missing_timestamp=%d",
            max_age_seconds, args.max_age_seconds, over_age, missing_ts
        )
        LOG.info(
            "max_ancestors_count=%d ancestors_threshold=%d over_ancestors=%d",
            max_ancestors, args.max_ancestors, over_ancestors
        )
        # Pending count or transaction age triggers whole-pool cleanup by default.
        # Ancestor depth alone continues to use targeted root removal.
        full_clear_reason = (
            "manual" if args.clear_tx_pool else
            "pending" if pending_count > args.pending_threshold else
            "age" if over_age > 0 else None
        )
        if full_clear_reason and (args.clear_tx_pool or not args.targeted_only):
            LOG.warning(
                "FULL_POOL_CLEAR reason=%s pending=%d threshold=%d over_age=%d "
                "age_threshold=%d reference=%s group=%s execute=%s",
                full_clear_reason, pending_count, args.pending_threshold, over_age,
                args.max_age_seconds, args.rpc_url, args.group, args.execute
            )
            if not args.execute:
                LOG.warning("DRY_RUN would clear reference and Ansible group")
                return 0
            clear_started = time.monotonic()
            rpc(args.rpc_url, "clear_tx_pool")
            LOG.warning("reference clear_tx_pool complete rpc=%s elapsed_seconds=%.3f",
                        args.rpc_url, time.monotonic() - clear_started)
            remote_cleanup(args, clear=True)
            after = rpc(args.rpc_url, "tx_pool_info")
            LOG.info("full_clear reason=%s reference=%s pending_before=%d pending_after=%d",
                     full_clear_reason, args.rpc_url, pending_count, int(after["pending"], 16))
            return 0
        if not candidates:
            LOG.info("no cleanup conditions matched")
            return 0
        # First choose high-ancestor and older transactions; limit the trace workload.
        candidates.sort(key=lambda v: (v[4], v[3], v[1]), reverse=True)
        selected_candidates = [x[0] for x in candidates[:args.max_candidates]]
        root_hashes = trace_roots(args.rpc_url, entries, selected_candidates)
        # Query root descendant counts, then prefer removing high-impact roots.
        root_details = {}
        vanished = 0
        for root, detail in zip(root_hashes, rpc_many(args.rpc_url, "get_pool_tx_detail_info", root_hashes)):
            if detail is None:
                vanished += 1
                continue
            root_details[root] = int(detail["descendants_count"], 16)
        ranked = sorted(root_details.items(), key=lambda kv: (-kv[1], kv[0]))
        selected = []
        estimated_total = 0
        for root, descendants in ranked:
            estimate = descendants + 1
            if len(selected) >= args.max_roots:
                break
            if estimated_total + estimate > args.max_estimated_txs:
                continue
            selected.append((root, descendants))
            estimated_total += estimate
        LOG.warning(
            "cleanup candidates=%d traced=%d root_candidates=%d roots_vanished=%d "
            "selected_roots=%d estimated_txs=%d budget=%d",
            len(candidates), len(selected_candidates), len(root_hashes), vanished,
            len(selected), estimated_total, args.max_estimated_txs
        )
        if not selected and root_details:
            LOG.warning("no roots fit the per-run estimated transaction budget")
        for h, descendants in selected:
            if not TX_RE.fullmatch(h):
                raise RuntimeError(f"Invalid transaction hash: {h}")
            LOG.warning("trigger root=%s descendants=%d estimated_remove=%d", h, descendants, descendants + 1)
            if not args.execute:
                LOG.info("DRY_RUN would remove root=%s on reference and Ansible group=%s", h, args.group)
                continue
            removed = rpc(args.rpc_url, "remove_transaction", [h])
            LOG.warning("reference remove root=%s result=%s", h, removed)
            if removed is True:
                remote_cleanup(args, h)
            else:
                LOG.warning("skip Ansible root=%s: reference transaction not found", h)
        if args.execute and selected:
            after = rpc(args.rpc_url, "tx_pool_info")
            LOG.info("pending_before=%d pending_after=%d", pending_count, int(after["pending"], 16))
        return 0
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        LOG.exception("guard failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

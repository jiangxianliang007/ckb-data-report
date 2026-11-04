#!/usr/bin/env python3
"""CKB TxPool 只读诊断报告：快照、依赖、手续费、时间分布和交易结构。"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import statistics
import urllib.request

CST = timezone(timedelta(hours=8))
BATCH = 20

def number(v):
    return int(v, 16) if isinstance(v, str) and v.startswith("0x") else int(v)

def rpc(url, calls, timeout):
    data = json.dumps([{"jsonrpc":"2.0","id":i,"method":m,"params":p}
                       for i,(m,p) in enumerate(calls)]).encode()
    request = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        replies = json.load(response)
    if not isinstance(replies, list) or len(replies) != len(calls):
        raise RuntimeError("RPC 批量返回不完整")
    by_id = {v["id"]: v for v in replies}
    values = []
    for i,(method,_) in enumerate(calls):
        item = by_id[i]
        if item.get("error"):
            raise RuntimeError(f"{method}: {item['error']}")
        values.append(item["result"])
    return values

def single(url, method, params, timeout):
    return rpc(url, [(method,params)],timeout)[0]

def batches(url, method, hashes, timeout):
    for j in range(0,len(hashes),BATCH):
        results = rpc(url,[(method,[h]) for h in hashes[j:j+BATCH]],timeout)
        for h,r in zip(hashes[j:j+BATCH],results):
            yield h,r

def stamp(ts):
    try:
        return datetime.fromtimestamp(number(ts)/1000,CST).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError,TypeError,OverflowError,OSError):
        return "未知"

def report(args):
    if args.snapshot:
        snapshot = json.loads(Path(args.snapshot).read_text())
        pool = snapshot.get("result", snapshot)
        info = None
        chain = "离线快照（未重新校验网络）"
    else:
        info = single(args.rpc_url,"tx_pool_info",[],args.timeout)
        chain = single(args.rpc_url,"get_blockchain_info",[],args.timeout)["chain"]
        pool = single(args.rpc_url,"get_raw_tx_pool",[True],args.timeout)
    if not isinstance(pool,dict) or not isinstance(pool.get("pending"),dict) or not isinstance(pool.get("proposed"),dict):
        raise ValueError("无效的 get_raw_tx_pool(true) 快照")
    entries = {**pool["pending"],**pool["proposed"]}
    rows = []
    ancestor_dist, size_dist, fee_dist, cycles_dist, timestamps = Counter(), Counter(), Counter(), Counter(), defaultdict(list)
    for h,v in entries.items():
        a,size,fee,cyc = (number(v[k]) for k in ("ancestors_count","size","fee","cycles"))
        ts = v.get("timestamp")
        rate = fee * 1000 // size if size else 0
        rows.append((h,a,size,fee,cyc,rate))
        ancestor_dist[a] += 1
        size_dist[size] += 1
        fee_dist[rate] += 1
        cycles_dist[cyc] += 1
        if ts is not None:
            timestamps[(size,fee)].append(number(ts))
    root_hashes = [h for h,a,*_ in rows if a==1]
    descendants = {}
    vanished = 0
    # Offline snapshots do not contain descendants_count; leave it unavailable rather than pretending zero.
    if not args.snapshot and not args.skip_details:
        for h,d in batches(args.rpc_url,"get_pool_tx_detail_info",root_hashes,args.timeout):
            if d is None:
                vanished += 1
            else:
                descendants[h] = number(d["descendants_count"])
    samples = {}
    for size,count in size_dist.most_common(args.top):
        sample_hashes = [h for h,a,s,fee,cyc,rate in rows if s==size][:args.samples]
        if args.snapshot or args.skip_samples:
            samples[size] = {"sampled":0,"note":"离线模式或显式跳过交易结构查询"}
            continue
        statuses,inputs,outputs,deps,locks,args_counter,parents,outpoints = (Counter() for _ in range(8))
        in_pool = 0
        missing = 0
        for h,resp in batches(args.rpc_url,"get_transaction",sample_hashes,args.timeout):
            if not resp or not resp.get("transaction"):
                missing += 1
                continue
            statuses[resp.get("tx_status",{}).get("status","unknown")] += 1
            tx=resp["transaction"]
            tx=tx.get("inner",tx)
            ins,outs=tx.get("inputs",[]),tx.get("outputs",[])
            inputs[len(ins)]+=1
            outputs[len(outs)]+=1
            for inp in ins:
                point=inp["previous_output"]
                parent=point["tx_hash"]
                parents[parent]+=1
                outpoints[(parent,point["index"])]+=1
                in_pool+=int(parent in entries)
            for dep in tx.get("cell_deps",[]):
                point=dep["out_point"]
                deps[(point["tx_hash"],point["index"])]+=1
            for out in outs:
                lock=out["lock"]
                locks[(lock["code_hash"],lock["hash_type"])]+=1
                args_counter[lock["args"]]+=1
        samples[size] = {
            "sampled":len(sample_hashes)-missing,"missing":missing,
            "statuses":statuses,"inputs":inputs,"outputs":outputs,
            "top_cell_deps":deps.most_common(3),"lock_types":locks.most_common(3),
            "unique_output_lock_args":len(args_counter),
            "inputs_referencing_pool":in_pool,
            "shared_parent_hashes":sum(v>1 for v in parents.values()),
            "duplicate_spent_outpoints":sum(v>1 for v in outpoints.values()),
        }
    def top_items(counter, n=None):
        return counter.most_common(n or args.top)
    lines=[
        "# CKB TxPool 只读诊断报告",
        "",
        f"- 生成时间（北京时间）：{datetime.now(CST):%Y-%m-%d %H:%M:%S}",
        f"- 网络：{chain}",
        f"- 数据来源：{args.snapshot or args.rpc_url}",
        "- 说明：所有 RPC 均为只读；报告不会删除交易或修改节点状态。",
        "",
        "## 一、交易池概况",
        f"- Pending：{len(pool['pending']):,}；Proposed：{len(pool['proposed']):,}；合计：{len(entries):,}",
        f"- 最大 ancestors_count（包含自身）：{max(ancestor_dist,default=0)}",
        f"- 祖先计数为 1 的根交易数：{len(root_hashes):,}",
    ]
    if info:
        lines += [f"- TxPool 配置/资源：min_fee_rate={number(info['min_fee_rate'])} shannons/KB；total_tx_size={number(info['total_tx_size']):,} bytes；max_tx_pool_size={number(info['max_tx_pool_size']):,} bytes；orphan={number(info['orphan'])}"]
    if descendants:
        lines += [f"- 最大根交易 descendants_count：{max(descendants.values())}；查询成功：{len(descendants)}；扫描期间消失：{vanished}"]
    else:
        lines += ["- 最大根交易 descendants_count：未采集（离线快照或跳过详情查询）" if args.snapshot or args.skip_details else "- 最大根交易 descendants_count：0（已查询，当前没有有效根详情）"]
    lines += [
        "",
        "## 二、祖先计数分布（不是最长链深度）",
        "| Ancestors（含自身） | 交易数 |","|---:|---:|",
    ]
    lines += [f"| {a} | {c:,} |" for a,c in sorted(ancestor_dist.items())]
    lines += ["","最大祖先计数不等于实际最长依赖路径；并行父交易会增加祖先数量。本报告不计算全池最长链深度。","",
              "## 三、手续费率及交易大小分布",
              "Fee Rate = fee(shannons) × 1000 / size(bytes)；按整数向下取整。","",
              "| Fee Rate (shannons/KB) | 交易数 |","|---:|---:|"]
    lines += [f"| {v:,} | {n:,} |" for v,n in top_items(fee_dist)]
    lines += ["","| Size (bytes) | 交易数 |","|---:|---:|"]
    lines += [f"| {v} | {n:,} |" for v,n in top_items(size_dist)]
    lines += ["","## 四、Cycles 分布（相同值出现次数）","| Cycles | 交易数 |","|---:|---:|"]
    lines += [f"| {v:,} | {n:,} |" for v,n in top_items(cycles_dist)]
    lines += ["","## 五、交易入池时间分布（北京时间）",
              "timestamp 为当前节点记录的入池时间，不一定是全网首次广播时间。",
              "| Size | Fee (shannons) | 数量 | 最早入池 | 最晚入池 |","|---:|---:|---:|---|---|"]
    for (size,fee),ts in sorted(timestamps.items(),key=lambda kv:len(kv[1]),reverse=True)[:args.top]:
        lines.append(f"| {size} | {fee:,} | {len(ts):,} | {stamp(min(ts))} | {stamp(max(ts))} |")
    lines += ["","## 六、根交易后代规模"]
    if descendants:
        lines += ["| Root TX Hash | Descendants |","|---|---:|"]
        for h,n in sorted(descendants.items(),key=lambda item:item[1],reverse=True)[:args.top]:
            lines.append(f"| `{h}` | {n} |")
    else:
        lines += ["未采集根交易后代详情。"]
    lines += ["","## 七、交易结构抽样（按高频 Size）",
              "这里只抽样部分交易；不能据此证明所有交易同源、双花或者恶意。Lock 类型不代表钱包地址相同。"]
    for size,s in samples.items():
        lines += [f"### Size {size} bytes",f"- 成功抽样：{s['sampled']} 笔"]
        if "note" in s:
            lines.append(f"- {s['note']}")
            continue
        lines += [f"- Inputs 数量分布：{dict(s['inputs'])}",
                  f"- Outputs 数量分布：{dict(s['outputs'])}",
                  f"- 交易状态：{dict(s['statuses'])}",
                  f"- 引用池内父交易的 Input 数量：{s['inputs_referencing_pool']}",
                  f"- 共享父交易 Hash 的数量：{s['shared_parent_hashes']}（不等于双花）",
                  f"- 重复消费相同 OutPoint 的数量：{s['duplicate_spent_outpoints']}（仅样本内）",
                  f"- 不同 Output Lock Args 数量：{s['unique_output_lock_args']}",
                  f"- 最常见 Cell Deps：{s['top_cell_deps']}",
                  f"- 最常见 Output Lock 类型：{s['lock_types']}",""]
    lines += ["## 八、解读与下一步",
              "- Pending 很多、链深度较低，并不能证明超长链攻击；需同时检查矿工 Proposal、区块模板及交易传播。",
              "- 交易集中在固定大小/手续费模板，提示批量生成模式，但不能单独认定恶意。",
              "- 本节点 Proposed=0 不能直接推断全网矿工没有 Proposal。",
              "- 要确认矿工不打包的根因，需要对照矿工节点 TxPool、Block Template、日志与链上各区块 Proposal。",
              "- 该报告仅保留统计与少量样本分析；如果要复盘历史现场，请同时保留原始快照文件。",
              ""]
    return "\n".join(lines), pool

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rpc-url",default="http://127.0.0.1:8114")
    p.add_argument("--snapshot",help="离线分析已有 get_raw_tx_pool(true) JSON 文件")
    p.add_argument("--output-dir",default="./txpool-reports")
    p.add_argument("--top",type=int,default=10)
    p.add_argument("--samples",type=int,default=10)
    p.add_argument("--timeout",type=int,default=120)
    p.add_argument("--skip-details",action="store_true")
    p.add_argument("--skip-samples",action="store_true")
    p.add_argument("--clear-tx-pool",action="store_true",help="仅手动清空指定 Testnet 节点交易池")
    p.add_argument("--execute",action="store_true",help="允许执行清理")
    p.add_argument("--yes",action="store_true",help="确认清理")
    args=p.parse_args()
    if args.snapshot and args.clear_tx_pool:
        p.error("不允许清理离线快照")
    if args.clear_tx_pool and not (args.execute and args.yes):
        p.error("清理需要 --execute --yes")
    if (args.execute or args.yes) and not args.clear_tx_pool:
        p.error("--execute/--yes 必须与 --clear-tx-pool 一起使用")
    if min(args.top,args.samples,args.timeout)<1:
        p.error("top、samples、timeout 必须为正数")
    target=Path(args.output_dir)
    target.mkdir(parents=True,exist_ok=True)
    report_text,pool=report(args)
    suffix=datetime.now(CST).strftime("%Y%m%d-%H%M%S")
    report_path=target / f"txpool-report-{suffix}.md"
    report_path.write_text(report_text,encoding="utf-8")
    raw_path=target / f"txpool-snapshot-{suffix}.json"
    raw_path.write_text(json.dumps({"result":pool},ensure_ascii=False),encoding="utf-8")
    print(report_text)
    print(f"\\n报告文件：{report_path}\\n交易池快照：{raw_path}")

if __name__=="__main__":
    main()

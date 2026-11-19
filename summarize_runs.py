"""Summarize original per-request SSE measurements without mixing collection protocols."""
import argparse,csv,hashlib,json,math
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--runs',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
a.output.mkdir(parents=True,exist_ok=True);items=json.loads(a.runs.read_text());all_rows=[];result=[]
def percentile(values,q):
 v=sorted(values)
 return v[max(0,math.ceil(len(v)*q)-1)]
for item in items:
 d=Path(item['folder']);raw=(d/'responses/requests.jsonl').read_bytes();rows=[json.loads(x) for x in raw.decode('utf-8-sig').splitlines()];formal=[r for r in rows if r['tag']=='meas']
 assert len(rows)==13 and len(formal)==10
 argv=json.loads((d/'nsys_argv.json').read_text(encoding='utf-8-sig'))['argv']
 assert argv[argv.index('--sample')+1]=='process-tree' and argv[argv.index('--sampling-frequency')+1]=='1000'
 summary=dict(id=item['id'],engine=item['engine'],count=len(formal),warmup=3,max_tokens=1024,concurrency=1,throughput=sum(r['completion_tokens'] for r in formal)/sum(r['wall_s'] for r in formal),output_tokens=sum(r['completion_tokens'] for r in formal),wall_s=sum(r['wall_s'] for r in formal),request_source_sha256=hashlib.sha256(raw).hexdigest(),folder=str(d))
 for name,key,factor in [('e2e_s','wall_s',1),('ttft_s','ttft_s',1),('tpot_ms','tpot_s',1000),('request_tps','tps_e2e',1)]:
  values=[r[key]*factor for r in formal];assert all(math.isfinite(v) and v>=0 for v in values)
  summary[name]=dict(mean=sum(values)/len(values),p50=percentile(values,.5),p90=percentile(values,.9),p95=percentile(values,.95),p99=percentile(values,.99),min=min(values),max=max(values))
 for r in formal:all_rows.append(dict(config=item['id'],engine=item['engine'],request_index=r['i'],completion_tokens=r['completion_tokens'],e2e_s=r['wall_s'],ttft_s=r['ttft_s'],tpot_ms=1000*r['tpot_s'],request_tps=r['tps_e2e'],started_utc=r['started_utc'],finished_utc=r['finished_utc']))
 result.append(summary)
(a.output/'metrics.json').write_text(json.dumps(dict(configurations=result,quantile_method='nearest rank, identical to the original SSE driver summary',throughput_definition='sum completion_tokens divided by sum wall_s; warmup excluded',latency_definition='client SSE request E2E, first content TTFT, driver TPOT per completion token',tail_caution='p90, p95 and p99 describe ten observed requests and are not stable population-tail estimates'),ensure_ascii=False,indent=2))
with (a.output/'requests.csv').open('w',newline='') as f:
 w=csv.DictWriter(f,fieldnames=list(all_rows[0]));w.writeheader();w.writerows(all_rows)
print(json.dumps(dict(configurations=len(result),formal_requests=len(all_rows)),indent=2))

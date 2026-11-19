"""Serial streaming chat workload with fixed sampling; saves each raw SSE response before the next request."""
import argparse,datetime,hashlib,json,math,pathlib,statistics,time,urllib.request
p=argparse.ArgumentParser()
p.add_argument('--port',type=int,required=True);p.add_argument('--data',type=pathlib.Path,required=True)
p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--max-tokens',type=int,default=1024)
p.add_argument('--seed-base',type=int)
p.add_argument('--warmup',type=int,default=3);p.add_argument('--requests',type=int,default=13)
a=p.parse_args()
if not (0<=a.warmup<a.requests and a.max_tokens>0):p.error('require 0 <= warmup < requests, max_tokens > 0')
items=[json.loads(x) for x in a.data.read_text(encoding='utf-8').splitlines()][:a.requests]
if len(items)!=a.requests:raise ValueError('insufficient prompts')
a.output.mkdir(parents=True,exist_ok=False)
(a.output/'plan.json').write_text(json.dumps(dict(port=a.port,data=str(a.data),data_sha256=hashlib.sha256(a.data.read_bytes()).hexdigest(),requests=a.requests,warmup=a.warmup,max_tokens=a.max_tokens,temperature=1,top_p=0.95,top_k=20,stream=True,seed_base=a.seed_base,seed_policy='server default' if a.seed_base is None else 'seed_base + request index, identical across configurations'),indent=2))
rows=[]
for i,it in enumerate(items):
    body=dict(messages=[dict(role='user',content=it.get('question') or it.get('prompt') or str(it))],max_tokens=a.max_tokens,temperature=1,top_p=0.95,top_k=20,stream=True,stream_options=dict(include_usage=True))
    if a.seed_base is not None:body['seed']=a.seed_base+i
    (a.output/f'request_{i:02d}.json').write_text(json.dumps(body,ensure_ascii=False),encoding='utf-8')
    req=urllib.request.Request(f'http://127.0.0.1:{a.port}/v1/chat/completions',data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    start_utc=datetime.datetime.now(datetime.timezone.utc).isoformat();start=time.perf_counter();first=last=None;usage={};timings={};chunks=0;finish=None;done=False
    with (a.output/f'request_{i:02d}.sse').open('wb') as raw,urllib.request.urlopen(req,timeout=7200) as response:
        for line in response:
            raw.write(line)
            if not line.startswith(b'data: '):continue
            payload=line[6:].strip()
            if payload==b'[DONE]':done=True;break
            event=json.loads(payload)
            if event.get('error'):raise RuntimeError(event['error'])
            if event.get('usage'):usage=event['usage']
            if event.get('timings'):timings=event['timings']
            for ch in event.get('choices',[]):
                delta=ch.get('delta',{})
                if delta.get('content') or delta.get('reasoning_content'):
                    now=time.perf_counter();first=now if first is None else first;last=now;chunks+=1
                if ch.get('finish_reason'):finish=ch['finish_reason']
    end=time.perf_counter();tokens=usage.get('completion_tokens',0)
    if not done or tokens<=0 or first is None:raise RuntimeError(f'incomplete request {i}: done={done}, tokens={tokens}')
    span=last-first
    row=dict(i=i,tag='warm' if i<a.warmup else 'meas',started_utc=start_utc,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),wall_s=end-start,ttft_s=first-start,gen_span_s=span,completion_tokens=tokens,prompt_tokens=usage.get('prompt_tokens'),n_tok_stream=chunks,tpot_s=span/(tokens-1) if tokens>1 else None,tps_e2e=tokens/(end-start),finish_reason=finish,timings=timings)
    with (a.output/'requests.jsonl').open('a',encoding='utf-8') as f:f.write(json.dumps(row)+'\n')
    rows.append(row);print(json.dumps(row),flush=True)
measured=[r for r in rows if r['tag']=='meas']
stats={}
for key in ['wall_s','ttft_s','tpot_s','tps_e2e','completion_tokens']:
    values=sorted(r[key] for r in measured if r[key] is not None)
    stats[key]=dict(count=len(values),mean=statistics.mean(values),min=values[0],max=values[-1],**{f'p{q}':values[max(0,math.ceil(len(values)*q/100)-1)] for q in [50,90,95,99]})
result=dict(status='passed',count=len(rows),warmup=a.warmup,measured=len(measured),total_tokens=sum(r['completion_tokens'] for r in measured),aggregate_tps=sum(r['completion_tokens'] for r in measured)/sum(r['wall_s'] for r in measured),metrics=stats)
(a.output/'summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)

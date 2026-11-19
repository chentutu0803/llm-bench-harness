# llm-bench-harness

A minimal, receipt-oriented harness for benchmarking LLM inference servers with **real streaming requests**, and for validating Nsight Systems captures of those same runs.

I use this in my research on on-device inference acceleration (consumer-GPU serving of MoE models, speculative decoding configurations). Every number I report comes out of these three scripts, so the harness is built around two ideas: identical request streams across configurations, and verifiable artifacts for every run.

## Components

**`stream_bench_client.py`** — the measurement client.

- Sends real SSE streaming requests (`stream: true`, parsed chunk by chunk); no fake streaming, no offline batch shortcuts.
- Protocol: `--warmup` discarded requests, then measured requests, one at a time, fixed sampling (temperature 1.0, top_p 0.95, top_k 20) and a seed policy (`seed_base + request index`) so every engine configuration sees byte-identical prompts and seeds.
- Per request it records wall time, TTFT (first content chunk), generation span, TPOT per completion token, token counts, finish reason, and keeps the **raw SSE transcript** on disk.
- Emits `summary.json` with count, mean, min, max, p50/p90/p95/p99 for wall time, TTFT, TPOT, and tokens-per-second.

**`summarize_runs.py`** — cross-run aggregation.

- Reads a list of run folders, keeps warmup and measured requests separate, and refuses to mix runs collected under different protocols (it asserts the recorded nsys argv before accepting a run).
- Produces `metrics.json` (per-configuration aggregates with explicit quantile method and throughput definition) and `requests.csv` (every measured request, one row, for plotting).

**`inspect_profile.py`** — Nsight Systems capture inspection.

- Opens an exported nsys SQLite read-only, runs a quick_check, counts CUPTI/NVTX activity, and summarizes kernel time by symbol.
- Cross-checks the CUDA kernel timeline against the client's own request log: if the trace ends more than one second before the final response finished, the capture is rejected as incomplete.
- Writes a receipt with the capture's SHA256, byte size, kernel span, and per-request coverage — so a profile used in a report can always be traced back to the exact bytes it came from.

## Typical usage

```bash
# 1. Benchmark an OpenAI-compatible server (warmup 3, measured 10, seed base 310)
python stream_bench_client.py --port 8080 \
    --data gsm8k_test.jsonl --output run_a/responses \
    --max-tokens 1024 --warmup 3 --requests 13 --seed-base 310

# 2. (with the server wrapped in nsys profile ...) validate the capture
python inspect_profile.py run_a/capture.sqlite run_a/profile_receipt.json

# 3. Aggregate several configurations into one table
python summarize_runs.py --runs runs.json --output summary/
```

## Why it looks like this

- **Percentiles over averages**: decode latency is heavy-tailed; p90/p95/p99 travel with every mean.
- **Raw transcripts over derived numbers**: every request's SSE stream is kept, so any metric can be recomputed later.
- **Receipts over trust**: SHA256 of the prompt set and of each capture, recorded argv, and a coverage check that ties the GPU trace to the client-observed requests.

Protocol details (warmup counts, sampling, seed policy) live in the scripts; nothing is configured outside the command line, so a run folder plus its command line fully reproduces the measurement.

#!/usr/bin/env python3
"""Inspect one immutable Nsight SQLite; Perfetto causal analysis is separate."""
import argparse
import collections
import datetime
import hashlib
import json
import sqlite3
from pathlib import Path


def inspect(source, output):
    db = sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True)
    db.text_factory = lambda value: value.decode('utf-8', errors='replace')
    db.row_factory = sqlite3.Row
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    check = db.execute('PRAGMA quick_check').fetchone()[0]
    if check != 'ok':
        raise RuntimeError(check)
    required = {'CUPTI_ACTIVITY_KIND_KERNEL', 'CUPTI_ACTIVITY_KIND_RUNTIME', 'StringIds'}
    if not required <= tables:
        raise RuntimeError('Missing CUDA profile tables: ' + str(required - tables))
    counts = {t: db.execute('SELECT COUNT(*) FROM "' + t + '"').fetchone()[0]
              for t in sorted(tables) if t.startswith('CUPTI_') or t == 'NVTX_EVENTS'}
    if not counts['CUPTI_ACTIVITY_KIND_KERNEL'] or not counts['CUPTI_ACTIVITY_KIND_RUNTIME']:
        raise RuntimeError('Empty CUDA kernel/runtime profile')
    low, high = db.execute('SELECT MIN(start), MAX(end) FROM CUPTI_ACTIVITY_KIND_KERNEL').fetchone()
    columns = {r[1] for r in db.execute('PRAGMA table_info(CUPTI_ACTIVITY_KIND_KERNEL)')}
    name_column = 'demangledName' if 'demangledName' in columns else 'shortName'
    kernels = [dict(r) for r in db.execute(f'''
        SELECT s.value AS name, COUNT(*) AS calls, SUM(k.end-k.start) AS total_ns,
               MIN(k.end-k.start) AS min_ns, MAX(k.end-k.start) AS max_ns,
               AVG(k.end-k.start) AS mean_ns
        FROM CUPTI_ACTIVITY_KIND_KERNEL k JOIN StringIds s ON s.id=k.{name_column}
        GROUP BY s.value ORDER BY total_ns DESC''')]
    streams = [dict(r) for r in db.execute('SELECT p.globalPid,f.value AS filename,c.value AS content FROM ProcessStreams p JOIN StringIds f ON f.id=p.filenameId JOIN StringIds c ON c.id=p.contentId')] if 'ProcessStreams' in tables else []
    output.with_name('application_streams.json').write_text(json.dumps(streams, ensure_ascii=False, indent=2), encoding='utf-8')
    resources = [dict(r) for r in db.execute(f'''SELECT s.value AS name, k.registersPerThread, k.staticSharedMemory,
        k.dynamicSharedMemory, k.localMemoryPerThread, COUNT(*) AS calls
        FROM CUPTI_ACTIVITY_KIND_KERNEL k JOIN StringIds s ON s.id=k.{name_column}
        WHERE LOWER(s.value) LIKE '%delta%'
        GROUP BY s.value,k.registersPerThread,k.staticSharedMemory,k.dynamicSharedMemory,k.localMemoryPerThread''')]
    shapes = [dict(r) for r in db.execute(f'''
        SELECT s.value AS name, k.gridX,k.gridY,k.gridZ,k.blockX,k.blockY,k.blockZ,
               COUNT(*) AS calls, SUM(k.end-k.start) AS total_ns
        FROM CUPTI_ACTIVITY_KIND_KERNEL k JOIN StringIds s ON s.id=k.{name_column}
        WHERE LOWER(s.value) LIKE '%delta%' OR LOWER(s.value) LIKE '%gather%'
           OR LOWER(s.value) LIKE '%get_rows%'
        GROUP BY s.value,k.gridX,k.gridY,k.gridZ,k.blockX,k.blockY,k.blockZ
        ORDER BY total_ns DESC''')]
    diagnostic = [dict(r) for r in db.execute('SELECT severity,text FROM DIAGNOSTIC_EVENT')] if 'DIAGNOSTIC_EVENT' in tables else []
    diagnostic_counts = dict(collections.Counter(r['text'] for r in diagnostic))
    session = dict(db.execute('SELECT * FROM TARGET_INFO_SESSION_START_TIME').fetchone())
    requests_path = source.parent / 'responses' / 'requests.jsonl'
    coverage = None
    if requests_path.exists():
        requests = [json.loads(line) for line in requests_path.read_text(encoding='utf-8-sig').splitlines()]
        final_utc = datetime.datetime.fromisoformat(requests[-1]['finished_utc'])
        end_ns = int(final_utc.timestamp() * 1_000_000_000) - session['utcEpochNs']
        coverage = dict(requests=len(requests), final_request_end_source_ns=end_ns,
                        final_kernel_end_source_ns=high, tail_gap_ns=end_ns-high,
                        tail_within_one_second=end_ns-high < 1_000_000_000)
    digest = hashlib.sha256()
    with source.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            digest.update(block)
    result = dict(source=str(source.resolve()), bytes=source.stat().st_size,
                  sha256=digest.hexdigest(), quick_check=check, activity_counts=counts,
                  kernel_span_ns=[low, high], session=session, diagnostic_counts=diagnostic_counts, request_coverage=coverage, kernel_symbols=kernels, state_kernel_shapes=shapes, state_kernel_resources=resources,
                  interpretation='Raw source-table work totals; not wall-time savings or Perfetto flow attribution')
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    if coverage is not None and not coverage['tail_within_one_second']:
        raise RuntimeError('CUDA trace ends over one second before the final response; profile incomplete')
    print(json.dumps({k: result[k] for k in ('bytes', 'sha256', 'quick_check', 'activity_counts', 'kernel_span_ns')}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    inspect(args.source, args.output)

#!/usr/bin/env python3
"""Audit the requested 46-structure run with eight s3 shards and CPU completion.

This establishes execution/file coverage, not correctness of mathematical GPU
rejections or completeness of the pool/catalogue supplied to the run.
"""
import argparse
import json
from pathlib import Path
from complete_deferred import digest,atomic_json,run as complete_cpu

ROOT=Path(__file__).resolve().parents[1]


def expected_counts(path):
    counts={}
    for line in path.read_text().splitlines():
        fields=[s.strip() for s in line.split('|')]
        if len(fields)>6 and fields[1].isdigit() and fields[2].split()[0].isdigit():
            counts[int(fields[2].split()[0])]=int(fields[5].replace(',',''))
    if set(counts)!=set(range(2,48)):raise ValueError('reference count table must cover all 46 structures')
    return counts


def audit(a):
    root=a.run_root.resolve(strict=True)
    expected=expected_counts(a.counts)
    jobs=[]; identities=None; totals={s:0 for s in range(2,48)}; inputs=[]; deferred=0
    for s in range(2,48):
        n=8 if s==3 else 1
        for k in range(n):
            path=root/f's{s:02d}-shard-{k:03d}-of-{n:03d}'/'job.json'
            j=json.loads(path.read_text()); r=j['gpu']
            if (r['structure_id'],r['shard_count'],r['shard_index'])!=(s,n,k):raise ValueError(f'{path}: wrong job identity')
            if r.get('identity_audited') is not True:raise ValueError('GPU receipt lacks candidate identity audit')
            if identities is None:identities=j['identities']
            if identities!=j['identities']:raise ValueError('jobs used different binaries/pools')
            size=r['scan_space']; start=size*k//n; end=size*(k+1)//n
            if (r['range_start'],r['range_count'],r['scanned_selections'])!=(start,end-start,end-start):raise ValueError('selection range mismatch')
            if r['processed']!=r['generated'] or r['accepted']+r['rejected']+r['deferred']!=r['processed']:raise ValueError('GPU accounting mismatch')
            for key,count in [('accepted',r['accepted']),('deferred',r['deferred'])]:
                f=Path(j[key]['path'])
                if j[key]['rows']!=count or digest(f)!=j[key]['sha256']:raise ValueError(f'{f}: missing/corrupted output')
            jobs.append(dict(receipt=str(path),sha256=digest(path)))
            totals[s]+=r['generated']; deferred+=r['deferred']; inputs.append(Path(j['deferred']['path']))
    if totals!=expected:raise ValueError(f'candidate count regression: {[(s,totals[s],expected[s]) for s in totals if totals[s]!=expected[s]]}')
    # Check the eight records actually refer to the same s3 selection space.
    s3=[json.loads(Path(j['receipt']).read_text())['gpu']['scan_space'] for j in jobs if Path(j['receipt']).parent.name.startswith('s03-')]
    if len(set(s3))!=1:raise ValueError('s3 shards used different selection spaces')
    ledger=dict(version=1,jobs=jobs,job_count=len(jobs),identities=identities,generated=sum(totals.values()),
                deferred=deferred,counts_reference_sha256=digest(a.counts),
                status='GPU execution coverage accounted; not a mathematical no-omissions certificate')
    atomic_json(root/'gpu-coverage.json',ledger)
    if a.cpu_driver and not (root/'cpu-completion'/'completion.json').exists():
        complete_cpu(argparse.Namespace(driver=a.cpu_driver,work_dir=root/'cpu-completion',workers=a.workers,
                     chunk_rows=a.chunk_rows,expected_rows=deferred,inputs=inputs))
    cpu=root/'cpu-completion'/'completion.json'
    if a.require_cpu or a.cpu_driver:
        def verify_cpu(path, expected_rows, expected_inputs):
            c=json.loads(path.read_text())
            if identities.get('cpu_driver') and c.get('driver_sha256')!=identities['cpu_driver']:
                raise ValueError('CPU completion used a different driver')
            if c['expected_rows']!=expected_rows or c['processed']!=expected_rows or c['accepted']+c['rejected']!=expected_rows:
                raise ValueError('CPU completion does not account for every deferred record')
            if [i['path'] for i in c['inputs']]!=[str(p.resolve()) for p in expected_inputs]:
                raise ValueError('CPU completion used a different deferred input list')
            for i in c['inputs']:
                if digest(Path(i['path']))!=i['sha256']:raise ValueError('CPU input changed')
            if digest(Path(c['accepted_output']))!=c['output_sha256']:raise ValueError('CPU accepted output changed')
            return dict(receipt=str(path),sha256=digest(path),processed=c['processed'])
        if cpu.exists():
            ledger['cpu_completion']=verify_cpu(cpu,deferred,inputs)
        else:
            per_job=[]
            for record in jobs:
                job=json.loads(Path(record['receipt']).read_text())
                binding=job.get('cpu_completion')
                if not binding:raise ValueError('a shard has no CPU-completion binding')
                path=Path(binding['receipt'])
                if digest(path)!=binding['sha256']:raise ValueError('CPU receipt changed')
                per_job.append(verify_cpu(path,job['gpu']['deferred'],[Path(job['deferred']['path'])]))
            ledger['cpu_completion']=per_job
        atomic_json(root/'execution-coverage.json',ledger)
    print(json.dumps(ledger,sort_keys=True))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run-root',type=Path,required=True)
    p.add_argument('--counts',type=Path,default=ROOT/'CWS_TYPE_COUNTS.md')
    p.add_argument('--cpu-driver',type=Path,help='after GPU audit, complete every deferred file')
    p.add_argument('--require-cpu',action='store_true',help='require and verify an existing CPU completion receipt')
    p.add_argument('--workers',type=int,default=1)
    p.add_argument('--chunk-rows',type=int,default=20000)
    try:audit(p.parse_args())
    except Exception as e:raise SystemExit(f'Run coverage INCOMPLETE: {e}')


if __name__=='__main__':main()

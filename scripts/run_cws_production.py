#!/usr/bin/env python3
"""Run structures 2..47 with eight structure-3 shards and mandatory CPU completion.

One sequential worker owns each visible CUDA device. The final coverage audit
runs only after all 53 GPU/CPU jobs succeed. Execution accounting is not a
mathematical certificate of the GPU rejection implementation.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import subprocess
import sys
import threading

from complete_deferred import atomic_json, digest

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Job:
    structure_id: int
    shard_count: int
    shard_index: int
    cuda_device: int

    @property
    def name(self):
        return f's{self.structure_id:02d}-shard-{self.shard_index:03d}-of-{self.shard_count:03d}'


def plan_jobs(devices):
    if len(devices) != 8 or len(set(devices)) != 8 or any(device < 0 for device in devices):
        raise ValueError('exactly eight distinct nonnegative CUDA device IDs are required')
    identities = [(3, 8, shard) for shard in range(8)]
    identities += [(structure, 1, 0) for structure in range(2, 48) if structure != 3]
    return [Job(*identity, devices[index % 8]) for index, identity in enumerate(identities)]


def job_command(args, job):
    command = [sys.executable, str(ROOT / 'scripts/run_cws_job.py'),
               '--binary', str(args.binary), '--run-root', str(args.run_root),
               '--weights-dir', str(args.weights_dir), '--cpu-driver', str(args.cpu_driver)]
    for key in ('structure_id', 'shard_count', 'shard_index', 'cuda_device'):
        command += ['--' + key.replace('_', '-'), str(getattr(job, key))]
    for key in ('chunk_capacity', 'slice_capacity', 'np_cap', 'threads', 'blocks',
                'cpu_workers', 'cpu_chunk_rows'):
        command += ['--' + key.replace('_', '-'), str(getattr(args, key))]
    return command


def run(args, invoke=subprocess.run):
    jobs = plan_jobs(args.devices)
    if min(args.chunk_capacity, args.slice_capacity, args.np_cap, args.threads,
           args.cpu_workers, args.cpu_chunk_rows) < 1 or args.blocks < 0:
        raise ValueError('capacities, threads and CPU worker/chunk counts must be positive; blocks may be zero')
    for key in ('binary', 'cpu_driver', 'weights_dir', 'counts'):
        setattr(args, key, getattr(args, key).resolve(strict=True))
    args.run_root = args.run_root.resolve()
    def identities():
        return dict(binary=digest(args.binary), cpu_driver=digest(args.cpu_driver),
                    pools={name: digest(args.weights_dir / name) for name in ('2d.txt', '3d.txt', '4d.txt')},
                    counts_reference=digest(args.counts))
    before = identities()
    manifest = dict(version=1, jobs=[dict(asdict(job), name=job.name, command=job_command(args, job)) for job in jobs],
                    identities=before, structure_1='already complete according to user; outside this run')
    if args.dry_run:
        print(json.dumps(manifest, sort_keys=True, indent=2))
        return manifest
    # A fresh run never reuses stale completion markers or overwrites old output.
    args.run_root.mkdir(parents=True, exist_ok=False)
    logs = args.run_root / 'scheduler-logs'
    logs.mkdir()
    atomic_json(args.run_root / 'production-plan.json', manifest)
    stop = threading.Event()
    lock = threading.Lock()
    completed, failed, started = [], [], []

    def worker(device):
        for job in (job for job in jobs if job.cuda_device == device):
            with lock:
                if stop.is_set():
                    return
                started.append(job.name)
                print(f'GPU {device}: starting {job.name}', flush=True)
            try:
                with (logs / f'{job.name}.stdout.log').open('wb') as out, \
                     (logs / f'{job.name}.stderr.log').open('wb') as err:
                    result = invoke(job_command(args, job), stdout=out, stderr=err)
                if result.returncode:
                    raise RuntimeError(f'job process exited {result.returncode}')
                with lock:
                    completed.append(job.name)
                    print(f'GPU {device}: GPU and CPU completion finished for {job.name}', flush=True)
            except Exception as error:
                with lock:
                    stop.set()
                    failed.append(dict(job=job.name, error=str(error)))
                    print(f'GPU {device}: INCOMPLETE {job.name}: {error}', file=sys.stderr, flush=True)
                return

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, device) for device in args.devices]
        for future in futures:
            future.result()
    status = dict(version=1, expected_jobs=len(jobs), completed=sorted(completed), failed=failed,
                  unstarted=sorted(job.name for job in jobs if job.name not in started),
                  status='incomplete; originals and per-job logs retained')
    atomic_json(args.run_root / 'scheduler-status.json', status)
    if failed or len(completed) != len(jobs):
        raise RuntimeError(f'{len(completed)}/53 GPU/CPU jobs completed; no final audit or completion marker written')
    if identities() != before:
        raise RuntimeError('binary, CPU driver, pools or reference counts changed during production')
    audit_command = [sys.executable, str(ROOT / 'scripts/audit_cws_run.py'),
                     '--run-root', str(args.run_root), '--counts', str(args.counts), '--require-cpu']
    with (logs / 'final-audit.stdout.log').open('wb') as out, (logs / 'final-audit.stderr.log').open('wb') as err:
        result = invoke(audit_command, stdout=out, stderr=err)
    if result.returncode:
        raise RuntimeError(f'final execution coverage audit failed ({result.returncode}); no completion marker written')
    if identities() != before:
        raise RuntimeError('binary, CPU driver, pools or reference counts changed during final audit')
    coverage = args.run_root / 'execution-coverage.json'
    if not coverage.is_file():
        raise RuntimeError('final auditor produced no execution-coverage.json')
    completion = dict(version=1, job_count=len(completed), identities=before,
                      execution_coverage=dict(path=str(coverage), sha256=digest(coverage)),
                      status='all 53 GPU/CPU jobs execution-accounted; formal proof status remains separate')
    atomic_json(args.run_root / 'production-execution.json', completion)
    status['status'] = completion['status']
    atomic_json(args.run_root / 'scheduler-status.json', status)
    print(json.dumps(completion, sort_keys=True))
    return completion


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--cpu-driver', type=Path, required=True)
    parser.add_argument('--run-root', type=Path, required=True, help='new output directory; must not already exist')
    parser.add_argument('--weights-dir', type=Path, default=ROOT / 'docs/data/single-weight-systems')
    parser.add_argument('--counts', type=Path, default=ROOT / 'CWS_TYPE_COUNTS.md')
    parser.add_argument('--devices', type=int, nargs='+', default=list(range(8)))
    parser.add_argument('--chunk-capacity', type=int, default=100000)
    parser.add_argument('--slice-capacity', type=int, default=10000)
    parser.add_argument('--np-cap', type=int, default=256)
    parser.add_argument('--threads', type=int, default=128)
    parser.add_argument('--blocks', type=int, default=0)
    parser.add_argument('--cpu-workers', type=int, default=1)
    parser.add_argument('--cpu-chunk-rows', type=int, default=20000)
    parser.add_argument('--dry-run', action='store_true', help='print all 53 commands; launch nothing and create no files')
    try:
        run(parser.parse_args())
    except Exception as error:
        raise SystemExit(f'Production execution INCOMPLETE: {error}')


if __name__ == '__main__':
    main()

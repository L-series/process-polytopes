#!/usr/bin/env python3
"""Run one integration CUDA shard, audit its receipt, and finish every deferred row."""
import argparse
import json
from pathlib import Path
import subprocess
from complete_deferred import digest, atomic_json, run as complete_cpu


def file_record(path):
    with path.open('rb') as stream:
        rows = sum(1 for _ in stream)
    return dict(path=str(path), sha256=digest(path), rows=rows)


def run(args):
    if not 2 <= args.structure_id <= 47 or args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        raise ValueError('invalid structure/shard')
    binary = args.binary.resolve(strict=True)
    weights = args.weights_dir.resolve(strict=True)
    driver = args.cpu_driver.resolve(strict=True)
    paths = [weights / name for name in ('2d.txt', '3d.txt', '4d.txt')]
    def identities():
        return dict(binary=digest(binary), cpu_driver=digest(driver), pools={p.name: digest(p) for p in paths})
    before = identities()
    job = args.run_root.resolve() / f's{args.structure_id:02d}-shard-{args.shard_index:03d}-of-{args.shard_count:03d}'
    job.mkdir(parents=True, exist_ok=False)
    accepted, deferred = job/'accepted.txt', job/'deferred.ovf'
    command = [str(binary), '--structure-id', str(args.structure_id), '--shard-count', str(args.shard_count),
               '--shard-index', str(args.shard_index), '--weights-dir', str(weights),
               '--chunk-capacity', str(args.chunk_capacity), '--slice-capacity', str(args.slice_capacity),
               '--np-cap', str(args.np_cap), '--threads', str(args.threads), '--blocks', str(args.blocks),
               '--cuda-device', str(args.cuda_device), '--accepted-output', str(accepted),
               '--deferred-output', str(deferred)]
    atomic_json(job/'invocation.json', dict(command=command, identities=before))
    with (job/'stdout.log').open('wb') as out, (job/'stderr.log').open('wb') as err:
        result = subprocess.run(command, stdout=out, stderr=err)
    if result.returncode:
        raise RuntimeError(f'GPU process failed ({result.returncode}); incomplete job retained at {job}')
    receipts = []
    with (job/'stderr.log').open() as stream:
        for line in stream:
            if line.startswith('CWS_GPU_COMPLETION_V1 '):
                receipts.append(json.loads(line.split(' ', 1)[1]))
    if len(receipts) != 1:
        raise RuntimeError('GPU binary must produce exactly one checked completion receipt')
    receipt = receipts[0]
    for key, value in dict(structure_id=args.structure_id, shard_count=args.shard_count,
                           shard_index=args.shard_index, identity_audited=True).items():
        if receipt.get(key) != value:
            raise RuntimeError('GPU receipt does not match this audited shard')
    size = receipt['scan_space']
    start, end = size*args.shard_index//args.shard_count, size*(args.shard_index+1)//args.shard_count
    if (receipt['range_start'], receipt['range_count'], receipt['scanned_selections']) != (start, end-start, end-start):
        raise RuntimeError('GPU selection range mismatch')
    if receipt['generated'] != receipt['processed'] or sum(receipt[k] for k in ('accepted', 'rejected', 'deferred')) != receipt['processed']:
        raise RuntimeError('GPU terminal accounting mismatch')
    ar, dr = file_record(accepted), file_record(deferred)
    if (ar['rows'], dr['rows']) != (receipt['accepted'], receipt['deferred']):
        raise RuntimeError('output row counts disagree with GPU receipt')
    if before != identities():
        raise RuntimeError('binary or pools changed during the run')
    gpu_job = dict(version=1, gpu=receipt, identities=before, command=command, accepted=ar, deferred=dr,
                   status='GPU execution accounted; CPU completion pending')
    atomic_json(job/'job.json', gpu_job)
    complete_cpu(argparse.Namespace(driver=driver, work_dir=job/'cpu-completion', inputs=[deferred],
                 expected_rows=receipt['deferred'], chunk_rows=args.cpu_chunk_rows, workers=args.cpu_workers))
    cpu = job/'cpu-completion/completion.json'
    gpu_job['cpu_completion'] = dict(receipt=str(cpu), sha256=digest(cpu))
    gpu_job['status'] = 'GPU and deferred CPU execution accounted; see formal proof status'
    atomic_json(job/'job.json', gpu_job)
    print(json.dumps(gpu_job, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--run-root', type=Path, required=True)
    parser.add_argument('--weights-dir', type=Path, default=Path(__file__).resolve().parents[1]/'docs/data/single-weight-systems')
    parser.add_argument('--cpu-driver', type=Path, required=True)
    parser.add_argument('--structure-id', type=int, required=True)
    parser.add_argument('--shard-count', type=int, default=1)
    parser.add_argument('--shard-index', type=int, default=0)
    parser.add_argument('--chunk-capacity', type=int, default=100000)
    parser.add_argument('--slice-capacity', type=int, default=10000)
    parser.add_argument('--np-cap', type=int, default=256)
    parser.add_argument('--threads', type=int, default=128)
    parser.add_argument('--blocks', type=int, default=0)
    parser.add_argument('--cuda-device', type=int, default=0)
    parser.add_argument('--cpu-workers', type=int, default=1)
    parser.add_argument('--cpu-chunk-rows', type=int, default=20000)
    try:
        run(parser.parse_args())
    except Exception as error:
        raise SystemExit(f'GPU/CPU job INCOMPLETE: {error}')


if __name__ == '__main__':
    main()

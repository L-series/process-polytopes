#!/usr/bin/env python3
"""Startup-inclusive CPU descriptor throughput, including IP/dual work and output."""
import argparse
import hashlib
import json
import pathlib
import platform
import re
import resource
import statistics
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=pathlib.Path, default=ROOT / 'build/cws.x')
    parser.add_argument('--weights-dir', type=pathlib.Path, default=ROOT / 'docs/data/single-weight-systems')
    parser.add_argument('--types', type=int, nargs='+', default=[5, 7])
    parser.add_argument('--repetitions', type=int, default=7)
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error('repetitions must be positive')
    binary, directory = args.binary.resolve(), args.weights_dir.resolve()
    _, hard = resource.getrlimit(resource.RLIMIT_STACK)
    resource.setrlimit(resource.RLIMIT_STACK, (hard, hard))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    cpu = next((line.split(':', 1)[1].strip()
                for line in pathlib.Path('/proc/cpuinfo').read_text().splitlines()
                if line.startswith('model name')), platform.machine())
    report = {'binary': str(binary), 'binary_sha256': sha256(binary), 'cpu': cpu,
              'host_processes': 1, 'platform': platform.platform(), 'warmups': 1,
              'boundary': 'pool loading, selection, generation, IP, dual data, output to file; no NF',
              'inputs': {p.name: sha256(p) for p in sorted(directory.glob('*d.txt'))},
              'sources': {str(p.relative_to(ROOT)): sha256(p) for p in [
                  *[ROOT / 'PALP' / name for name in ['cws.c', 'cws_generate.h',
                     'Coord.c', 'Rat.c', 'Vertex.c', 'Polynf.c', 'LG.c', 'Global.h', 'LG.h', 'Rat.h']],
                  *sorted((ROOT / 'src/cws').glob('*.[ch]')), ROOT / 'Makefile']}, 'results': []}
    for descriptor in args.types:
        timings, counts, output_hashes = [], [], set()
        for iteration in range(args.repetitions + 1):
            with tempfile.TemporaryFile() as output:
                start = time.perf_counter()
                result = subprocess.run([str(binary), '-n5', f'-s{descriptor}', '--weights-dir', str(directory)],
                                        stdout=output, stderr=subprocess.PIPE, text=True, check=True)
                elapsed = time.perf_counter() - start
                match = re.fullmatch(r'CWS candidates: (\d+)\n', result.stderr)
                if not match:
                    raise RuntimeError(result.stderr)
                output.seek(0)
                data = output.read()
            output_hashes.add(hashlib.sha256(data).hexdigest())
            counts.append((int(match[1]), data.count(b'\n')))
            if iteration:
                timings.append(elapsed)
        if len(set(counts)) != 1 or len(output_hashes) != 1:
            raise RuntimeError('Run-to-run output or count mismatch')
        candidates, accepted = counts[0]
        median = statistics.median(timings)
        report['results'].append({'type': descriptor, 'candidates': candidates, 'accepted': accepted,
                                  'seconds': timings, 'median_seconds': median,
                                  'candidate_cws_per_second': candidates / median,
                                  'output_sha256': next(iter(output_hashes))})
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()

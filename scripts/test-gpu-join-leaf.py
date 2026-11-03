#!/usr/bin/env python3
"""CBMC: the join's leaf accumulation and verdict equal their specification
(verification/gpu/join_leaf.cpp) on symbolic inputs."""
from pathlib import Path
import argparse
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
CHECKS = ['--bounds-check', '--pointer-check', '--signed-overflow-check', '--unwinding-assertions']


def run(defines, label, unwind, *, mutation=False, expected_failure=None):
    with tempfile.TemporaryDirectory(prefix='join-leaf-') as directory:
        header = Path(directory) / 'join.h'
        subprocess.run([sys.executable, str(ROOT / 'verification/gpu/extract.py'), str(header),
                        'struct SelectedEntry', 'struct DeviceCwsCandidate', 'struct GJoinKey', 'struct GJoinIdx',
                        'struct GJoinTable', 'struct GJoinPlan', 'struct GJoinAcc',
                        '__device__ bool device_gjoin_leaf(', '__device__ int device_gjoin_verdict('], check=True)
        source = Path(directory) / 'check.cpp'
        production = header.read_text().replace('#pragma once', '')
        if mutation:
            assert production.count('acc.tz |= m0;') == 1
            production = production.replace('acc.tz |= m0;', 'acc.tz |= m1;')
        source.write_text('#define nullptr 0\n'
                          'inline int __ffs(unsigned int x) { if (!x) return 0; int i=1; '
                          'if (!(x & 65535u)) { x >>= 16; i += 16; } '
                          'if (!(x & 255u)) { x >>= 8; i += 8; } '
                          'if (!(x & 15u)) { x >>= 4; i += 4; } '
                          'if (!(x & 3u)) { x >>= 2; i += 2; } '
                          'return i + !(x & 1u); }\n'
                          + production
                          + (ROOT / 'verification/gpu/join_leaf.cpp').read_text())
        binary = Path(directory) / 'check.gb'
        subprocess.run(['goto-cc', '-DPOLY_Dmax=5', *defines, '-I', str(ROOT / 'src/gpu'), str(source), '-o', str(binary)],
                       check=True)
        result = subprocess.run(['cbmc', str(binary), *CHECKS, '--slice-formula', '--unwind', str(unwind)], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=1200)
        if expected_failure:
            failures = [l for l in result.stdout.splitlines() if 'FAILURE' in l]
            if result.returncode != 10 or not any(expected_failure in l for l in failures):
                print(result.stdout[-3000:])
                raise SystemExit(f'{label}: missing expected counterexample')
            print(f'{label}: expected counterexample found')
            return
        if result.returncode != 0 or 'VERIFICATION SUCCESSFUL' not in result.stdout:
            print('\n'.join(l for l in result.stdout.splitlines() if 'FAILURE' in l) or result.stdout[-3000:])
            raise SystemExit(f'{label}: NOT verified')
        print(f'{label}: VERIFICATION SUCCESSFUL')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, nargs='+', choices=range(1, 6), default=[1, 2, 3, 4, 5])
    parser.add_argument('--verdict-only', action='store_true')
    parser.add_argument('--groups', type=int, nargs='+', choices=range(5), default=list(range(5)))
    parser.add_argument('--skip-verdict', action='store_true')
    controls = parser.add_mutually_exclusive_group()
    controls.add_argument('--nonvacuity', action='store_true')
    controls.add_argument('--mutation', action='store_true')
    args = parser.parse_args()
    run(['-DFFS'], 'CUDA ffs shim (all 32-bit words)', 2)
    if args.nonvacuity:
        run(['-DLEAF', '-DJOIN_ROWS=5', '-DMASK_GROUP=0', '-DNONVACUITY'],
            'Join leaf nonvacuity', 12, expected_failure='reachable conclusive leaf')
        raise SystemExit(0)
    if args.mutation:
        run(['-DLEAF', '-DJOIN_ROWS=1', '-DMASK_GROUP=0'],
            'Join leaf mutation (tz uses m1)', 12, mutation=True, expected_failure='tz: some X_A == 0')
        raise SystemExit(0)
    for rows in ([] if args.verdict_only else args.rows):
        for group in args.groups:
            run(['-DLEAF', f'-DJOIN_ROWS={rows}', f'-DMASK_GROUP={group}'],
                f'Join leaf ({rows} rows, mask group {group}, symbolic layout/masks, unit counts)', 12)
    if not args.skip_verdict:
        run(['-DVERDICT'], 'Join verdict (symbolic accumulator, 1..10 coordinates)', 12)

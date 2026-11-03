#!/usr/bin/env python3
"""CBMC soundness of the actual mod-2 rank checker on all 5x10 parity matrices."""
import argparse
from pathlib import Path
import subprocess
import tempfile
ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--nonvacuity', action='store_true')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='walk-rank-') as directory:
        directory = Path(directory)
        header = directory / 'rank.h'
        subprocess.run(['python3', str(ROOT / 'verification/gpu/extract.py'), str(header),
                        'struct DeviceCwsCandidate', '__device__ long long device_walk_add(',
                        '__device__ long long device_walk_mul(', '__device__ int device_walk_basis_certificate('], check=True)
        source = directory / 'rank.cpp'
        source.write_text('#define FP_NMAX 5\nextern "C" {\n' + header.read_text().replace('#pragma once','') + '\n}\n'
                          + (ROOT / 'verification/gpu/walk_rank.cpp').read_text())
        binary = directory / 'rank.gb'
        subprocess.run(['goto-cc','-DPOLY_Dmax=5', *(['-DWALK_NONVACUITY'] if args.nonvacuity else []),
                        '-I', str(ROOT / 'src/gpu'), str(source), '-o', str(binary)], check=True)
        try:
            result = subprocess.run(['cbmc', str(binary), '--bounds-check', '--pointer-check', '--signed-overflow-check',
                                     '--div-by-zero-check', '--unwind', '11', '--unwinding-assertions', '--slice-formula'],
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=args.timeout)
        except subprocess.TimeoutExpired:
            raise SystemExit('Rank check timed out; NOT verified') from None
        success = result.returncode == 0 and 'VERIFICATION SUCCESSFUL' in result.stdout
        if args.nonvacuity:
            failures = [line for line in result.stdout.splitlines() if line.endswith(': FAILURE')]
            success = result.returncode == 10 and bool(failures) and all('nonvacuity:' in line for line in failures)
        if not success:
            print(result.stdout)
            raise SystemExit(f'Rank check failed (exit {result.returncode})')
        print('Rank certificate (all 5x10 parity matrices): ' + ('nonempty witness' if args.nonvacuity else 'VERIFICATION SUCCESSFUL'))


if __name__ == '__main__':
    main()

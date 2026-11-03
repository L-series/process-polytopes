#!/usr/bin/env python3
"""Verify the actual mutable key matcher over the complete CUDA arity bounds.

Requires cbmc/goto-cc on PATH (nix develop .#gpu-verify). The source's fixed
bounds are exhaustive here: k=0..4, coordinates=0..9, full machine payloads.
This does not verify traversal, table construction, or CUDA compilation.
"""
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
    with tempfile.TemporaryDirectory(prefix='join-match-') as directory:
        header = Path(directory) / 'match.h'
        subprocess.run(['python3', str(ROOT / 'verification/gpu/extract.py'), str(header),
                        '__device__ bool device_gjoin_match_key('], check=True)
        source = Path(directory) / 'match.cpp'
        source.write_text('extern "C" {\n' + header.read_text().replace('#pragma once', '')
                          + '\n}\n' + (ROOT / 'verification/gpu/join_match.cpp').read_text())
        binary = Path(directory) / 'match.gb'
        subprocess.run(['goto-cc', '-DPOLY_Dmax=5',
                        *(['-DJOIN_NONVACUITY'] if args.nonvacuity else []),
                        '-I', str(ROOT / 'src/gpu'), str(source), '-o', str(binary)], check=True)
        try:
            result = subprocess.run(['cbmc', str(binary), '--bounds-check', '--pointer-check',
                                     '--signed-overflow-check', '--div-by-zero-check',
                                     '--unwind', '11', '--unwinding-assertions', '--slice-formula'],
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    timeout=args.timeout)
        except subprocess.TimeoutExpired:
            raise SystemExit('Mutable join matcher: timed out; NOT verified') from None
        success = result.returncode == 0 and 'VERIFICATION SUCCESSFUL' in result.stdout
        if args.nonvacuity:
            failures = [line for line in result.stdout.splitlines() if line.endswith(': FAILURE')]
            success = result.returncode == 10 and bool(failures) and all(
                'nonvacuity:' in line for line in failures)
        if not success:
            print(result.stdout)
            raise SystemExit(f'Mutable join matcher failed (exit {result.returncode})')
        print('Mutable join matcher: ' + ('nonempty success witness' if args.nonvacuity else 'VERIFICATION SUCCESSFUL'))


if __name__ == '__main__':
    main()

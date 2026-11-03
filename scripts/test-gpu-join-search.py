#!/usr/bin/env python3
"""Bounded source-level join traversal check. Requires cbmc/goto-cc on PATH.

Run in `nix develop .#gpu-verify`. This verifies the stated finite bounds,
not arbitrary production table sizes or CUDA compilation/scheduling.
"""
import argparse
import re
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--range', action='store_true', dest='range_only',
                        help='check full-width four-position range search at positions 0 and 2')
    parser.add_argument('--rows', type=int, choices=(2, 3), default=3)
    parser.add_argument('--max-keys', type=int, choices=(1, 2), default=1,
                        help='maximum records per row in DFS check (default: 1)')
    parser.add_argument('--weighted', action='store_true',
                        help='also quantify each fibre count over 1..3 (more expensive)')
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--shape', type=int, choices=range(4), action='append')
    parser.add_argument('--nonvacuity', action='store_true')
    parser.add_argument('--mutation', action='store_true',
                        help='shape 0 only: require a counterexample after deleting a scan range')
    args = parser.parse_args()
    if args.range_only and (args.shape or args.mutation):
        parser.error('--range excludes --shape and --mutation')
    if args.mutation and (args.nonvacuity or args.shape not in (None, [0])):
        parser.error('--mutation supports shape 0 alone and excludes --nonvacuity')
    with tempfile.TemporaryDirectory(prefix='join-search-') as directory:
        header = Path(directory) / 'join.h'
        subprocess.run(['python3', str(ROOT / 'verification/gpu/extract.py'), str(header),
                        'struct SelectedEntry', 'struct DeviceCwsCandidate',
                        'struct GJoinKey', 'struct GJoinIdx', 'struct GJoinTable', 'struct GJoinPlan',
                        '__device__ void device_gjoin_begin_level(',
                        '__device__ bool device_gjoin_match_key(',
                        '__device__ int device_gjoin_decide('], check=True)
        source = Path(directory) / 'check.cpp'
        # C linkage avoids CBMC loop IDs containing commas from C++ signatures.
        # The bodies remain verbatim; the two intrinsics use C linkage as well.
        source.write_text('// CBMC C++ frontend: CUDA intrinsic and null-pointer spelling only.\n'
                          '#define nullptr 0\n'
                          'inline int __ffs(unsigned int x) { if (!x) return 0; int i=1; '
                          'while (!(x & 1u)) { x >>= 1; ++i; } return i; }\n'
                          + 'extern \"C\" {\n' + header.read_text().replace('#pragma once', '') + '\n}\n'
                          + (ROOT / 'verification/gpu' / ('join_range.cpp' if args.range_only else 'join_search.cpp')).read_text())
        if args.mutation:
            text = source.read_text()
            if text.count('end[j] = lo;') != 2:
                raise SystemExit('Mutation target changed; inspect source')
            source.write_text(text.replace('end[j] = lo;', 'end[j] = it[j];'))
        shapes = [0, 2] if args.range_only else (args.shape or ([0] if args.mutation else range(4)))
        for shape in shapes:
            binary = Path(directory) / f'check-{shape}.gb'
            subprocess.run(['goto-cc', '-DPOLY_Dmax=5', f'-DJOIN_SHAPE={shape}',
                            f'-DJOIN_POSITION={shape}', f'-DJOIN_MAX_KEYS={args.max_keys}',
                            f'-DJOIN_ROWS={args.rows}',
                            *(['-DJOIN_NONVACUITY'] if args.nonvacuity else []),
                            *(['-DJOIN_WEIGHTED'] if args.weighted else []),
                            '-I', str(ROOT / 'src/gpu'), str(source), '-o', str(binary)], check=True)
            loops = subprocess.run(['cbmc', str(binary), '--show-loops'], check=True,
                                   text=True, stdout=subprocess.PIPE).stdout
            depth_line = source.read_text().split('    while (depth >= 0) {')[0].count('\n') + 1
            matches = [name for name, line in re.findall(
                r'Loop (.*):\n  file .* line (\d+) function device_gjoin_decide', loops)
                if int(line) == depth_line]
            if len(matches) != 1:
                raise SystemExit('Cannot identify production DFS loop for bounded unwinding')
            final_line = source.read_text().split('    for (int A = 0; A < N; ++A) {')[0].count('\n') + 1
            final = [name for name, line in re.findall(
                r'Loop (.*):\n  file .* line (\d+) function device_gjoin_decide', loops)
                if int(line) == final_line]
            if len(final) != 1:
                raise SystemExit('Cannot identify production verdict loop')
            dfs_bound = 8 if args.max_keys == 1 else (10 if args.rows == 2 else 22)
            unwindset = f'{matches[0]}:{dfs_bound},{final[0]}:5'
            command = ['cbmc', str(binary), '--bounds-check', '--pointer-check',
                                     '--signed-overflow-check', '--div-by-zero-check',
                                     '--unwind', '6' if args.range_only else '4', '--unwindset', unwindset,
                                     '--unwinding-assertions', '--slice-formula', '--symex-cache-dereferences']
            try:
                result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, timeout=args.timeout)
            except subprocess.TimeoutExpired as error:
                output = error.stdout or b''
                print(output.decode(errors='replace') if isinstance(output, bytes) else output)
                raise SystemExit(f'join search shape {shape}: timed out; NOT verified') from None
            success = result.returncode == 0 and 'VERIFICATION SUCCESSFUL' in result.stdout
            if args.nonvacuity:
                success = result.returncode == 10 and 'nonvacuity: a nonempty join is reachable: FAILURE' in result.stdout
                # A nonvacuity witness must not hide another failed property.
                success = success and all('nonvacuity:' in line for line in result.stdout.splitlines()
                                          if line.endswith(': FAILURE'))
            if args.mutation:
                failures = [line for line in result.stdout.splitlines() if line.endswith(': FAILURE')]
                success = result.returncode == 10 and bool(failures) and all(
                    'join count equals brute-force compatible fibre product' in line for line in failures)
            if not success:
                print(result.stdout)
                raise SystemExit(f'join search shape {shape} failed (exit {result.returncode})')
            label = 'mutation detected' if args.mutation else (
                'nonempty witness' if args.nonvacuity else 'VERIFICATION SUCCESSFUL')
            bounds = 'four packed 16-bit positions, <=2 keys' if args.range_only else (
                f'{args.rows} rows, <={args.max_keys} keys/row, '
                f'counts {"1..3" if args.weighted else "1"}')
            print(f'Join {"range position" if args.range_only else "search shape"} '
                  f'{shape} ({bounds}): {label}', flush=True)


if __name__ == '__main__':
    main()

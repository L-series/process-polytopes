#!/usr/bin/env python3
"""CBMC: the extracted weight-based prefix matching loop is a bijection.

The finite limit four is enforced by validate_gjoin_plan; weights range over
all signed ints and coordinates over all injective prefixes of ten positions.
The resulting weight-preserving bijection is the premise of
JoinLeafState.matched_weights_equation. No symbolic multiplication or division.
"""
from pathlib import Path
import subprocess
import sys
import tempfile
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]


def main():
    source = (ROOT / 'src/gpu/join.cuh').read_text()
    decide = block(source, '__device__ int device_gjoin_decide(')
    matching = decide[decide.index('unsigned int used = 0;'):decide.index('kb[s] =')]
    with tempfile.TemporaryDirectory(prefix='join-prefix-') as directory:
        header = Path(directory) / 'join.h'
        subprocess.run([sys.executable, str(ROOT / 'verification/gpu/extract.py'), str(header),
                        'struct SelectedEntry', 'struct DeviceCwsCandidate', 'struct GJoinKey',
                        'struct GJoinIdx', 'struct GJoinTable', 'struct GJoinPlan'], check=True)
        check = Path(directory) / 'check.cpp'
        check.write_text('#define nullptr 0\n' + header.read_text().replace('#pragma once', '') +
                         '\nint match_prefix(const GJoinPlan &plan, const SelectedEntry &E, '
                         'const DeviceCwsCandidate &c, int k, int epos_coord[kMaxSlots][kMaxSize], '
                         'unsigned int &used_out) { const int s = 0;\n' + matching +
                         '\nused_out = used; return 0; }\n' +
                         (ROOT / 'verification/gpu/join_prefix.cpp').read_text())
        binary = Path(directory) / 'check.gb'
        subprocess.run(['goto-cc', '-DPOLY_Dmax=5', '-I', str(ROOT / 'src/gpu'),
                        str(check), '-o', str(binary)], check=True)
        result = subprocess.run(['cbmc', str(binary), '--bounds-check', '--pointer-check',
                                 '--signed-overflow-check', '--unwinding-assertions', '--unwind', '12',
                                 '--slice-formula'], text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=180)
        if result.returncode != 0 or 'VERIFICATION SUCCESSFUL' not in result.stdout:
            print('\n'.join(l for l in result.stdout.splitlines() if 'FAILURE' in l) or result.stdout[-3000:])
            raise SystemExit('Join prefix matching: NOT verified')
        print('Join prefix matching: VERIFICATION SUCCESSFUL (all layouts and signed weights, 0..4 shared positions)')


if __name__ == '__main__':
    main()

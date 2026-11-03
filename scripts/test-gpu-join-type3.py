#!/usr/bin/env python3
"""Small extracted-source CBMC refinements for fused type-3 masks and matching."""
from pathlib import Path
import subprocess
import sys
import tempfile
from test_gpu_ip_source import block
ROOT=Path(__file__).resolve().parents[1]

def run(mode):
    with tempfile.TemporaryDirectory(prefix='join-type3-') as d:
        d=Path(d); h=d/'join.h'
        subprocess.run([sys.executable,str(ROOT/'verification/gpu/extract.py'),str(h),
            'struct SelectedEntry','struct DeviceCwsCandidate','struct GJoinKey','struct GJoinIdx',
            'struct GJoinTable','struct GJoinPlan','struct GJoinAcc','struct JoinKey',
            '__device__ bool device_gjoin_leaf(', '__device__ int device_gjoin_verdict(',
            '__device__ __forceinline__ unsigned long long device_spread7(',
            '__device__ __forceinline__ void device_join_cert_bits(',
            '__device__ __forceinline__ unsigned long long device_join_pack('],check=True)
        decide=block((ROOT/'src/gpu/join_type3.cuh').read_text(),'__device__ int device_join_decide(')
        verdict=decide[decide.rindex('*np_out = np;'):].rsplit('}',1)[0].replace('constexpr','const')
        matching=decide[decide.index('unsigned int used = 0;'):decide.index('const unsigned int lb =')]
        # The actual perm initializer belongs to the caller, so copy its values.
        match='void fused_match(const int eRw[3],const int prefix[3],int perm[3],unsigned int &used_out) { perm[0]=0;perm[1]=1;perm[2]=2;'+matching+'used_out=used; }'
        shim='inline int __ffs(unsigned int x) { if(!x)return 0; int i=1; if(!(x&65535u)){x>>=16;i+=16;} if(!(x&255u)){x>>=8;i+=8;} if(!(x&15u)){x>>=4;i+=4;} if(!(x&3u)){x>>=2;i+=2;} return i+!(x&1u); }'
        src=d/'check.cpp'; src.write_text('#define nullptr 0\n'+shim+'\n'+h.read_text().replace('#pragma once','')+'\nint fused_verdict(unsigned long long np,unsigned int tz,unsigned int t2,unsigned long long LO,unsigned long long HI,unsigned int p34,unsigned int p56) { unsigned long long out; unsigned long long *np_out=&out;'+verdict+'}\n'+match+'\n'+(ROOT/'verification/gpu/join_type3.cpp').read_text())
        subprocess.run(['goto-cc','-DPOLY_Dmax=5','-D'+mode,'-I',str(ROOT/'src/gpu'),str(src),'-o',str(d/'check.gb')],check=True)
        r=subprocess.run(['cbmc',str(d/'check.gb'),'--bounds-check','--pointer-check','--signed-overflow-check','--unwinding-assertions','--unwind','12','--slice-formula'],text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=180)
        if r.returncode or 'VERIFICATION SUCCESSFUL' not in r.stdout:
            print('\n'.join(x for x in r.stdout.splitlines() if 'FAILURE' in x) or r.stdout[-3000:]); raise SystemExit('type3 '+mode+': NOT verified')
        print('Type-3 '+mode+': VERIFICATION SUCCESSFUL',flush=True)
if __name__=='__main__':
    for mode in (sys.argv[1:] or ('PACK','PERM','CERT')): run(mode)

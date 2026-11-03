#!/usr/bin/env python3
"""Pin device_ip_acceptance_verified to its Lean model (CudaIPAcceptanceSource.lean).

sourceAcceptance transcribes the function: its early returns become the
conjuncts, each checked helper becomes chkAdd/chkMul (exact by the generated
guard theorems), and the sticky failure flag becomes Option failure. Any change
to the source text fails this check until the model is reviewed and reproved.
"""
import hashlib
from pathlib import Path
import re
import sys
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / 'proofs/Refpoly/CudaIPAcceptanceSource.lean'
EXPECTED = (
    'if(cert.d==0||cert.k<=0)return0;longlongmu_total=0;for(intr=0;r<5;++r){'
    'if(cert.b[r]<0||cert.b[r]>=point_count||cert.idx[r]<0||cert.idx[r]>=point_count)return0;'
    'if(cert.mu[r]<0)return0;mu_total=device_ip_add(mu_total,cert.mu[r],arithmetic_failed);}'
    'if(mu_total<=0)return0;for(inti=0;i<5;++i)for(intj=0;j<5;++j){longlongvalue=0;'
    'for(intt=0;t<5;++t)value=device_ip_add(value,device_ip_mul(cert.adj[i][t],points[cert.b[t]*5+j],'
    'arithmetic_failed),arithmetic_failed);if(value!=(i==j?cert.d:0))return0;}'
    'for(intj=0;j<5;++j){longlongvalue=0;for(intr=0;r<5;++r){'
    'value=device_ip_add(value,device_ip_mul(cert.mu[r],points[cert.idx[r]*5+j],arithmetic_failed),arithmetic_failed);'
    'value=device_ip_add(value,device_ip_mul(cert.k,points[cert.b[r]*5+j],arithmetic_failed),arithmetic_failed);}'
    'if(value!=0)return0;}return!*arithmetic_failed;')


def main():
    source = (ROOT / 'src/gpu/ip_check.cuh').read_text()
    function = block(source, '__device__ int device_ip_acceptance_verified(')
    body = re.sub(r'//[^\n]*|\s+', '', function[function.index('{') + 1:function.rindex('}')])
    if body != EXPECTED:
        raise SystemExit('device_ip_acceptance_verified changed; review sourceAcceptance and reprove')
    digest = hashlib.sha256(function.encode()).hexdigest()
    header = f'-- Source-function SHA256: {digest}\n'
    model = MODEL.read_text()
    if '--write' in sys.argv:
        model = re.sub(r'^-- Source-function SHA256: [0-9a-f]+\n', '', model)
        MODEL.write_text(header + model)
    elif not model.startswith(header):
        raise SystemExit('CudaIPAcceptanceSource.lean is not pinned to the current checker source')
    else:
        print('Acceptance checker source matches its pinned Lean model')


if __name__ == '__main__':
    main()

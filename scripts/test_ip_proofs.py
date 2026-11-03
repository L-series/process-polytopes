#!/usr/bin/env python3
"""Build source-linked IP proofs and audit their actual Lean axiom closures."""
import importlib.util
from pathlib import Path
import re
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
PROOFS=ROOT/'proofs'
spec=importlib.util.spec_from_file_location('proof_audit',PROOFS/'ci/proof_audit.py')
audit=importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
errors=audit.check_pins(PROOFS)
files=sorted((PROOFS/'Refpoly').glob('CudaIP*.lean'))
for path in files:
    forbidden=set(audit.source_tokens(path.read_text())) & (audit.FORBIDDEN|{'sorry','admit'})
    if forbidden: errors.append(f'{path.name}: forbidden tokens {sorted(forbidden)}')
if errors: raise SystemExit('\n'.join(errors))
for generator in ('generate_ip_eval_model.py','generate_ip_arithmetic_model.py','generate_ip_certificate_model.py',
                  'check_ip_acceptance_model.py'):
    subprocess.run([sys.executable,str(ROOT/'scripts'/generator)],check=True)
subprocess.run(['lake','build','Refpoly.CudaIPScan','Refpoly.CudaIPArithmeticSource','Refpoly.CudaIPCertificate','Refpoly.CudaIPAcceptance','Refpoly.CudaIPAcceptanceSource'],cwd=PROOFS,check=True)
result=subprocess.run(['lake','env','lean','ci/IPAudit.lean'],cwd=PROOFS,text=True,capture_output=True,check=True)
closures=re.findall(r'depends on axioms:\s*\[([^\]]*)\]',result.stdout)
empty=result.stdout.count('does not depend on any axioms')
expected=(PROOFS/'ci/IPAudit.lean').read_text().count('#print axioms')
if len(closures)+empty!=expected: raise SystemExit('Unrecognized audit output:\n'+result.stdout)
for closure in closures:
    axioms={a.strip() for a in closure.split(',') if a.strip()}
    if axioms-audit.FOUNDATIONS: raise SystemExit(f'Unexpected axioms: {axioms-audit.FOUNDATIONS}')
print(f'Checked {len(files)} IP modules and {expected} theorem axiom closures; foundations only.')

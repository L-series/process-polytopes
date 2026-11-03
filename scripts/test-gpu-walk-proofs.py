#!/usr/bin/env python3
"""Build walk certificate proofs and audit their theorem axiom closures."""
import importlib.util
from pathlib import Path
import re
import subprocess
ROOT = Path(__file__).resolve().parents[1]
PROOFS = ROOT / 'proofs'


def main():
    spec = importlib.util.spec_from_file_location('proof_audit', PROOFS / 'ci/proof_audit.py')
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    errors = audit.check_pins(PROOFS)
    source = PROOFS / 'Refpoly/GpuWalk.lean'
    forbidden = set(audit.source_tokens(source.read_text())) & (audit.FORBIDDEN | {'sorry', 'admit'})
    if forbidden:
        errors.append(f'GpuWalk.lean: forbidden tokens {sorted(forbidden)}')
    if errors:
        raise SystemExit('\n'.join(errors))
    subprocess.run(['lake','build','Refpoly.GpuWalk'], cwd=PROOFS, check=True)
    result = subprocess.run(['lake','env','lean','ci/GpuWalkAudit.lean'], cwd=PROOFS,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
    closures = re.findall(r'depends on axioms:\s*\[([^\]]*)\]', result.stdout)
    empty = result.stdout.count('does not depend on any axioms')
    expected = (PROOFS / 'ci/GpuWalkAudit.lean').read_text().count('#print axioms')
    if len(closures) + empty != expected:
        raise SystemExit('Unrecognized walk axiom audit output')
    for closure in closures:
        used = {a.strip() for a in closure.split(',') if a.strip()}
        if used - audit.FOUNDATIONS:
            raise SystemExit(f'Unexpected walk axioms: {used - audit.FOUNDATIONS}')
    print(f'Checked {expected} walk certificate theorem closures; foundations only.')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Build the GPU proofs (Join*, Gpu* modules) and reject nonfoundational axioms in their closures."""
import importlib.util
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PROOFS = ROOT / "proofs"
spec = importlib.util.spec_from_file_location("proof_audit", PROOFS / "ci/proof_audit.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
errors = audit.check_pins(PROOFS)
files = sorted((PROOFS / "Refpoly").glob("Join*.lean")) + sorted((PROOFS / "Refpoly").glob("Gpu*.lean"))
for path in files:
    forbidden = (set(audit.source_tokens(path.read_text())) & (audit.FORBIDDEN | {"sorry", "admit"}))
    if forbidden:
        errors.append(f"{path.name}: forbidden tokens {sorted(forbidden)}")
if errors:
    raise SystemExit("\n".join(errors))
subprocess.run(["lake", "build", "Refpoly.JoinStack", "Refpoly.GpuDecode", "Refpoly.GpuPipeline"], cwd=PROOFS, check=True)
expected = 0
closures = []
empty = 0
for audit_file in ("ci/JoinAudit.lean", "ci/GpuAudit.lean"):
    result = subprocess.run(["lake", "env", "lean", audit_file], cwd=PROOFS, text=True, capture_output=True, check=True)
    closures += re.findall(r"depends on axioms:\s*\[([^\]]*)\]", result.stdout)
    # Closed computational proofs can also be reported with an empty axiom set.
    empty += result.stdout.count("does not depend on any axioms")
    expected += (PROOFS / audit_file).read_text().count("#print axioms")
if len(closures) + empty != expected:
    raise SystemExit("Unrecognized axiom-audit output")
for closure in closures:
    axioms = {a.strip() for a in closure.split(",") if a.strip()}
    if axioms - audit.FOUNDATIONS:
        raise SystemExit(f"Unexpected axioms: {axioms - audit.FOUNDATIONS}")
print(f"Checked {len(files)} GPU proof modules and {expected} theorem axiom closures; foundations only.")

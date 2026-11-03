#!/usr/bin/env python3
"""Pin row_solution_count (src/gpu/join.cuh) to dpCount (JoinRowCount.lean).

dpCount processes the positions from last to first, each pass being dpStep:
`for r = w..degree: dp[r] += dp[r - w]` in place, from dp = [1, 0, ...].
The additions are checked (an overflow throws, never wraps). Any change to the
source text fails this check until the model is reviewed and reproved.
"""
from pathlib import Path
import re
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = ('inlinestd::uint64_trow_solution_count(constSelectedEntry&entry,intsize){'
            'if(entry.degree<0)throwstd::runtime_error("gjoin:negativerowdegree");'
            'std::vector<std::uint64_t>dp(static_cast<std::size_t>(entry.degree)+1,0);dp[0]=1;'
            'for(intp=size-1;p>=0;--p){if(entry.w[p]<=0)throwstd::runtime_error("gjoin:nonpositiverowweight");'
            'for(intr=entry.w[p];r<=entry.degree;++r)dp[r]=checked_add(dp[r],dp[r-entry.w[p]]);}'
            'returndp[static_cast<std::size_t>(entry.degree)];}')

function = block((ROOT / 'src/gpu/join.cuh').read_text(), 'inline std::uint64_t row_solution_count(')
if re.sub(r'//[^\n]*|\s+', '', function) != EXPECTED:
    raise SystemExit('row_solution_count changed; review dpCount and reprove')
print('Row-count DP source matches its pinned Lean model')

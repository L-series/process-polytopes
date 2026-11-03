#!/usr/bin/env python3
"""Pin the fused join to JoinType3.lean and its extracted certificate refinement.

sourceScan transcribes each increasing outer index: presence, Bloom and exact
lookup, uint32-count product/addition, cap-before-certificate branch, then
certificate OR. matchedPairs factors the same scan into its successful pairs;
sourceScan_eq proves that factoring. find is the source binary lookup with the
safe midpoint (midpoint_exact, midpoint_u32); the all-length exactness theorem
is find_iff. FiltersComplete is checked for EVERY canonical stored key by the
builder's two final loops, which are pinned with the entire builder. Hashing
uses word64 multiplication modulo 2^64, then the high nine bits; the same
expression is checked against every key, so no injectivity claim is needed.

remap_matching proves both shorter-side iterations enumerate the same key
relation, with presence_permutation for reordered mask checks. The extracted
PERM query proves the weight matching is bijective, including repeated weights.
The extracted CERT query proves spread7 multiplication, masks and final verdict
equal the general leaf. Its replaced ffs shim is separately checked by the
join-leaf queries. count_u64 proves no count wrap for uint32 factors while the
prior count is at most a nonnegative signed-int cap. The incomplete cap branch
always keeps. Builder normal return, supported type-3 layout and source word
widths are the interpretation preconditions, not exact-count/mask assumptions.
OPEN composition boundary: the checked source uses stored JoinKey.count,
whereas the public Lean endpoint currently scans eventSummary's fibre-card
counts. The validator-to-Layout equality and raw row-mask adapters must be
composed before this pin is an end-to-end source refinement. A passing hash
check freezes the reviewed source; it does not discharge that obligation.
This is a reviewed transcription/CBMC split, not a verified C++ frontend.
"""
import hashlib
from pathlib import Path
import re
import sys
from test_gpu_ip_source import block
ROOT=Path(__file__).resolve().parents[1]
PINS={
 'device_join_find':'b53c167c2010129a31a37c5b8780e686fab121008a6fba0890b8e1f0d7d86220',
 'device_join_pack':'e29cc1452efb63b99024beed82fb749dd52e90bd270eb5a6805da4147a77f6ba',
 'device_spread7':'505c6382ba8a30d476a253c2b9302ef534d052ea8e30cc2468282114512a093e',
 'device_join_cert_bits':'604e65e8589f0fc8226012841a5e7cd80b2577d944166b54af64d633d43277e5',
 'device_join_decide':'6487e442dd8f83b168c7fcf520d43522d578a1f596ec12f9e31892c3d07d61a1',
 'build_join_table':'7e4e3e28c7d5bacddc1de27c973826cca17ac4214c53c29bd5dd178789e969af',
 'type3_layout_supported':'8c4d1b01ad201181418ab145803cb12733099b2b84cf6a9e41be05aba2532952',
}
MARKERS=(
 '__device__ __forceinline__ unsigned int device_join_find(',
 '__device__ __forceinline__ unsigned long long device_join_pack(',
 '__device__ __forceinline__ unsigned long long device_spread7(',
 '__device__ __forceinline__ void device_join_cert_bits(',
 '__device__ int device_join_decide(',
 'inline void build_join_table(',
 'inline bool type3_layout_supported(',
)
def main():
    source=(ROOT/'src/gpu/join_type3.cuh').read_text()
    for marker in MARKERS:
        name=marker.split('(')[0].split()[-1]
        normalized=re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|\s+','',block(source,marker))
        digest=hashlib.sha256(normalized.encode()).hexdigest()
        if '--print' in sys.argv: print(name+': '+digest)
        elif PINS.get(name)!=digest: raise SystemExit(name+' changed; review JoinType3.lean and type-3 CBMC refinement, then repin')
    if '--print' not in sys.argv: print('Fused type-3 join and builder match the reviewed Lean/CBMC split')
if __name__=='__main__': main()

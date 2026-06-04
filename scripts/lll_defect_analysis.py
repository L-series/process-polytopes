#!/usr/bin/env python3
"""Offline LLL analysis of dumped triangular enumeration bases.

Reads `BASIS n=.. N=.. np=.. | row0 | row1 ... |X xmax..` lines and, per basis,
compares the orthogonality defect of the PALP triangular (HNF) basis vs an
LLL-reduced basis.  The orthogonality defect d = prod||b_i|| / covol(L) is the
standard measure of how much a basis inflates lattice-enumeration cost; LLL
minimizes it.  d_tri / d_lll estimates the over-search factor LLL could remove.
Exact integer/Fraction arithmetic (dims are tiny: n<=5)."""
import sys, math
from fractions import Fraction as F

def dot(a, b): return sum(x*y for x, y in zip(a, b))

def lll(basis, delta=F(3,4)):
    B = [list(map(F, row)) for row in basis]
    n = len(B)
    def gram_schmidt():
        Bs, mu = [], [[F(0)]*n for _ in range(n)]
        for i in range(n):
            bi = list(B[i])
            for j in range(i):
                mu[i][j] = dot(B[i], Bs[j]) / dot(Bs[j], Bs[j]) if dot(Bs[j],Bs[j]) else F(0)
                bi = [x - mu[i][j]*y for x, y in zip(bi, Bs[j])]
            Bs.append(bi)
        return Bs, mu
    Bs, mu = gram_schmidt()
    k = 1
    while k < n:
        for j in range(k-1, -1, -1):
            if abs(mu[k][j]) > F(1, 2):
                q = round(mu[k][j])
                B[k] = [x - q*y for x, y in zip(B[k], B[j])]
                Bs, mu = gram_schmidt()
        if dot(Bs[k], Bs[k]) >= (delta - mu[k][k-1]**2) * dot(Bs[k-1], Bs[k-1]):
            k += 1
        else:
            B[k], B[k-1] = B[k-1], B[k]
            Bs, mu = gram_schmidt()
            k = max(k-1, 1)
    return B

def covol(B):
    # sqrt(det(B B^T)) via Gram matrix determinant (exact, then sqrt as float)
    n = len(B)
    G = [[F(dot(B[i], B[j])) for j in range(n)] for i in range(n)]
    # fraction-free determinant
    import copy
    M = copy.deepcopy(G); det = F(1)
    for i in range(n):
        p = next((r for r in range(i, n) if M[r][i] != 0), None)
        if p is None: return 0.0
        if p != i: M[i], M[p] = M[p], M[i]; det = -det
        det *= M[i][i]
        for r in range(i+1, n):
            f = M[r][i]/M[i][i]
            M[r] = [M[r][c]-f*M[i][c] for c in range(n)]
    return math.sqrt(float(det))

def norm(v): return math.sqrt(float(dot(v, v)))

def defect(B):
    cv = covol(B)
    if cv == 0: return None
    p = 1.0
    for r in B: p *= norm(r)
    return p / cv

def parse(line):
    head, *rest = line.strip().split('|')
    d = dict(tok.split('=') for tok in head.split()[1:])
    n, N, np_ = int(d['n']), int(d['N']), int(d['np'])
    rows = []
    for seg in rest:
        seg = seg.strip()
        if seg.startswith('X'): break
        rows.append([int(x) for x in seg.split()])
    rows = rows[:n]
    return n, N, np_, rows

def main():
    recs = []
    for line in sys.stdin:
        if not line.startswith('BASIS'): continue
        try:
            n, N, np_, rows = parse(line)
            if n < 2 or any(len(r) != N for r in rows): continue
            dt = defect(rows)
            B2 = lll(rows)
            dl = defect(B2)
            if dt and dl: recs.append((np_, dt, dl, dt/dl))
        except Exception:
            continue
    if not recs:
        print("no bases parsed"); return
    recs.sort()
    import statistics as st
    def q(xs, p): xs = sorted(xs); return xs[min(len(xs)-1, int(len(xs)*p))]
    dts = [r[1] for r in recs]; dls = [r[2] for r in recs]; rr = [r[3] for r in recs]
    nps = [r[0] for r in recs]
    print(f"bases analyzed: {len(recs)}   np: min={min(nps)} p50={q(nps,.5)} p90={q(nps,.9)} max={max(nps)}")
    print(f"orthogonality defect (triangular/HNF):  median={st.median(dts):.3f}  p90={q(dts,.9):.3f}  p99={q(dts,.99):.3f}  max={max(dts):.3f}")
    print(f"orthogonality defect (after LLL)      :  median={st.median(dls):.3f}  p90={q(dls,.9):.3f}  p99={q(dls,.99):.3f}  max={max(dls):.3f}")
    print(f"defect reduction  d_tri/d_lll         :  median={st.median(rr):.3f}  p90={q(rr,.9):.3f}  p99={q(rr,.99):.3f}  max={max(rr):.3f}")
    # heavy-tail focus: top decile by np (the candidates that dominate walk cost)
    heavy = [r for r in recs if r[0] >= q(nps, .9)]
    if heavy:
        hdt=[r[1] for r in heavy]; hrr=[r[3] for r in heavy]
        print(f"\nheavy tail (np >= p90, n={len(heavy)}):")
        print(f"  defect tri median={st.median(hdt):.3f}  reduction median={st.median(hrr):.3f}  reduction max={max(hrr):.3f}")
    # fraction where LLL changes the basis materially
    mat = sum(1 for r in recs if r[3] > 1.10)
    print(f"\nbases where LLL cuts defect by >10%: {mat}/{len(recs)} ({100*mat/len(recs):.1f}%)")

if __name__ == '__main__':
    main()

// Full finite-domain refinement of fused certificate accumulation to the
// already checked general join leaf. Count arithmetic is proved in Lean.
unsigned int nondet_uint();
unsigned long long nondet_ull();
int nondet_int();
static bool bit64(unsigned long long v, int n) { return (v >> n) & 1ULL; }
#ifdef CERT
int main() {
    JoinKey l, r;
    l.key = nondet_ull() & 0xFFFFFFFFFFFFULL;
    r.key = nondet_ull() & 0xFFFFFFFFFFFFULL;
    l.count = r.count = 1;
    l.cat_a = nondet_uint() & 15u; l.cat_b = nondet_uint() & 15u;
    r.cat_a = nondet_uint() & 15u; r.cat_b = nondet_uint() & 15u;
    l.pair = nondet_uint() & 3u; r.pair = nondet_uint() & 3u;
    GJoinKey records[2];
    records[0].count = records[1].count = 1;
    records[0].cats = l.cat_a | (static_cast<unsigned>(l.cat_b) << 4);
    records[1].cats = r.cat_a | (static_cast<unsigned>(r.cat_b) << 4);
    records[0].pairs = l.pair; records[1].pairs = r.pair;
    GJoinPlan plan;
    plan.nrows = 2; plan.ambient = 7; plan.shared_mask = 7;
    for (int s=0; s<2; ++s) {
        plan.size[s]=5; plan.shared[s]=3; plan.order[s]=s;
        plan.table[s].keys=records;
        for (int q=0; q<3; ++q) plan.coord[s][q]=q;
        plan.coord[s][3]=3+2*s; plan.coord[s][4]=4+2*s;
    }
    for (int a=0; a<10; ++a) plan.same_row[a]=0;
    plan.same_row[3]=1u<<4; plan.same_row[4]=1u<<3;
    plan.same_row[5]=1u<<6; plan.same_row[6]=1u<<5;
    int val[10]={};
    for (int q=0; q<3; ++q) val[q]=(l.key >> (32-16*q)) & 65535;
    const unsigned long long chosen[kMaxSlots]={0,1};
    GJoinAcc acc={};
    __CPROVER_assert(device_gjoin_leaf(plan, chosen, val, acc), "general leaf succeeds for unit counts");
    unsigned int tz=0,t2=0,p34=0,p56=0;
    unsigned long long LO=0,HI=0;
    device_join_cert_bits(l,r,tz,t2,LO,HI,p34,p56);
    __CPROVER_assert(tz==acc.tz && t2==acc.t2, "fused coordinate masks equal general leaf");
    const int a=nondet_int(),b=nondet_int();
    __CPROVER_assume(a>=0 && a<7 && b>=0 && b<7);
    __CPROVER_assert(bit64(LO,7*a+b)==bit64(acc.LO[a],b), "fused low pair matrix equals general leaf");
    __CPROVER_assert(bit64(HI,7*a+b)==bit64(acc.HI[a]|acc.m3_all,b), "fused high pair matrix equals general leaf");
    __CPROVER_assert((p34&1u)==bit64(acc.LS[3],4) && ((p34>>1)&1u)==bit64(acc.HS[3],4), "left private joint pair");
    __CPROVER_assert((p56&1u)==bit64(acc.LS[5],6) && ((p56>>1)&1u)==bit64(acc.HS[5],6), "right private joint pair");
    unsigned int z0=nondet_uint(),z2=nondet_uint(),j0=nondet_uint(),j1=nondet_uint();
    unsigned long long lo0=nondet_ull(),hi0=nondet_ull();
    unsigned int z=z0,zp=z2,jl=j0,jr=j1;
    unsigned long long low=lo0,high=hi0;
    device_join_cert_bits(l,r,z,zp,low,high,jl,jr);
    __CPROVER_assert(z==(z0|tz) && zp==(z2|t2), "fused coordinate updates preserve arbitrary prior bits");
    __CPROVER_assert(low==(lo0|LO) && high==(hi0|HI), "fused pair updates preserve arbitrary prior bits");
    __CPROVER_assert(jl==(j0|p34) && jr==(j1|p56), "fused joint updates preserve arbitrary prior bits");
    acc.np=nondet_ull();
    __CPROVER_assert(fused_verdict(acc.np,tz,t2,LO,HI,p34,p56)==device_gjoin_verdict(acc,plan), "fused verdict equals general verdict");
}
#endif
#ifdef PERM
int main() {
    int eRw[3],prefix[3],wanted[3];
    for(int i=0;i<3;++i) {
        eRw[i]=nondet_int(); wanted[i]=nondet_int();
        __CPROVER_assume(wanted[i]>=0 && wanted[i]<3);
        for(int j=0;j<i;++j) __CPROVER_assume(wanted[i]!=wanted[j]);
    }
    for(int i=0;i<3;++i) prefix[i]=eRw[wanted[i]];
    int perm[3];
    unsigned int used_out=0;
    fused_match(eRw,prefix,perm,used_out);
    __CPROVER_assert(used_out==7, "fused prefix uses every entry position");
    for(int i=0;i<3;++i) {
        __CPROVER_assert(perm[i]>=0 && perm[i]<3 && prefix[i]==eRw[perm[i]], "fused prefix preserves weights");
        for(int j=0;j<i;++j) __CPROVER_assert(perm[i]!=perm[j], "fused prefix is a bijection with repeated weights");
    }
}
#endif

#ifdef PACK
int main() {
    int v[3];
    for(int i=0;i<3;++i) { v[i]=nondet_int(); __CPROVER_assume(v[i]>=0 && v[i]<=65535); }
    const unsigned long long key=device_join_pack(v);
    __CPROVER_assert(key<0x1000000000000ULL, "packed shared key fits 48 bits");
    for(int i=0;i<3;++i) __CPROVER_assert(((key>>(32-16*i))&65535ULL)==static_cast<unsigned>(v[i]), "packed field equals its input");
}
#endif

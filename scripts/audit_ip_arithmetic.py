#!/usr/bin/env python3
"""Clang-typed audit of IP arithmetic and failure-flag propagation.

This checks a source property, not geometric correctness or CUDA memory safety.
It excludes bounded array-index arithmetic and unsigned incidence arithmetic.
The checked helper bodies and the one unreachable-on-checked-path raw evaluator
are source-linked to Lean by the two generation scripts.
"""
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from test_gpu_ip_source import translation_unit
from test_gpu_ip_source import block

ROOT = Path(__file__).resolve().parents[1]


def walk(node, ancestors=()):
    yield node,ancestors
    for child in node.get('inner',[]):
        yield from walk(child,ancestors+(node,))


def referenced(node):
    return [n.get('referencedDecl',{}).get('name') for n,_ in walk(node)
            if n.get('kind') == 'DeclRefExpr']


def main():
    for generator in ('generate_ip_eval_model.py','generate_ip_arithmetic_model.py'):
        subprocess.run([sys.executable,str(ROOT/'scripts'/generator)],check=True)
    with tempfile.TemporaryDirectory(prefix='ip-arithmetic-audit-') as directory:
        cpp=Path(directory)/'ip.cpp'
        cpp.write_text(translation_unit())
        result=subprocess.run(['clang++','-std=c++20','-pthread','-Wno-parentheses','-fsyntax-only',
            '-Xclang','-ast-dump=json','-Xclang','-ast-dump-filter=device_',str(cpp)],
            check=True,capture_output=True,text=True)
    decoder=json.JSONDecoder()
    offset=0
    functions={}
    while offset < len(result.stdout):
        while offset<len(result.stdout) and result.stdout[offset].isspace(): offset+=1
        if offset==len(result.stdout): break
        node,offset=decoder.raw_decode(result.stdout,offset)
        if node.get('kind') != 'FunctionDecl':
            continue  # Standard-library enum constants also contain the filter text.
        functions[node['name']]=node
    primitives={'device_ip_'+op for op in ('add','sub','mul','div','rem')}
    allowed=primitives|{'device_eval_eq'}
    flag_functions={name for name,f in functions.items() if any(
        p.get('kind')=='ParmVarDecl' and p.get('name')=='arithmetic_failed' for p in f.get('inner',[]))}
    flag_callers=flag_functions|{'device_ip_check'}
    errors=[]
    for name,f in functions.items():
        for node,ancestors in walk(f):
            kind=node.get('kind')
            if node.get('type',{}).get('qualType')=='long long' and name not in allowed:
                if (kind in ('BinaryOperator','CompoundAssignOperator') and node.get('opcode') in
                    ('+','-','*','/','%','+=','-=','*=','/=','%=') or
                    kind=='UnaryOperator' and node.get('opcode') in ('-','++','--') and not (
                        node.get('opcode')=='-' and [c.get('kind') for c in node.get('inner',[])]==['IntegerLiteral'])):
                    errors.append(f'{name}: unchecked signed payload arithmetic {node.get("opcode")}')
            if kind!='CallExpr' or not node.get('inner'): continue
            callee=referenced(node['inner'][0])
            if not callee: continue
            callee=callee[0]
            if callee not in functions and callee not in {'__popcll','__syncthreads','atomicAdd','atomicExch','clock64'}:
                errors.append(f'{name}: unaudited callee {callee}')
            if callee in flag_functions or callee in primitives:
                if name not in flag_callers:
                    errors.append(f'{name}: unchecked entry into {callee}')
                if referenced(node['inner'][-1]) != ['arithmetic_failed']:
                    errors.append(f'{name}: failure flag not propagated into {callee}')
            if callee=='device_eval_eq' and name!='device_ip_eval_eq':
                errors.append(f'{name}: raw equation evaluation on IP path')
    source=(ROOT/'src/gpu/ip_check.cuh').read_text()
    wrapper=block(source,'__device__ long long device_ip_eval_eq(')
    body=wrapper[wrapper.index('{')+1:wrapper.rindex('}')]
    normalized=re.sub(r'\s+','',body)
    expected=('if(!arithmetic_failed)returndevice_eval_eq(equation,point);'
              'if(device_eval_bounded(equation,point))returndevice_eval_eq(equation,point);'
              'longlongvalue=equation.c;for(inti=0;i<5;++i)'
              'value=device_ip_add(value,device_ip_mul(equation.a[i],point[i],arithmetic_failed),arithmetic_failed);returnvalue;')
    if normalized!=expected:
        errors.append('Checked evaluator loop/body changed outside the audited fragment')
    for root in ('device_ip_check',):
        bindings=[n for n,_ in walk(functions[root]) if n.get('kind')=='VarDecl' and n.get('name')=='arithmetic_failed']
        if len(bindings)!=1:
            errors.append(f'{root}: failure-flag binding missing')
        else:
            init=bindings[0].get('inner',[])
            if not init or init[0].get('kind')!='UnaryOperator' or init[0].get('opcode')!='&':
                errors.append(f'{root}: failure flag is not an address of scratch storage')
            elif not any(n.get('kind')=='MemberExpr' and n.get('name')=='arithmetic_failed' for n,_ in walk(init[0])):
                errors.append(f'{root}: failure flag does not reference the scratch member')
    if errors: raise SystemExit('\n'.join(errors))
    print(f'Clang audit passed: {len(functions)} IP functions; signed payload arithmetic is mediated by checked helpers and flags are forwarded.')


if __name__=='__main__':
    main()

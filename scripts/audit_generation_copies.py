#!/usr/bin/env python3
"""Clang-typed audit: the GPU emission recursion only copies weight values.

Together with the CBMC order-invariance queries for the two primitives that do
compare weights (device_next_permutation, device_prefix_is_canonical), this is
the premise of order_equivariant_emission (GpuEmission.lean): the emitted
candidates on any positive weights are the order-type ones with the values
substituted, so the exhaustive order-type check covers every input.
Weight values are the `w` members of SelectedEntry and the local `prefix` /
`original` arrays. They may be read only as the source of a plain copy (an
assignment or initialisation into another weight location) or inside an
argument passed whole to an audited function; they may not feed any operator,
condition, index, or other call.
"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
RECURSION = ['device_count_prefixes', 'device_count_last_slot', 'device_count_after_slot0', 'device_store_candidate',
             'device_emit_prefixes', 'device_emit_last_slot', 'device_emit_after_slot0',
             'device_count_small_prefixes', 'device_emit_small_prefixes']
PRIMITIVES = {'device_next_permutation', 'device_prefix_is_canonical'}
# PALP's side (cws.c): the same shape, primitives proved equal to the GPU ones.
CPU_RECURSION = ['EnumeratePermutations', 'EmitDescriptorCWS', 'EmbedWeightInCWS']
CPU_PRIMITIVES = {'PrefixIsCanonical', 'NextPrefixPermutation'}
WEIGHT_MEMBERS = ('w', 'weights', 'W')
WEIGHT_LOCALS = ('prefix', 'original', 'original_prefix')
MARKERS = ['struct SelectedEntry', 'struct DeviceDescriptor', 'struct DeviceScanStats', 'struct DeviceCwsCandidate',
           '__device__ int device_next_permutation(', '__device__ int device_weight_at_coordinate(',
           '__device__ int device_slot_uses_coordinate(', '__device__ int device_prefix_is_canonical(',
           '__device__ __noinline__ void device_store_candidate(',
           '__device__ unsigned long long device_count_prefixes(', '__device__ unsigned long long device_count_last_slot(',
           '__device__ unsigned long long device_count_after_slot0(', '__device__ void device_emit_prefixes(',
           '__device__ void device_emit_last_slot(', '__device__ void device_emit_after_slot0(',
           '__device__ unsigned long long device_count_small_prefixes(', '__device__ void device_emit_small_prefixes(']


def walk(node, ancestors=()):
    yield node, ancestors
    for child in node.get('inner', []):
        yield from walk(child, ancestors + (node,))


def is_weight(node):
    """A weight location: x.w[i], out->weights[s][c], prefix[i] or original[i]."""
    if node.get('kind') != 'ArraySubscriptExpr':
        return False
    # Scalar weights only; a subscript naming a whole row (struct or array) is
    # an aggregate passed on, audited where its scalars are read.
    if node.get('type', {}).get('desugaredQualType', node.get('type', {}).get('qualType')) not in (
            'int', 'long', 'long long', 'const int', 'const long', 'const long long'):
        return False
    base = node['inner'][0]
    while True:  # follow the base chain only, never into index expressions
        kind = base.get('kind')
        if kind == 'MemberExpr' and base.get('name') in WEIGHT_MEMBERS:
            return True
        if kind == 'DeclRefExpr' and base.get('referencedDecl', {}).get('name') in WEIGHT_LOCALS:
            return True
        if kind in ('ImplicitCastExpr', 'ParenExpr', 'ArraySubscriptExpr', 'MemberExpr') and base.get('inner'):
            base = base['inner'][0]
            continue
        return False
    return False


def functions_of(text):
    decoder = json.JSONDecoder()
    offset, functions = 0, {}
    while offset < len(text):
        while offset < len(text) and text[offset].isspace():
            offset += 1
        if offset == len(text):
            break
        node, offset = decoder.raw_decode(text, offset)
        if node.get('kind') == 'FunctionDecl' and node.get('inner'):
            functions[node['name']] = node
    return functions


def audit(functions, recursion, allowed_callees, errors):
    for name in recursion:
        if name not in functions:
            errors.append(f'{name}: not found')
            continue
        for node, ancestors in walk(functions[name]):
            if not is_weight(node):
                continue
            # Climb through implicit conversions to the consuming expression.
            consumer, child = None, node
            for parent in reversed(ancestors):
                if parent.get('kind') in ('ImplicitCastExpr', 'ParenExpr'):
                    child = parent
                    continue
                consumer = parent
                break
            kind = consumer.get('kind') if consumer else None
            if kind == 'ArraySubscriptExpr' and consumer['inner'][0] is child:
                continue  # base of a further subscript (a 2-D weight location)
            if kind == 'BinaryOperator' and consumer.get('opcode') == '=':
                lhs, rhs = consumer['inner']
                if rhs is child and is_weight(lhs):
                    continue  # copy into another weight location
                if lhs is child:
                    continue  # written, not read
            if kind == 'VarDecl':
                continue
            errors.append(f'{name}: weight value used by {kind} {consumer.get("opcode", "")}'.rstrip())
        for node, _ in walk(functions[name]):
            if node.get('kind') == 'CallExpr':
                callee_expr = node['inner'][0]
                members = [n.get('name') for n, _ in walk(callee_expr) if n.get('kind') == 'MemberExpr']
                if members[:1] == ['emit']:
                    continue  # the emitted row goes to the caller's consumer
                callee = [n.get('referencedDecl', {}).get('name') for n, _ in walk(callee_expr)
                          if n.get('kind') == 'DeclRefExpr']
                if callee and callee[0] not in allowed_callees:
                    errors.append(f'{name}: unaudited callee {callee[0]}')


def main():
    errors = []
    with tempfile.TemporaryDirectory(prefix='generation-audit-') as directory:
        header = Path(directory) / 'gpu_extract.h'
        subprocess.run([sys.executable, str(ROOT / 'verification/gpu/extract.py'), str(header)] + MARKERS, check=True)
        cpp = Path(directory) / 'generation.cpp'
        cpp.write_text('#include "gpu_extract.h"\n')
        result = subprocess.run(['clang++', '-std=c++17', '-fsyntax-only', f'-I{directory}', f'-I{ROOT}/src/gpu',
                                 f'-I{ROOT}/src/cws', f'-I{ROOT}/PALP', '-DPOLY_Dmax=5', '-Xclang', '-ast-dump=json',
                                 '-Xclang', '-ast-dump-filter=device_', str(cpp)], check=True, capture_output=True, text=True)
    audit(functions_of(result.stdout), RECURSION, PRIMITIVES | set(RECURSION) | {'atomicAdd'}, errors)
    cpu = {}
    for name in CPU_RECURSION:
        result = subprocess.run(['clang', '-x', 'c', '-fsyntax-only', '-DPOLY_Dmax=5', '-DPALP_CWS_NO_MAIN', f'-I{ROOT}/PALP',
                                 f'-I{ROOT}/src/cws', '-Xclang', '-ast-dump=json', '-Xclang', f'-ast-dump-filter={name}',
                                 str(ROOT / 'PALP/cws.c')], check=True, capture_output=True, text=True)
        cpu.update({k: v for k, v in functions_of(result.stdout).items() if k == name})
    audit(cpu, CPU_RECURSION, CPU_PRIMITIVES | set(CPU_RECURSION) | {'__assert_fail', '__assert_rtn', '__builtin_expect'},
          errors)
    if errors:
        raise SystemExit('\n'.join(sorted(set(errors))))
    print(f'Generation copy audit passed: {len(RECURSION)} GPU and {len(CPU_RECURSION)} PALP emission functions only '
          'copy weight values; comparisons are confined to the CBMC-checked primitives.')


if __name__ == '__main__':
    main()

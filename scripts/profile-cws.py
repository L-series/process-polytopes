#!/usr/bin/env python3
"""Measure native geometry throughput, full function timing and instruction maps."""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import platform
import resource
import statistics
import subprocess
import sys
import time

sys.dont_write_bytecode = True

from cws_profile_format import normalize, normalize_instructions, parse_callgrind

PACKAGE = pathlib.Path(__file__).resolve().parents[1]
FILES = ['Coord.c', 'Rat.c', 'Vertex.c', 'Polynf.c', 'LG.c']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')


def command(args, **kwargs):
    return subprocess.run(list(map(str, args)), check=True, **kwargs)


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


def build(repo, output, cc):
    build = output / 'build'
    build.mkdir(parents=True, exist_ok=True)
    flags = ['-O3', '-g', '-DPOLY_Dmax=5']
    includes = ['-I', str(repo / 'PALP'), '-I', str(repo / 'src/cws')]
    commands, binaries = [], {}
    with (output / 'build.log').open('w') as log:
        for mode in ['native', 'timed']:
            directory = build / mode
            directory.mkdir(exist_ok=True)
            extra = [] if mode == 'native' else ['-finstrument-functions', '-fno-optimize-sibling-calls', '-fno-pie']
            def compile_source(source, label, defines=()):
                obj = directory / f'{label}.o'
                args = [cc, *flags, *includes, *extra, *defines, '-c', source, '-o', obj]
                commands.append(list(map(str, args)))
                command(args, stdout=log, stderr=subprocess.STDOUT)
                return obj
            common = [compile_source(repo / 'PALP' / name, f'palp-{name[:-2]}') for name in FILES]
            runtime = []
            if mode == 'timed':
                runtime = [directory / 'function-timing.o']
                # The instrumentation runtime must never instrument itself.
                args = [cc, '-O3', '-g', '-fno-pie', '-c', repo / 'src/benchmarks/function-timing.c', '-o', runtime[0]]
                commands.append(list(map(str, args)))
                command(args, stdout=log, stderr=subprocess.STDOUT)
            geometry = [compile_source(repo / 'src/benchmarks/geometry.c', 'geometry')]
            pipeline = [compile_source(repo / 'PALP/cws.c', 'palp-cws', ['-DPALP_CWS_NO_MAIN'])]
            pipeline += [compile_source(repo / 'src/cws' / name, f'project-{name[:-2]}')
                         for name in ['main.c', 'catalogue.c', 'weight_pool.c']]
            for kind, objects in [('geometry', geometry), ('pipeline', pipeline)]:
                binary = directory / kind
                args = [cc, *objects, *common, *runtime, '-lm', '-o', binary]
                if mode == 'timed':
                    args.append('-no-pie')
                commands.append(list(map(str, args)))
                command(args, stdout=log, stderr=subprocess.STDOUT)
                binaries[mode, kind] = binary
    save(output / 'build-commands.json', commands)
    return binaries, flags


def timing_records(path, binary):
    addresses, records = [], []
    for line in path.read_text().splitlines():
        address, calls, self_ns, inclusive_ns = line.split()
        addresses.append(address)
        records.append({'calls': int(calls), 'self_ns': int(self_ns), 'inclusive_ns': int(inclusive_ns)})
    resolved = subprocess.check_output(['addr2line', '-f', '-e', str(binary), *addresses], text=True).splitlines()
    if len(resolved) != len(addresses) * 2:
        raise ValueError('Unresolved function timing addresses')
    merged = {}
    for i, record in enumerate(records):
        name, location = resolved[2*i:2*i+2]
        if name == '??':
            raise ValueError(f'Unknown instrumented function {addresses[i]}')
        source, _, line = location.rpartition(':')
        key = source, normalize(name)
        target = merged.setdefault(key, {'file': source, 'name': key[1], 'line': line,
                                         'calls': 0, 'self_ns': 0, 'inclusive_ns': 0})
        for field in record:
            target[field] += record[field]
    return list(merged.values())


def run_native(binary, args, directory, name, environment=None):
    stdout, stderr = directory / f'{name}.stdout', directory / f'{name}.stderr'
    with stdout.open('w') as out, stderr.open('w') as err:
        start = time.perf_counter_ns()
        result = subprocess.run([str(binary), *map(str, args)], stdout=out, stderr=err, env=environment)
        elapsed = time.perf_counter_ns() - start
    return result.returncode, elapsed, stdout, stderr


def identity(path):
    data = json.loads(path.read_text())
    return {k: data[k] for k in ['rows', 'points', 'reflexive']}


def one_case(case, binaries, output, repetitions, valgrind):
    directory = output / case['name'].replace('/', '-')
    directory.mkdir(exist_ok=True)
    kind, args = case['kind'], case['args']
    native = binaries['native', kind]
    runs, expected = [], None
    for i in range(repetitions + 1):
        status, wall, stdout, stderr = run_native(native, args, directory, f'native-{i}')
        if status:
            raise RuntimeError(stderr.read_text()[-2000:] + stdout.read_text()[-1000:])
        same = identity(stdout) if kind == 'geometry' else digest(stdout)
        if expected is not None and same != expected:
            raise ValueError('Native repetitions disagree')
        expected = same
        if i:
            runs.append({'process_ns': wall, **(json.loads(stdout.read_text()) if kind == 'geometry' else {})})
    timed_path = directory / 'function-timing.txt'
    status, timed_wall, timed_out, timed_err = run_native(binaries['timed', kind], args, directory, 'timed',
        {**os.environ, 'CWS_FUNCTION_TIMING': str(timed_path)})
    if status:
        raise RuntimeError(timed_err.read_text()[-2000:])
    if (identity(timed_out) if kind == 'geometry' else digest(timed_out)) != expected:
        raise ValueError('Timing instrumentation changed scientific results')
    timed_functions = timing_records(timed_path, binaries['timed', kind])
    rows = expected['rows'] if kind == 'geometry' else sum(
        f['calls'] for f in timed_functions if f['name'] == 'PRINT_CWS')
    if not rows:
        raise ValueError('No CWS counted in the timed pipeline')
    callgrind = directory / 'callgrind.out'
    cg_stdout, cg_stderr = directory / 'callgrind.stdout', directory / 'callgrind.stderr'
    vg_args = [valgrind, '--tool=callgrind', '--read-inline-info=yes', '--separate-recs=64',
               '--error-exitcode=99', f'--callgrind-out-file={callgrind}']
    if kind == 'geometry':
        vg_args += ['--toggle-collect=run_geometry*']
    with cg_stdout.open('w') as out, cg_stderr.open('w') as err:
        command([*vg_args, native, *args], stdout=out, stderr=err)
    if (identity(cg_stdout) if kind == 'geometry' else digest(cg_stdout)) != expected:
        raise ValueError('Callgrind changed scientific results')
    profile = parse_callgrind(callgrind)
    if profile['total_instructions'] <= 0:
        raise ValueError('Callgrind collected no instructions')
    normalize_instructions(profile, rows)
    if kind == 'pipeline':
        observed = sum(f['calls'] for f in profile['functions'] if f['name'] == 'PRINT_CWS')
        if observed != rows:
            raise ValueError('Instruction and timed profiles disagree on candidate count')
    timing_total = sum(f['self_ns'] for f in timed_functions)
    root_time = next(f['inclusive_ns'] for f in timed_functions if f['name'] == 'main')
    if timing_total != root_time:
        raise ValueError('Instrumented self times do not reconcile with main')
    for function in timed_functions:
        function['self_time_percent'] = 100 * function['self_ns'] / timing_total
        function['inclusive_time_percent'] = 100 * function['inclusive_ns'] / timing_total
        function['self_ns_per_cws'] = function['self_ns'] / rows
        function['inclusive_ns_per_cws'] = function['inclusive_ns'] / rows
        function['self_ns_per_call'] = function['self_ns'] / function['calls']
        function['inclusive_ns_per_call'] = function['inclusive_ns'] / function['calls']
    # Keep hardware measurements distinct: no PMU permission changes or guessed counts.
    pmu = {'status': 'not-applicable'}
    if kind == 'geometry':
        status, _, pmu_stdout, pmu_stderr = run_native(native, [*args, '--pmu'], directory, 'hardware')
        if status == 77:
            pmu = {'status': 'unavailable', 'reason': pmu_stderr.read_text().strip()}
        elif status:
            raise RuntimeError(pmu_stderr.read_text())
        else:
            if identity(pmu_stdout) != expected:
                raise ValueError('Hardware-count run changed results')
            pmu = {'status': 'measured', **json.loads(pmu_stdout.read_text())}
    key = 'wall_ns' if kind == 'geometry' else 'process_ns'
    ns = [r[key] for r in runs]
    result = {'name': case['name'], 'kind': kind, 'rows': rows, 'workload': case['workload'],
              'boundary': ('preloaded matrix construction, point enumeration, primal IP/hull, validation; no dual or NF'
                           if kind == 'geometry' else 'pool loading, selection, CWS generation, IP, dual construction and output'),
              'scientific_result': (expected if kind == 'geometry' else {'accepted_rows': len(timed_out.read_text().splitlines()), 'output_sha256': expected}),
              'native': {'runs': runs, 'mean_ns': statistics.mean(ns), 'median_ns': statistics.median(ns),
                         'mean_ns_per_cws': statistics.mean(ns) / rows,
                         'cws_per_second': rows * 1e9 / statistics.mean(ns)},
              'hardware': pmu, 'instructions': profile,
              'instrumented_timing': {'process_ns': timed_wall, 'native_process_ratio': timed_wall / statistics.mean(
                  [r['process_ns'] for r in runs]), 'functions': timed_functions}}
    if kind == 'geometry':
        result['native'].update({f'{stage}_mean_ns_per_cws': statistics.mean(r[f'{stage}_ns'] for r in runs) / rows
                                for stage in ['point', 'hull']})
    save(directory / 'result.json', result)
    print(case['name'], rows, 'CWS', round(result['native']['cws_per_second']), 'CWS/s', flush=True)
    return result


def prepare_cases(repo, output, limit):
    corpus = repo / 'docs/data/cws-benchmarks'
    manifest = json.loads((corpus / 'manifest.json').read_text())
    if manifest['verification']['status'] != 'passed':
        raise ValueError('Corpus is not verified')
    data = output / 'inputs'
    data.mkdir()
    cases = []
    for group in ['small', 'medium', 'large']:
        source, meta = corpus / f'{group}.cws', corpus / f'{group}.jsonl'
        if digest(source) != manifest['groups'][group]['cws_sha256'] or digest(meta) != manifest['groups'][group]['metadata_sha256']:
            raise ValueError('Corpus SHA-256 mismatch')
        lines = source.read_text().splitlines()
        metadata = [json.loads(line) for line in meta.read_text().splitlines()]
        if len(lines) != len(metadata) or len(lines) != 10000:
            raise ValueError('Corpus size/alignment mismatch')
        if any(hashlib.sha256(line.encode()).hexdigest() != m['id'] for line, m in zip(lines, metadata)):
            raise ValueError('Corpus ID mismatch')
        selected = lines[:limit] if limit else lines
        labels = metadata[:limit] if limit else metadata
        path, expectations = data / f'{group}.cws', data / f'{group}.expected'
        path.write_text(''.join(line + '\n' for line in selected))
        expectations.write_text(''.join(f"{m['points']} {m['vertices']} {int(m['reflexive'])}\n" for m in labels))
        cases.append({'name': f'geometry/{group}', 'kind': 'geometry', 'args': [str(path), str(expectations)],
                      'workload': {'sha256': digest(path), 'expectations_sha256': digest(expectations),
                                   'rows': len(selected), 'group': group}})
    # Fixed bounded generator ranges exercise slot ordering and shared-coordinate
    # permutations; full 5D generation would contain trillions of candidates.
    pools = data / 'weight-pools'
    pools.mkdir()
    for d in [2, 3, 4]:
        lines = (repo / f'docs/data/single-weight-systems/{d}d.txt').read_text().splitlines()
        n = 3 if limit else 12
        (pools / f'{d}d.txt').write_text(''.join(l + '\n' for l in lines[:n]))
    for descriptor in [3, 11]:
        cases.append({'name': f'generation/type{descriptor}', 'kind': 'pipeline',
                      'args': ['-n5', f'-s{descriptor}', '--weights-dir', str(pools)],
                      'workload': {'descriptor': descriptor, 'pools': {p.name: digest(p) for p in pools.glob('*.txt')}}})
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=pathlib.Path, default=PACKAGE)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--machine', required=True, help='Stable label for the local measurement machine')
    parser.add_argument('--methodology', default='serial-v1', help='Versioned measurement protocol label')
    parser.add_argument('--measurement-note', default='', help='Explain protocol or environment changes')
    parser.add_argument('--cc', default=os.environ.get('CC', 'cc'))
    parser.add_argument('--valgrind', default='valgrind')
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--limit', type=int, help='Smoke test only: first N rows per bucket and tiny generator pools')
    args = parser.parse_args()
    if args.repetitions < 1 or (args.limit is not None and not 1 <= args.limit <= 10000):
        parser.error('positive repetitions and limit in 1..10000 required')
    repo, output = args.repo.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / 'inputs').exists():
        parser.error('choose a fresh output directory')
    _, hard = resource.getrlimit(resource.RLIMIT_STACK)
    resource.setrlimit(resource.RLIMIT_STACK, (hard, hard))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    binaries, flags = build(repo, output, args.cc)
    cases = prepare_cases(repo, output, args.limit)
    cpu = next((l.split(':', 1)[1].strip() for l in pathlib.Path('/proc/cpuinfo').read_text().splitlines()
                if l.startswith('model name')), platform.machine())
    used = [repo / 'PALP' / f for f in [*FILES, 'cws.c', 'Global.h', 'LG.h', 'Rat.h', 'cws_generate.h']]
    used += sorted((repo / 'src/cws').glob('*.[ch]'))
    manifest = {'schema': 1, 'created': started, 'commit': git(repo, 'rev-parse', 'HEAD'),
                'branch': git(repo, 'branch', '--show-current'), 'palp_commit': git(repo / 'PALP', 'rev-parse', 'HEAD'),
                'dirty': bool(git(repo, 'status', '--porcelain')), 'cpu': cpu,
                'architecture': platform.machine(), 'platform': platform.platform(),
                'compiler': subprocess.check_output([args.cc, '--version'], text=True).splitlines()[0],
                'flags': flags, 'valgrind': subprocess.check_output([args.valgrind, '--version'], text=True).strip(),
                'source_sha256': {str(p.relative_to(repo)): digest(p) for p in used},
                'harness_sha256': {p.name: digest(p) for p in [repo / 'src/benchmarks/geometry.c', repo / 'src/benchmarks/function-timing.c',
                                                              pathlib.Path(__file__), PACKAGE / 'scripts/cws_profile_format.py']},
                'measurement': {'methodology': args.methodology, 'machine': args.machine,
                                'note': args.measurement_note,
                                'execution': {'model': 'serial', 'workers': 1, 'threads_per_worker': 1,
                                              'isolation': 'single process per workload',
                                              'cpu_affinity': sorted(os.sched_getaffinity(0)),
                                              'thread_environment': {k: os.environ[k] for k in
                                                  ['OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS'] if k in os.environ}}},
                'smoke_test': args.limit is not None, 'repetitions': args.repetitions, 'warmups': 1,
                'complete': False, 'cases': []}
    for case in cases:
        manifest['cases'].append(one_case(case, binaries, output, args.repetitions, args.valgrind))
        save(output / 'run.json', manifest)
    manifest['complete'] = True
    save(output / 'run.json', manifest)
    print('Profile complete:', output / 'run.json', flush=True)


if __name__ == '__main__':
    main()

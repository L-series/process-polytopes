#!/usr/bin/env python3
"""Export reconciled per-function results and a self-contained history dashboard."""
import argparse
import csv
import hashlib
import json
import pathlib
import sys

sys.dont_write_bytecode = True

from cws_profile_format import normalize_instructions, parse_callgrind

OWN = {'cws.c', 'cws_dim5.h', 'cws_generate.h', 'main.c', 'catalogue.c', 'weight_pool.c', 'Global.h', 'LG.h', 'Rat.h', 'Coord.c', 'Rat.c', 'Vertex.c', 'Polynf.c', 'LG.c'}
TEMPLATE = pathlib.Path(__file__).resolve().parents[1] / 'src/reporting/dashboard.html'


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')


def short_file(value):
    return pathlib.Path(value).name


def functions(case):
    records = {}
    for source in case['instructions']['functions']:
        if short_file(source['file']) not in OWN:
            continue
        key = short_file(source['file']) + ':' + source['name']
        target = records.setdefault(key, {'function': source['name'], 'file': short_file(source['file'])})
        target.update({k: v for k, v in source.items() if k not in ['name', 'file']})
        target['instruction_calls'] = target.pop('calls')
    for source in case['instrumented_timing']['functions']:
        if short_file(source['file']) not in OWN:
            continue
        key = short_file(source['file']) + ':' + source['name']
        target = records.setdefault(key, {'function': source['name'], 'file': short_file(source['file'])})
        target.update({k: v for k, v in source.items() if k not in ['name', 'file', 'calls']})
        target['instrumented_calls'] = source['calls']
    return sorted(records.values(), key=lambda f: f.get('self_instructions_per_cws', -1), reverse=True)


def measurement_protocol(run):
    # Legacy records predate explicit execution metadata; never infer affinity.
    return run.get('measurement', {
        'methodology': 'legacy-serial-v1', 'machine': 'legacy-local-machine',
        'execution': {'model': 'serial', 'workers': 1, 'threads_per_worker': 1,
                      'isolation': 'single process per workload', 'cpu_affinity': 'unrecorded'}})


def compact(run):
    result = {k: v for k, v in run.items() if k != 'cases'}
    result['measurement'] = measurement_protocol(run)
    result['cases'] = []
    for case in run['cases']:
        protocol = {k: v for k, v in result['measurement'].items() if k != 'note'}
        compatibility = {'schema': 2, 'measurement': protocol,
                         'producer': run['harness_sha256'].get('profile-cws.py', 'unrecorded'),
                         'repetitions': run['repetitions'], 'warmups': run['warmups'],
                         'boundary': case['boundary'], 'cpu': run['cpu'], 'architecture': run['architecture'],
                         'compiler': run['compiler'], 'flags': run['flags'], 'valgrind': run['valgrind'],
                         'harness': run['harness_sha256'],
                         'counter_parser': run['harness_sha256'].get('cws_profile_format.py'), 'workload': case['workload']}
        # Analysis/renderer changes need not split scientific series; actual
        # geometry and timing harness changes do.
        compatibility['harness'] = {k: v for k, v in run['harness_sha256'].items()
                                    if k in ['geometry.c', 'function-timing.c']}
        key = hashlib.sha256(json.dumps(compatibility, sort_keys=True).encode()).hexdigest()
        result['cases'].append({'name': case['name'], 'rows': case['rows'], 'kind': case['kind'],
                                'compatibility': key, 'native': case['native'], 'hardware': case['hardware'],
                                'boundary': case['boundary'], 'functions': functions(case),
                                'total_instructions': case['instructions']['total_instructions'],
                                'instrumentation_ratio': case['instrumented_timing']['native_process_ratio']})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--history', type=pathlib.Path)
    args = parser.parse_args()
    run = json.loads(args.run.read_text())
    if not run.get('complete'):
        parser.error('refusing a partial or failed profiling run')
    if run['smoke_test']:
        # Smoke artifacts remain useful, but cannot enter a real history.
        if args.history:
            parser.error('smoke results cannot be appended to benchmark history')
    for case in run['cases']:
        for stage in ['point', 'hull']:
            ns = case['native'].get(f'{stage}_mean_ns_per_cws')
            if ns:
                case['native'][f'{stage}_cws_per_second'] = 1e9 / ns
    # Reparse raw counters, preserving the independently measured native times.
    # Reconciliation rejects double counting/missing instruction events.
    reparsed = False
    for case in run['cases']:
        raw = args.run.parent / case['name'].replace('/', '-') / 'callgrind.out'
        if raw.exists():
            reparsed = True
            case['instructions'] = normalize_instructions(parse_callgrind(raw), case['rows'])
    parser_path = pathlib.Path(__file__).with_name('cws_profile_format.py')
    if reparsed:
        run['harness_sha256']['cws_profile_format.py'] = hashlib.sha256(parser_path.read_bytes()).hexdigest()
    snapshot = compact(run)
    history = []
    if args.history and args.history.exists():
        history = json.loads(args.history.read_text())
        if not isinstance(history, list):
            raise ValueError('History must be a list of immutable run snapshots')
    identity = (snapshot['created'], snapshot['commit'], snapshot['source_sha256'])
    if not any((h['created'], h['commit'], h['source_sha256']) == identity for h in history):
        history.append(snapshot)
    history.sort(key=lambda h: (h['created'], h['commit']))
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    save(output / 'history.json', history)
    save(output / 'latest.json', run)
    save(output / 'callgraph.json', [{'case': c['name'], 'edges': c['instructions']['edges']} for c in run['cases']])
    all_rows = []
    for case in run['cases']:
        for f in functions(case):
            all_rows.append({'case': case['name'], 'cws': case['rows'], **f})
    columns = ['case', 'function', 'file', 'cws', 'instruction_calls', 'self_instructions',
               'inclusive_instructions', 'self_instructions_per_cws', 'inclusive_instructions_per_cws',
               'self_instructions_per_call', 'inclusive_instructions_per_call', 'instrumented_calls',
               'self_ns', 'inclusive_ns', 'self_time_percent', 'inclusive_time_percent', 'self_ns_per_cws', 'inclusive_ns_per_cws',
               'self_ns_per_call', 'inclusive_ns_per_call', 'line']
    with (output / 'functions.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, columns, extrasaction='raise', lineterminator='\n')
        writer.writeheader()
        writer.writerows(all_rows)
    # Avoid script-tag termination even when repository/branch labels are hostile.
    payload = json.dumps(history, separators=(',', ':')).replace('<', '\\u003c')
    output.joinpath('index.html').write_text(TEMPLATE.read_text().replace('__CWS_HISTORY__', payload))
    print('Wrote', output / 'index.html', 'with', len(history), 'run(s)')


if __name__ == '__main__':
    main()

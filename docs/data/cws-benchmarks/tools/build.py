#!/usr/bin/env python3
"""Select and independently recount reproducible 5D IP CWS benchmark inputs."""
import argparse
import collections
import hashlib
import heapq
import itertools
import json
import pathlib
import re
import resource
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[4]
DEFAULT_OUTPUT = pathlib.Path(__file__).resolve().parents[1]
SOURCES = [('cws3-d1-20-cwsx.txt', 3), ('cws11.txt', 11),
           ('cws24.txt', 24), ('cws28.txt', 28)]
BANDS = {'small': (6, 30), 'medium': (50, 150), 'large': (500, None)}


def sha256(path):
    with path.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256')
    return digest.hexdigest()


def canonical(matrix):
    """Exact identity under equation and ambient-coordinate permutations.

    This is CWS identity, not polytope normal-form equivalence. Hashes order
    sampling; full tuple equality decides identity.
    """
    return min((tuple(row[0] for row in rows),
                tuple(sorted(tuple(row[j] for row in rows)
                             for j in range(1, len(rows[0])))))
               for rows in itertools.permutations(matrix))


def encode(key):
    degrees, columns = key
    return '  '.join(' '.join(map(str, (degree, *(c[i] for c in columns))))
                     for i, degree in enumerate(degrees))


def parse(line, arity):
    numbers, marker, annotation = line.partition(' M:')
    if not marker:
        raise ValueError('Missing PALP M annotation')
    values = tuple(map(int, numbers.split()))
    width = 6 + arity
    if len(values) != arity * width:
        raise ValueError('Wrong width for a five-dimensional CWS')
    matrix = tuple(values[i:i + width] for i in range(0, len(values), width))
    for row in matrix:
        if min(row[1:]) < 0 or sum(row[1:]) != row[0] or row[0] <= 0:
            raise ValueError('Invalid degree/weights')
    if any(not any(row[j] for row in matrix) for j in range(1, width)):
        raise ValueError('Unused ambient coordinate')
    match = re.fullmatch(r'(\d+) (\d+) (?:N:(\d+) (\d+)|F:(\d+) N:(\d+))', annotation.strip())
    if not match:
        raise ValueError('Unexpected PALP annotation')
    return matrix, int(match[1]), int(match[2]), match[3] is not None


class Sample:
    def __init__(self, count):
        self.count = count
        self.entries = {}
        self.heap = []
        self.forced = {}

    def add(self, key, metadata):
        # Include every available >=3000-point system in the large band.
        forced = metadata['points'] >= 3000
        target = self.forced if forced else self.entries
        if key in target:
            old = target[key][1]
            for field in ['points', 'vertices', 'reflexive']:
                if old[field] != metadata[field]:
                    raise ValueError(f'Conflicting source annotations: {field}')
            return
        encoded = encode(key)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        score = int(digest, 16)
        metadata = {'id': digest, **metadata}
        if forced:
            target[key] = (encoded, metadata)
            return
        if len(self.heap) == self.count:
            if score >= -self.heap[0][0]:
                return
            _, removed = heapq.heappop(self.heap)
            del self.entries[removed]
        heapq.heappush(self.heap, (-score, key))
        self.entries[key] = (encoded, metadata)

    def finish(self):
        if len(self.forced) > self.count:
            raise ValueError('Forced extremes exceed requested sample size')
        ordinary = sorted(self.entries.values(), key=lambda item: item[1]['id'])
        chosen = list(self.forced.values()) + ordinary[:self.count - len(self.forced)]
        if len(chosen) != self.count:
            raise ValueError(f'Only {len(chosen)} distinct CWS available; need {self.count}')
        return sorted(chosen, key=lambda item: item[1]['id'])


def quantiles(hist):
    result, total = {}, sum(hist.values())
    for q in (0, .1, .25, .5, .75, .9, .95, .99, 1):
        cumulative = 0
        for point, count in sorted(hist.items()):
            cumulative += count
            if cumulative >= max(1, q * total):
                result[str(q)] = point
                break
    return result


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')


def select(source_dir, output, count):
    descriptors = json.loads((ROOT / 'tests/descriptor-reference.json').read_text())
    descriptors = {d['id']: d for d in descriptors}
    signatures = {}
    for _, descriptor in SOURCES:
        d = descriptors[descriptor]
        signatures[descriptor] = canonical(tuple(
            (len(block), *(int(j in block) for j in range(1, d['ambient'] + 1)))
            for block in d['blocks']))
    samples = {name: Sample(count) for name in BANDS}
    histogram, records = collections.Counter(), []
    eligible = collections.Counter()
    for name, descriptor in SOURCES:
        path = source_dir / name
        source_hist = collections.Counter()
        source_rows = 0
        arity = len(descriptors[descriptor]['blocks'])
        for number, line in enumerate(path.open(), 1):
            if not line.strip():
                raise ValueError(f'{name}:{number}: blank row')
            matrix, points, vertices, reflexive = parse(line, arity)
            source_rows += 1
            source_hist[points] += 1
            band = next((band for band, (low, high) in BANDS.items()
                         if low <= points and (high is None or points <= high)), None)
            if band is None:
                continue
            eligible[band] += 1
            support = tuple((sum(x > 0 for x in row[1:]), *(int(x > 0) for x in row[1:]))
                            for row in matrix)
            if canonical(support) != signatures[descriptor]:
                raise ValueError(f'{name}:{number}: wrong descriptor support')
            key = canonical(matrix)
            samples[band].add(key, {'points': points, 'vertices': vertices,
                                   'reflexive': reflexive, 'descriptor': descriptor,
                                   'source': name, 'source_line': number})
        histogram.update(source_hist)
        records.append({'file': name, 'descriptor': descriptor, 'sha256': sha256(path),
                        'rows': source_rows, 'point_quantiles': quantiles(source_hist)})
        print(f'{name}: scanned {source_rows:,} rows', flush=True)
    output.mkdir(parents=True, exist_ok=True)
    manifest = {'schema': 1, 'dimension': 5, 'point_convention': 'all primal Newton-polytope lattice points; PALP M',
                'selection': 'smallest SHA-256 of exact equation/coordinate-canonical CWS; include all available >=3000-point cases',
                'source_scope': 'four local archived descriptor outputs, not a uniform sample of all 46 types',
                'sources': records, 'source_rows': sum(histogram.values()),
                'source_point_quantiles': quantiles(histogram), 'groups': {},
                'verification': {'status': 'pending'}}
    seen = set()
    for band, sample in samples.items():
        selected = sample.finish()
        hist, by_type = collections.Counter(), collections.Counter()
        for row, metadata in selected:
            # Distinct canonical CWS across the entire corpus.
            if row in seen:
                raise ValueError('Duplicate CWS across groups')
            seen.add(row)
            hist[metadata['points']] += 1
            by_type[metadata['descriptor']] += 1
        (output / f'{band}.cws').write_text(''.join(row + '\n' for row, _ in selected))
        (output / f'{band}.jsonl').write_text(''.join(json.dumps(meta, sort_keys=True, separators=(',', ':')) + '\n'
                                                  for _, meta in selected))
        low, high = BANDS[band]
        manifest['groups'][band] = {'rows': len(selected), 'eligible_source_rows_before_deduplication': eligible[band],
                                   'range': [low, high], 'point_quantiles': quantiles(hist),
                                   'point_histogram': dict(sorted(hist.items())),
                                   'descriptor_counts': dict(sorted(by_type.items())),
                                   'reflexive_rows': sum(meta['reflexive'] for _, meta in selected),
                                   'at_least_3000_points': sum(meta['points'] >= 3000 for _, meta in selected),
                                   'cws_sha256': sha256(output / f'{band}.cws'),
                                   'metadata_sha256': sha256(output / f'{band}.jsonl')}
        print(band, manifest['groups'][band]['point_quantiles'], dict(by_type), flush=True)
    write_json(output / 'manifest.json', manifest)


def verify(output, binary):
    manifest = json.loads((output / 'manifest.json').read_text())
    for band, group in manifest['groups'].items():
        path = output / f'{band}.cws'
        metadata_path = output / f'{band}.jsonl'
        if sha256(path) != group['cws_sha256'] or sha256(metadata_path) != group['metadata_sha256']:
            raise ValueError('Input or metadata fingerprint changed')
        metadata = [json.loads(line) for line in metadata_path.read_text().splitlines()]
        with path.open() as source:
            process = subprocess.Popen([str(binary)], stdin=source, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
            count = 0
            for line in process.stdout:
                fields = line.split()
                if len(fields) != 6:
                    process.kill()
                    raise ValueError(f'Unexpected verifier output: {line}')
                index, dimension, points, ip, vertices, reflexive = map(int, fields)
                if index != count + 1 or count >= len(metadata):
                    raise ValueError('Wrong verifier output row count/order')
                expected = metadata[count]
                if (dimension, points, ip, vertices, reflexive) != (
                    5, expected['points'], 1, expected['vertices'], int(expected['reflexive'])):
                    process.kill()
                    raise ValueError(f'{band} row {index}: PALP disagrees with source metadata')
                count += 1
            errors = process.stderr.read()
            status = process.wait()
        if status or errors or count != group['rows']:
            raise ValueError(f'{band}: verification failed ({status}, {count} rows): {errors}')
        print(f'{band}: PALP recounted and IP-checked all {count:,} rows', flush=True)
    used = ['Coord.c', 'Rat.c', 'Vertex.c', 'Polynf.c', 'LG.c', 'Global.h', 'LG.h', 'Rat.h']
    manifest['verification'] = {
        'status': 'passed', 'rows': sum(g['rows'] for g in manifest['groups'].values()),
        'checks': ['dimension=5', 'exact primal lattice-point count', 'IP', 'vertex count', 'reflexivity flag'],
        'tool': 'direct PALP Read_CWS_PP + IP_Check, fresh point enumeration, no supplied point lists',
        'verifier_sha256': sha256(binary),
        'verifier_source_sha256': sha256(pathlib.Path(__file__).with_name('verify.c')),
        'palp_commit': subprocess.check_output(['git', '-C', str(ROOT / 'PALP'), 'rev-parse', 'HEAD'], text=True).strip(),
        'palp_source_sha256': {name: sha256(ROOT / 'PALP' / name) for name in used}}
    write_json(output / 'manifest.json', manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sources', type=pathlib.Path, default=ROOT / 'PALP/cws')
    parser.add_argument('--output', type=pathlib.Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--count', type=int, default=10000)
    parser.add_argument('--verifier', type=pathlib.Path)
    parser.add_argument('--verify-only', action='store_true')
    args = parser.parse_args()
    if args.count < 1:
        parser.error('count must be positive')
    _, hard = resource.getrlimit(resource.RLIMIT_STACK)
    resource.setrlimit(resource.RLIMIT_STACK, (hard, hard))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if not args.verify_only:
        select(args.sources.resolve(), args.output.resolve(), args.count)
    if args.verifier:
        verify(args.output.resolve(), args.verifier.resolve())
    elif args.verify_only:
        parser.error('--verify-only requires --verifier')


if __name__ == '__main__':
    main()

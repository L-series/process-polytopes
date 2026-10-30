#!/usr/bin/env python3
"""Exact bounded descriptor checks; independent oracle imported from C01/C02."""
import argparse
import collections
import json
import pathlib
import re
import resource
import subprocess
import tempfile
import unittest

from cws_reference import Reference, canonical

ROOT = pathlib.Path(__file__).resolve().parents[1]
CATALOGUE = json.loads((ROOT / 'tests/descriptor-reference.json').read_text())


def run(args):
    return subprocess.run(list(map(str, args)), text=True, capture_output=True, timeout=120)


def emitted(result, descriptor):
    if result.returncode:
        raise AssertionError(result.stderr + result.stdout)
    width = descriptor['ambient'] + 1
    rows = []
    for line in result.stdout.splitlines():
        numbers, marker, annotation = line.partition(' M:')
        assert marker, line
        values = tuple(map(int, numbers.split()))
        assert len(values) == width * len(descriptor['blocks']), line
        rows.append(tuple(values[i:i + width] for i in range(0, len(values), width)))
    assert re.fullmatch(r'CWS candidates: \d+\n', result.stderr), result.stderr
    return rows


class DescriptorTests(unittest.TestCase):
    def test_catalogue_matches_reviewed_descriptors(self):
        # Compile only the actual constructor; compare every field with the
        # frozen coordinate-block catalogue from the independently checked C02.
        with tempfile.TemporaryDirectory() as tmp:
            source = pathlib.Path(tmp) / 'catalogue.c'
            binary = pathlib.Path(tmp) / 'catalogue'
            source.write_text('''#include <stdio.h>
#include <string.h>
#include "Global.h"
#include "catalogue.h"
int main(void) {
  CWSDescriptor d;
  if (ProjectCWSDescriptor(1, &d) || ProjectCWSDescriptor(48, &d)) return 1;
  for (int id=2; id<=47; id++) {
    if (!ProjectCWSDescriptor(id, &d)) return 2;
    printf("%d %d %d", id, d.coordinate_count, d.row_count);
    for (int s=0; s<d.row_count; s++) {
      printf(" %d %d", d.row_sizes[s], d.shared_counts[s]);
      for (int j=0; j<d.row_sizes[s]; j++) printf(" %d", d.mappings[s][j]);
    }
    puts("");
  }
}
''')
            built = run(['cc', '-std=c99', '-DPOLY_Dmax=5', '-I', SOURCE_DIR, '-I', ROOT / 'src/cws',
                         source, ROOT / 'src/cws/catalogue.c', '-o', binary])
            self.assertEqual(built.returncode, 0, built.stderr)
            actual = run([binary])
            self.assertEqual(actual.returncode, 0, actual.stderr)
            self.assertEqual(len(actual.stdout.splitlines()), 46)
            for line, descriptor in zip(actual.stdout.splitlines(), CATALOGUE):
                expected = [descriptor['id'], descriptor['ambient'], len(descriptor['blocks'])]
                for block, shared in zip(descriptor['blocks'], descriptor['shared']):
                    expected += [len(block), shared, *block]
                self.assertEqual(list(map(int, line.split())), expected)
                self.assertEqual(descriptor['ambient'] - len(descriptor['blocks']), 5)

    def test_all_types_against_unpruned_exact_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = pathlib.Path(tmp)
            # Unequal weights exercise shared-coordinate permutations. Two
            # entries per size also exercise weight-selection ordering.
            weights = {2: [(1, 1)], 3: [(1, 1, 1), (1, 1, 2)],
                       4: [(1, 1, 1, 1), (1, 1, 1, 2)],
                       5: [(1, 1, 1, 1, 1), (1, 1, 1, 1, 2)]}
            for size, pool in weights.items():
                (directory / f'{size - 1}d.txt').write_text(''.join(
                    ' '.join(map(str, w)) + '\n' for w in pool))
                (directory / f'pool{size}.txt').write_text(''.join(
                    ' '.join(map(str, (sum(w), *w))) + '\n' for w in pool))
            for d in CATALOGUE:
                with self.subTest(type=d['id']):
                    reference = Reference({'structures': [{'blocks': d['blocks'], 'pools': [
                        f'pool{len(b)}.txt' for b in d['blocks']]}]}, directory, 5)
                    self.assertEqual(reference.failures, [])
                    result = run([BINARY, '-n5', f"-s{d['id']}", '--weights-dir', directory])
                    rows = emitted(result, d)
                    keys = {canonical(row) for row in rows}
                    self.assertEqual(keys, reference.ip_keys(), 'Missing or unexpected IP classes')
                    for row, line in zip(rows, result.stdout.splitlines()):
                        record = reference.classes[canonical(row)]
                        annotation = line.split(' M:')[1]
                        self.assertEqual(int(annotation.split()[0]), record['points'])

    def test_slot_order_requires_descriptor_symmetry(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = pathlib.Path(tmp) / 'slot-symmetry.c'
            binary = pathlib.Path(tmp) / 'slot-symmetry'
            source.write_text('''#include "Global.h"
#include "cws_generate.h"
#include "catalogue.h"
int main(void) {
  CWSDescriptor d;
  int swap[CWS_MAX_ROWS] = {1, 0, 2, 3, 4};
  assert(ProjectCWSDescriptor(12, &d));
  /* These slots use the same pool, but play different descriptor roles. */
  assert(d.row_sizes[0] == d.row_sizes[1]);
  assert(!CWSDescriptorAutomorphism(&d, swap));
  assert(ProjectCWSDescriptor(3, &d));
  assert(CWSDescriptorAutomorphism(&d, swap));
}''')
            result = run(['cc', '-O0', '-DPOLY_Dmax=5', '-ffunction-sections',
                          '-fdata-sections', '-DPALP_CWS_NO_MAIN', '-Wl,--gc-sections', '-I', SOURCE_DIR, '-I', ROOT / 'src/cws',
                          source, ROOT / 'src/cws/catalogue.c', SOURCE_DIR / 'cws.c', '-o', binary])
            self.assertEqual(result.returncode, 0, result.stderr)
            checked = run([binary])
            self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_type12_larger_pruning_regression(self):
        # Frozen C02 exact-oracle result for 5 four-weight and 6 five-weight
        # systems: the old future-blind prefix pruning lost 4,859 IP classes.
        with tempfile.TemporaryDirectory() as tmp:
            directory = pathlib.Path(tmp)
            for dim, count in ((3, 5), (4, 6)):
                # The committed complete pools have PALP reverse-lexicographic
                # order; use the original fixture ordering for this regression.
                fixture = FIXTURES / ('ws-3d-first5.txt' if dim == 3 else 'ws-4d-first1000.txt')
                lines = fixture.read_text().splitlines()[:count]
                (directory / f'{dim}d.txt').write_text(''.join(
                    ' '.join(line.split()[1:]) + '\n' for line in lines))
            result = run([BINARY, '-n5', '-s12', '--weights-dir', directory])
            rows = emitted(result, CATALOGUE[10])
            self.assertEqual(len(rows), 5994)
            self.assertEqual(len({canonical(row) for row in rows}), 5994)
            self.assertEqual(result.stderr, 'CWS candidates: 9072\n')

    def test_errors_fail_before_output(self):
        bad_args = [[], ['-s'], ['-s1'], ['-s48'], ['-s2junk'], ['-s2', '-s3'],
                    ['-t2'], ['-s2', '--weights-dir', '/nonexistent']]
        for args in bad_args:
            with self.subTest(args=args):
                result = run([BINARY, '-n5', *args])
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, '')
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / '4d.txt'
            for text in ['', '1 1 1 1\n', '1 1 1 1 2 extra\n', '2 2 2 2 2\n',
                         '2 1 1 1 1\n', '1 1 1 1 2147483647\n', '1 1 1 1 2\n\n']:
                with self.subTest(text=text):
                    path.write_text(text)
                    result = run([BINARY, '-n5', '-s2', '--weights-dir', tmp])
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, '')

    def test_full_small_builtin_types_match_legacy(self):
        # Complete 2D x 3D product; old/new paths do identical IP and dual work.
        directory = WEIGHTS_DIR
        with tempfile.TemporaryDirectory() as tmp:
            paths = []
            for dim in (2, 3):
                path = pathlib.Path(tmp) / f'ws{dim}.txt'
                path.write_text(''.join(str(sum(w)) + ' ' + ' '.join(map(str, w)) + '\n'
                    for w in [tuple(map(int, l.split()))
                              for l in (directory / f'{dim}d.txt').read_text().splitlines()]))
                paths.append(path)
            old = run([BINARY, '-c5', '-n2', *paths, '-t', '0', '0'])
            new = run([BINARY, '-n5', '-s5', '--weights-dir', directory])
            self.assertEqual(old.returncode, 0, old.stderr)
            rows = emitted(new, CATALOGUE[3])
            width = CATALOGUE[3]['ambient'] + 1
            old_rows = [tuple(tuple(values[i:i + width]) for i in range(0, len(values), width))
                        for values in [list(map(int, line.split(' M:')[0].split()))
                                       for line in old.stdout.splitlines()]]
            self.assertEqual(collections.Counter(map(canonical, old_rows)),
                             collections.Counter(map(canonical, rows)))
            self.assertEqual(len(rows), 285)
            self.assertEqual(new.stderr, 'CWS candidates: 285\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=pathlib.Path, default=ROOT / 'build/cws.x')
    parser.add_argument('--source-dir', type=pathlib.Path, default=ROOT / 'PALP')
    parser.add_argument('--fixtures', type=pathlib.Path, default=ROOT / 'tests/fixtures/cws/inputs')
    parser.add_argument('--weights-dir', type=pathlib.Path, default=ROOT / 'docs/data/single-weight-systems')
    options, remaining = parser.parse_known_args()
    BINARY = options.binary.resolve()
    SOURCE_DIR = options.source_dir.resolve()
    WEIGHTS_DIR = options.weights_dir.resolve()
    FIXTURES = options.fixtures.resolve()
    _, hard = resource.getrlimit(resource.RLIMIT_STACK)
    resource.setrlimit(resource.RLIMIT_STACK, (hard, hard))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    unittest.main(argv=[__file__, *remaining])

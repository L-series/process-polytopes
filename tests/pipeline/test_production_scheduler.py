#!/usr/bin/env python3
"""Mocked production orchestration tests; launches no CUDA or PALP binaries."""
import argparse
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
spec = importlib.util.spec_from_file_location('production_scheduler', ROOT / 'scripts/run_cws_production.py')
scheduler = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = scheduler
spec.loader.exec_module(scheduler)


class MockProcesses:
    def __init__(self, root, fail_job=None, fail_audit=False, omit_coverage=False, change_driver=None):
        self.root = root
        self.fail_job = fail_job
        self.fail_audit = fail_audit
        self.omit_coverage = omit_coverage
        self.change_driver = change_driver
        self.lock = threading.Lock()
        self.active = set()
        self.jobs = []
        self.audits = []
        self.peak = 0
        self.first_wave = threading.Barrier(8)

    def __call__(self, command, **streams):
        if command[1].endswith('audit_cws_run.py'):
            with self.lock:
                assert not self.active
                self.audits.append(command)
                assert len(self.jobs) == 53
            assert '--require-cpu' in command
            if not self.fail_audit and not self.omit_coverage:
                (self.root / 'execution-coverage.json').write_text('{"job_count":53,"mock":true}\n')
            return subprocess.CompletedProcess(command, int(self.fail_audit))
        assert command[1].endswith('run_cws_job.py')
        def value(option):
            return command[command.index(option) + 1]
        identity = tuple(int(value(option)) for option in ('--structure-id','--shard-count','--shard-index'))
        device = int(value('--cuda-device'))
        with self.lock:
            assert device not in self.active, f'overlapping jobs on GPU {device}'
            self.active.add(device)
            self.peak = max(self.peak, len(self.active))
            self.jobs.append((identity, device, command))
        if identity[0] == 3:
            self.first_wave.wait(timeout=10)
        failed = identity == self.fail_job
        if not failed:
            time.sleep(0.01)
        streams['stdout'].write(b'mocked per-job GPU and CPU completion\n')
        with self.lock:
            self.active.remove(device)
            if self.change_driver and len(self.jobs) == 53 and not self.active:
                self.change_driver.write_bytes(b'changed CPU driver')
        return subprocess.CompletedProcess(command, int(failed))


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='scheduler-test-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        weights = self.base / 'weights'
        weights.mkdir()
        for name in ('2d.txt','3d.txt','4d.txt'):
            (weights / name).write_text('1 1 1\n')
        binary = self.base / 'gpu.x'; binary.write_bytes(b'mock GPU binary')
        driver = self.base / 'cpu.x'; driver.write_bytes(b'mock CPU driver')
        counts = self.base / 'counts.md'; counts.write_text('mock count reference\n')
        self.args = argparse.Namespace(binary=binary, cpu_driver=driver, weights_dir=weights,
            counts=counts, run_root=self.base/'run', devices=list(range(8)), chunk_capacity=100,
            slice_capacity=20, np_cap=32, threads=128, blocks=0, cpu_workers=1,
            cpu_chunk_rows=10, dry_run=False)

    def execute(self, mock):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return scheduler.run(self.args, invoke=mock)

    def test_exact_53_jobs_and_exclusive_device_workers(self):
        mock = MockProcesses(self.args.run_root)
        result = self.execute(mock)
        expected = {(3,8,k) for k in range(8)} | {(s,1,0) for s in range(2,48) if s != 3}
        self.assertEqual({identity for identity, _, _ in mock.jobs}, expected)
        self.assertEqual(len(mock.jobs), len(expected))
        self.assertEqual(mock.peak, 8)
        self.assertEqual({device for identity, device, _ in mock.jobs if identity[0] == 3}, set(range(8)))
        self.assertEqual(len(mock.audits), 1)
        self.assertEqual(result['job_count'], 53)
        self.assertTrue((self.args.run_root/'production-execution.json').is_file())
        for _, _, command in mock.jobs:
            self.assertEqual(Path(command[command.index('--cpu-driver')+1]), self.args.cpu_driver)
        manifest = json.loads((self.args.run_root/'production-plan.json').read_text())
        self.assertEqual(len(manifest['jobs']), 53)
        self.assertTrue(all(job['structure_id'] == 3 for job in manifest['jobs'][:8]))

    def test_job_failure_prevents_final_audit_and_marker(self):
        mock = MockProcesses(self.args.run_root, fail_job=(3,8,0))
        with self.assertRaisesRegex(RuntimeError, 'no final audit'):
            self.execute(mock)
        self.assertFalse(mock.audits)
        self.assertFalse((self.args.run_root/'production-execution.json').exists())
        status = json.loads((self.args.run_root/'scheduler-status.json').read_text())
        self.assertEqual(status['failed'][0]['job'], 's03-shard-000-of-008')
        self.assertGreater(len(status['unstarted']), 0)
        self.assertTrue((self.args.run_root/'scheduler-logs').is_dir())

    def test_failed_final_audit_prevents_marker(self):
        mock = MockProcesses(self.args.run_root, fail_audit=True)
        with self.assertRaisesRegex(RuntimeError, 'final execution coverage audit failed'):
            self.execute(mock)
        self.assertEqual(len(mock.audits), 1)
        self.assertFalse((self.args.run_root/'production-execution.json').exists())

    def test_success_exit_without_audit_receipt_is_incomplete(self):
        mock = MockProcesses(self.args.run_root, omit_coverage=True)
        with self.assertRaisesRegex(RuntimeError, 'no execution-coverage'):
            self.execute(mock)
        self.assertFalse((self.args.run_root/'production-execution.json').exists())

    def test_driver_change_prevents_audit(self):
        mock = MockProcesses(self.args.run_root, change_driver=self.args.cpu_driver)
        with self.assertRaisesRegex(RuntimeError, 'changed during production'):
            self.execute(mock)
        self.assertFalse(mock.audits)
        self.assertFalse((self.args.run_root/'production-execution.json').exists())

    def test_dry_run_launches_nothing(self):
        self.args.dry_run = True
        def forbidden(*_, **__):
            self.fail('dry run launched a subprocess')
        manifest = self.execute(forbidden)
        self.assertEqual(len(manifest['jobs']), 53)
        self.assertFalse(self.args.run_root.exists())

    def test_device_identity_validation(self):
        for devices in (list(range(7)), [0]*8, [-1,*range(7)]):
            with self.assertRaisesRegex(ValueError, 'eight distinct'):
                scheduler.plan_jobs(devices)
        custom = scheduler.plan_jobs(list(range(8,16)))
        self.assertEqual([job.cuda_device for job in custom[:8]], list(range(8,16)))

    def test_cli_with_mock_gpu_and_cpu_binaries(self):
        # Exercise the real single-job runner, CPU completion receipt generation
        # and final auditor. Only the GPU/CPU executables and counts are fake.
        state = self.base / 'mock-device-state.json'
        state.write_text('{"active":[],"jobs":[],"finished":0}')
        gpu_source = """#!/usr/bin/env python3
import argparse, fcntl, json, sys, time
from pathlib import Path
p=argparse.ArgumentParser()
for name in ('structure-id','shard-count','shard-index','cuda-device'):
    p.add_argument('--'+name,type=int,required=True)
p.add_argument('--accepted-output',type=Path,required=True)
p.add_argument('--deferred-output',type=Path,required=True)
a,_=p.parse_known_args()
state=Path(STATE_FILE)
def update(enter):
    with state.open('r+') as f:
        fcntl.flock(f,fcntl.LOCK_EX)
        value=json.load(f)
        if enter:
            assert a.cuda_device not in value['active'], 'device overlap'
            value['active'].append(a.cuda_device)
            value['jobs'].append([a.structure_id,a.shard_count,a.shard_index,a.cuda_device])
        else:
            value['active'].remove(a.cuda_device)
            value['finished']+=1
        f.seek(0);json.dump(value,f);f.truncate();f.flush()
update(True)
time.sleep(0.02)
update(False)
a.accepted_output.write_bytes(b'')
a.deferred_output.write_bytes(b'')
size=16 if a.structure_id==3 else 1
start=size*a.shard_index//a.shard_count
end=size*(a.shard_index+1)//a.shard_count
receipt=dict(structure_id=a.structure_id,shard_count=a.shard_count,shard_index=a.shard_index,
    identity_audited=True,scan_space=size,range_start=start,range_count=end-start,
    scanned_selections=end-start,generated=1,processed=1,accepted=0,rejected=1,deferred=0)
print('CWS_GPU_COMPLETION_V1 '+json.dumps(receipt),file=sys.stderr)
"""
        self.args.binary.write_text(gpu_source.replace('STATE_FILE', repr(str(state))))
        self.args.binary.chmod(0o755)
        self.args.cpu_driver.write_text(
            '#!/usr/bin/env python3\nimport sys,json\n'
            'n=sum(1 for _ in sys.stdin)\n'
            'print("CWS_COMPLETION_V1 "+json.dumps(dict(processed=n,accepted=0,rejected=n)),file=sys.stderr)\n')
        self.args.cpu_driver.chmod(0o755)
        self.args.counts.write_text(''.join(
            f'| {i} | {structure} | mock | mock | {8 if structure == 3 else 1} | mock |\n'
            for i, structure in enumerate(range(2,48),1)))
        command = [sys.executable,str(ROOT/'scripts/run_cws_production.py'),
            '--binary',str(self.args.binary),'--cpu-driver',str(self.args.cpu_driver),
            '--weights-dir',str(self.args.weights_dir),'--counts',str(self.args.counts),
            '--run-root',str(self.args.run_root)]
        result=subprocess.run(command,text=True,capture_output=True,timeout=60)
        self.assertEqual(result.returncode,0,result.stderr)
        observed=json.loads(state.read_text())
        self.assertEqual(observed['finished'],53)
        self.assertFalse(observed['active'])
        self.assertEqual(len({tuple(row[:3]) for row in observed['jobs']}),53)
        self.assertEqual({row[3] for row in observed['jobs'] if row[0]==3},set(range(8)))
        final=json.loads((self.args.run_root/'production-execution.json').read_text())
        self.assertEqual(final['job_count'],53)
        coverage=json.loads((self.args.run_root/'execution-coverage.json').read_text())
        self.assertEqual(len(coverage['cpu_completion']),53)
        self.assertEqual(coverage['deferred'],0)

    def test_existing_run_is_preserved(self):
        self.args.run_root.mkdir()
        previous = self.args.run_root/'production-execution.json'
        previous.write_text('old result\n')
        def forbidden(*_, **__):
            self.fail('an existing run launched another job')
        with self.assertRaises(FileExistsError):
            self.execute(forbidden)
        self.assertEqual(previous.read_text(), 'old result\n')


if __name__ == '__main__':
    unittest.main()

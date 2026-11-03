#!/usr/bin/env python3
"""Failure-injection tests for GPU/deferred execution receipts (not IP proofs)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]
SCRIPTS=ROOT/'scripts'
sys.path.insert(0,str(SCRIPTS))
from complete_deferred import digest,atomic_json
from audit_cws_run import expected_counts
REAL_DRIVER=None


def executable(path,code):
    path.write_text('#!'+sys.executable+'\n'+code)
    path.chmod(0o755)
    return path


def fixture():
    positive=[]
    for r in range(5):positive += [2]+[int(i//2==r) for i in range(10)]
    negative=[]
    for d,w in [(6,[1,2,3,0,0,0,0,0]),(6,[3,0,0,1,2,0,0,0]),(3,[0,0,0,0,0,1,1,1])]:negative += [d]+w
    return ' '.join(map(str,positive))+'\n'+' '.join(map(str,negative))+'\n'+' '.join(map(str,positive))+'\n'


class CompletionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='completion-tests-')
        self.root=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def runner(self,driver,input_file,expected,work='work'):
        return subprocess.run([sys.executable,str(SCRIPTS/'complete_deferred.py'),'--driver',str(driver),
            '--work-dir',str(self.root/work),'--expected-rows',str(expected),'--chunk-rows','1','--workers','1',str(input_file)],
            capture_output=True,text=True)
    def cpu(self,mode='good'):
        extra=''
        if mode=='exit0':extra='sys.exit(0)\n'
        if mode=='partial':extra='rows=rows[:-1]\n'
        if mode=='resume':extra=f'''marker=pathlib.Path({str(self.root/'fault-used')!r})
if rows==['2\\n'] and not marker.exists():
    marker.write_text('fault injected');sys.exit(0)
'''
        return executable(self.root/'cpu.py','''import sys,json,pathlib
rows=sys.stdin.readlines()
'''+extra+'''sys.stdout.writelines(rows)
print('CWS_COMPLETION_V1 '+json.dumps(dict(processed=len(rows),accepted=len(rows),rejected=0)),file=sys.stderr)
''')
    def test_duplicate_records_preserved(self):
        inp=self.root/'deferred';inp.write_text('1\n2\n1\n')
        r=self.runner(self.cpu(),inp,3)
        self.assertEqual(r.returncode,0,r.stderr)
        c=json.loads((self.root/'work/completion.json').read_text())
        self.assertEqual((c['processed'],c['accepted']),(3,3))
        self.assertEqual((self.root/'work/accepted-cpu.txt').read_text(),inp.read_text())
        self.assertTrue(inp.exists())
    def test_success_exit_without_footer_is_failure(self):
        inp=self.root/'deferred';inp.write_text('1\n')
        r=self.runner(self.cpu('exit0'),inp,1)
        self.assertNotEqual(r.returncode,0)
        self.assertFalse((self.root/'work/completion.json').exists())
        self.assertTrue(inp.exists())
    def test_short_read_receipt_is_failure(self):
        inp=self.root/'deferred';inp.write_text('1\n')
        self.assertNotEqual(self.runner(self.cpu('partial'),inp,1).returncode,0)
        self.assertFalse((self.root/'work/completion.json').exists())
    def test_missing_deferred_input_count_is_failure(self):
        inp=self.root/'deferred';inp.write_text('1\n')
        self.assertNotEqual(self.runner(self.cpu(),inp,2).returncode,0)
        self.assertFalse((self.root/'work/completion.json').exists())
    def test_resume_reuses_committed_chunks(self):
        inp=self.root/'deferred';inp.write_text('1\n2\n3\n')
        driver=self.cpu('resume')
        self.assertNotEqual(self.runner(driver,inp,3).returncode,0)
        first=self.root/'work/chunks/00000000.json'; previous=first.read_bytes(); stamp=first.stat().st_mtime_ns
        r=self.runner(driver,inp,3)
        self.assertEqual(r.returncode,0,r.stderr)
        self.assertEqual(first.read_bytes(),previous)
        self.assertEqual(first.stat().st_mtime_ns,stamp)
        self.assertEqual((self.root/'work/accepted-cpu.txt').read_text(),inp.read_text())
    def test_native_driver(self):
        if not REAL_DRIVER:self.skipTest('supply --driver to exercise native PALP completion')
        inp=self.root/'deferred';inp.write_text(fixture())
        r=self.runner(REAL_DRIVER,inp,3)
        self.assertEqual(r.returncode,0,r.stderr)
        c=json.loads((self.root/'work/completion.json').read_text())
        self.assertEqual((c['processed'],c['accepted'],c['rejected']),(3,2,1))
        bad=self.root/'malformed';bad.write_text('1 2 3\n')
        self.assertNotEqual(self.runner(REAL_DRIVER,bad,1,'badwork').returncode,0)
        self.assertFalse((self.root/'badwork/completion.json').exists())
    def gpu_job(self, mode='good'):
        if not REAL_DRIVER:self.skipTest('supply --driver to exercise native GPU/CPU orchestration')
        weights=self.root/'weights';weights.mkdir()
        for name in ('2d.txt','3d.txt','4d.txt'):(weights/name).write_text('bound mock input\n')
        gpu=executable(self.root/'gpu.py',"""import sys,json,pathlib
args=sys.argv
def option(flag):return args[args.index(flag)+1]
pathlib.Path(option('--accepted-output')).write_text('')
pathlib.Path(option('--deferred-output')).write_text("""+repr(fixture())+""")
receipt=dict(structure_id=2,shard_count=1,shard_index=0,scan_space=1,range_start=0,
             range_count=1,scanned_selections=1,generated=3,processed=3,accepted=0,
             rejected=0,deferred=3,identity_audited="""+repr(mode!='bad_identity')+""")
"""+("print('CWS_GPU_COMPLETION_V1 '+json.dumps(receipt),file=sys.stderr)\n" if mode!='no_receipt' else ''))
        run=self.root/'run'
        result=subprocess.run([sys.executable,str(SCRIPTS/'run_cws_job.py'),'--binary',str(gpu),
            '--weights-dir',str(weights),'--run-root',str(run),'--cpu-driver',str(REAL_DRIVER),
            '--structure-id','2','--cpu-chunk-rows','1'],capture_output=True,text=True)
        return result,run/'s02-shard-000-of-001'
    def test_gpu_job_completes_every_deferred_row(self):
        result,job=self.gpu_job()
        self.assertEqual(result.returncode,0,result.stderr)
        receipt=json.loads((job/'job.json').read_text())
        cpu=json.loads(Path(receipt['cpu_completion']['receipt']).read_text())
        self.assertEqual((cpu['processed'],cpu['accepted'],cpu['rejected']),(3,2,1))
        self.assertTrue((job/'deferred.ovf').exists())
    def test_gpu_job_success_exit_without_receipt_is_incomplete(self):
        result,job=self.gpu_job('no_receipt')
        self.assertNotEqual(result.returncode,0)
        self.assertFalse((job/'job.json').exists())
    def test_gpu_job_requires_identity_audit(self):
        result,job=self.gpu_job('bad_identity')
        self.assertNotEqual(result.returncode,0)
        self.assertFalse((job/'job.json').exists())
    def job_tree(self):
        counts=expected_counts(ROOT/'CWS_TYPE_COUNTS.md')
        run=self.root/'gpu';run.mkdir()
        for s in range(2,48):
            n=8 if s==3 else 1
            for k in range(n):
                job=run/f's{s:02d}-shard-{k:03d}-of-{n:03d}';job.mkdir()
                generated=counts[s]*(k+1)//n-counts[s]*k//n
                a=job/'accepted.txt';a.write_text('');d=job/'deferred.ovf';d.write_text('')
                gpu=dict(structure_id=s,shard_count=n,shard_index=k,scan_space=80,
                    range_start=80*k//n,range_count=80*(k+1)//n-80*k//n,
                    scanned_selections=80*(k+1)//n-80*k//n,generated=generated,
                    processed=generated,accepted=0,rejected=generated,deferred=0,identity_audited=True)
                def record(p):return dict(path=str(p),rows=0,sha256=digest(p))
                atomic_json(job/'job.json',dict(gpu=gpu,identities={'binary':'test','w5':'test','palp_cws':'test'},
                    accepted=record(a),deferred=record(d)))
        return run
    def audit(self,run):
        return subprocess.run([sys.executable,str(SCRIPTS/'audit_cws_run.py'),'--run-root',str(run)],capture_output=True,text=True)
    def test_all_53_jobs_and_missing_shard(self):
        run=self.job_tree();r=self.audit(run)
        self.assertEqual(r.returncode,0,r.stderr)
        self.assertEqual(json.loads((run/'gpu-coverage.json').read_text())['job_count'],53)
        (run/'s03-shard-007-of-008/job.json').unlink()
        self.assertNotEqual(self.audit(run).returncode,0)
    def test_output_corruption_detected(self):
        run=self.job_tree();(run/'s02-shard-000-of-001/deferred.ovf').write_text('lost or extra row\n')
        self.assertNotEqual(self.audit(run).returncode,0)
    def test_wrong_shard_identity_detected(self):
        run=self.job_tree();p=run/'s03-shard-007-of-008/job.json'
        j=json.loads(p.read_text());j['gpu']['shard_index']=6;atomic_json(p,j)
        self.assertNotEqual(self.audit(run).returncode,0)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--driver',type=Path)
    a,rest=parser.parse_known_args();REAL_DRIVER=a.driver
    unittest.main(argv=[sys.argv[0]]+rest)

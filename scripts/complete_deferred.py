#!/usr/bin/env python3
"""Account for every explicitly supplied deferred record before committing results.

Uses the assertion-enabled, UB-trapping complete_deferred driver, not the legacy
cws -i filter (which has error paths that exit successfully without a receipt).
Original inputs are never removed. A failed chunk has no completion receipt.
This is execution accounting, not a formal proof of PALP's IP algorithm.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import hashlib
import json
import os
from pathlib import Path
import subprocess


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for data in iter(lambda:f.read(1024*1024),b''): h.update(data)
    return h.hexdigest()


def atomic_json(path,value):
    temporary=path.with_suffix('.json.tmp')
    with temporary.open('w') as f:
        json.dump(value,f,sort_keys=True,indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(temporary,path)


def complete_chunk(driver,binary_hash,work,index,records):
    stem=f'{index:08d}'
    source=work/'chunks'/f'{stem}.input'
    output=work/'chunks'/f'{stem}.accepted'
    receipt=work/'chunks'/f'{stem}.json'
    error=work/'chunks'/f'{stem}.stderr'
    block_hash=hashlib.sha256(b''.join(records)).hexdigest()
    if receipt.exists():
        old=json.loads(receipt.read_text())
        if old['input_sha256']!=block_hash or old['driver_sha256']!=binary_hash or old['processed']!=len(records):
            raise RuntimeError(f'chunk {stem} differs from its previous receipt; use a new work directory')
        if not output.is_file() or digest(output)!=old['output_sha256']:
            raise RuntimeError(f'chunk {stem} committed output is missing/corrupt')
        return old
    with source.open('wb') as f:
        f.writelines(records); f.flush(); os.fsync(f.fileno())
    temporary=output.with_suffix('.accepted.tmp')
    with source.open('rb') as src, temporary.open('wb') as dst, error.open('wb') as err:
        result=subprocess.run([str(driver)],stdin=src,stdout=dst,stderr=err)
        dst.flush(); os.fsync(dst.fileno())
    with error.open('rb') as f:
        f.seek(max(0,error.stat().st_size-65536))
        lines=f.read().splitlines()
    prefix=b'CWS_COMPLETION_V1 '
    if result.returncode or not lines or not lines[-1].startswith(prefix):
        raise RuntimeError(f'chunk {stem} has no successful CPU receipt (exit {result.returncode}); see {error}')
    stats=json.loads(lines[-1][len(prefix):])
    if any(type(stats.get(k)) is not int or stats[k]<0 for k in ('processed','accepted','rejected')):
        raise RuntimeError(f'chunk {stem}: invalid receipt fields')
    if stats['processed']!=len(records) or stats['accepted']+stats['rejected']!=len(records):
        raise RuntimeError(f'chunk {stem}: not every input has a CPU decision')
    remaining=Counter(records)
    written=0
    with temporary.open('rb') as f:
        for row in f:
            if remaining[row]<=0: raise RuntimeError(f'chunk {stem}: output is not an input submultiset')
            remaining[row]-=1; written+=1
    if written!=stats['accepted']: raise RuntimeError(f'chunk {stem}: accepted output count mismatch')
    os.replace(temporary,output)
    receipt_data=dict(stats,index=index,input_sha256=block_hash,output_sha256=digest(output),driver_sha256=binary_hash)
    atomic_json(receipt,receipt_data)
    source.unlink()  # Original deferred files remain; committed chunks are reproducible.
    return receipt_data


def run(args):
    driver=args.driver.resolve(strict=True)
    inputs=[p.resolve(strict=True) for p in args.inputs]
    if len(set(inputs))!=len(inputs): raise ValueError('duplicate deferred input paths')
    if args.workers<1 or args.chunk_rows<1 or args.expected_rows<0: raise ValueError('invalid counts')
    work=args.work_dir.resolve()
    work.mkdir(parents=True,exist_ok=True)
    (work/'chunks').mkdir(exist_ok=True)
    if (work/'completion.json').exists(): raise ValueError('work directory already has a completed run; retain it unchanged')
    binary_hash=digest(driver)
    config=dict(version=1,inputs=[str(p) for p in inputs],driver_sha256=binary_hash,
                expected_rows=args.expected_rows,chunk_rows=args.chunk_rows)
    config_file=work/'run.json'
    if config_file.exists() and json.loads(config_file.read_text())!=config:
        raise ValueError('resume configuration differs; use a new work directory')
    if not config_file.exists(): atomic_json(config_file,config)
    pending=set(); receipts=[]; input_records=[]; count=0; chunks=0
    def collect(done):
        for future in done: receipts.append(future.result())
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        block=[]
        def submit():
            nonlocal block,chunks,pending
            pending.add(pool.submit(complete_chunk,driver,binary_hash,work,chunks,block))
            chunks+=1; block=[]
            if len(pending)>=2*args.workers:
                done,pending=wait(pending,return_when=FIRST_COMPLETED); collect(done)
        for path in inputs:
            before=path.stat(); h=hashlib.sha256(); rows=0
            with path.open('rb') as f:
                for raw in f:
                    h.update(raw)
                    if not raw.strip(): raise ValueError(f'{path}: blank deferred record')
                    block.append(raw if raw.endswith(b'\n') else raw+b'\n')
                    rows+=1; count+=1
                    if len(block)==args.chunk_rows: submit()
            after=path.stat()
            if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
                raise ValueError(f'{path}: input changed during processing')
            input_records.append(dict(path=str(path),sha256=h.hexdigest(),rows=rows))
        if block: submit()
        collect(pending)
    if count!=args.expected_rows:
        raise ValueError(f'deferred input coverage mismatch: GPU expected {args.expected_rows}, files contain {count}; no final completion receipt written')
    if digest(driver)!=binary_hash: raise ValueError('CPU driver changed during processing')
    receipts.sort(key=lambda r:r['index'])
    if [r['index'] for r in receipts]!=list(range(chunks)): raise ValueError('missing/duplicate chunk receipts')
    processed=sum(r['processed'] for r in receipts)
    accepted=sum(r['accepted'] for r in receipts)
    rejected=sum(r['rejected'] for r in receipts)
    if processed!=count or accepted+rejected!=count: raise ValueError('aggregate CPU accounting mismatch')
    destination=work/'accepted-cpu.txt'; temporary=work/'accepted-cpu.txt.tmp'
    output_hash=hashlib.sha256(); written=0
    with temporary.open('wb') as out:
        for r in receipts:
            source=work/'chunks'/f'{r["index"]:08d}.accepted'
            if digest(source)!=r['output_sha256']: raise ValueError('committed CPU chunk changed')
            with source.open('rb') as f:
                for row in f: out.write(row); output_hash.update(row); written+=1
        out.flush(); os.fsync(out.fileno())
    if written!=accepted: raise ValueError('merged CPU accepted count mismatch')
    os.replace(temporary,destination)
    complete=dict(config,inputs=input_records,processed=processed,accepted=accepted,rejected=rejected,
                  chunks=chunks,accepted_output=str(destination),output_sha256=output_hash.hexdigest(),
                  status='all supplied deferred records processed; PALP algorithm remains a reference trust boundary')
    atomic_json(work/'completion.json',complete)
    print(json.dumps(complete,sort_keys=True))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--driver',type=Path,required=True)
    p.add_argument('--work-dir',type=Path,required=True)
    p.add_argument('--expected-rows',type=int,required=True,help='sum of GPU deferred counts, not a count inferred from these input files')
    p.add_argument('--workers',type=int,default=1)
    p.add_argument('--chunk-rows',type=int,default=20000)
    p.add_argument('inputs',type=Path,nargs='+')
    try: run(p.parse_args())
    except Exception as e: raise SystemExit(f'CPU completion INCOMPLETE: {e}')


if __name__=='__main__': main()

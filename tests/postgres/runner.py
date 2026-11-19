"""Invocation-local builds and bounded, isolated foreground workers (STO-7/54)."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import tarfile
import threading
import time
import tomllib

EXAMPLES=[item['name'] for item in tomllib.loads(Path('Cargo.toml').read_text())['example']]

PHASES=['w1','checks','commits','regressions','corrections','core','workflow',
        'default-workflow','w5','w6','w7','controls']
DEFAULT_JOBS=6


@contextmanager
def port_pool(root,base):
    """Keep idle allocations out of all workers' outgoing ephemeral-port use."""
    from harness import check_port_range
    check_port_range()
    blocks=list(range(base,26000,10));reserved={};allocated=set();cursor=0
    def reserve(candidate):
        sockets=[]
        try:
            for port in range(candidate,candidate+10):
                sock=socket.socket();sockets.append(sock)
                sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                sock.bind(('127.0.0.1',port))
            return sockets
        except OSError:
            for sock in sockets:sock.close()
            return []
    for candidate in blocks:reserved[candidate]=reserve(candidate)
    server=socket.socket(socket.AF_UNIX);path=root/'ports.sock'
    server.bind(str(path));server.listen();server.settimeout(.2)
    stop=threading.Event()
    def serve():
        nonlocal cursor
        while not stop.is_set():
            try:client,_=server.accept()
            except socket.timeout:continue
            with client:
                request=json.loads(client.makefile('rb').readline())
                selected=None
                if request['action']=='allocate':
                    for step in range(len(blocks)):
                        candidate=blocks[(cursor+step)%len(blocks)]
                        if candidate in allocated:continue
                        if not reserved[candidate]:reserved[candidate]=reserve(candidate)
                        if len(reserved[candidate])!=10:continue
                        for sock in reserved[candidate]:sock.close()
                        reserved[candidate]=[];allocated.add(candidate)
                        selected=candidate;cursor=(cursor+step+1)%len(blocks);break
                    response=dict(base=selected)
                else:
                    selected=request['base']
                    assert selected in allocated
                    allocated.remove(selected);reserved[selected]=reserve(selected)
                    response=dict(released=selected)
                with (root/'ports.jsonl').open('a') as log:
                    log.write(json.dumps(dict(action=request['action'],base=selected,timestamp=time.time()))+'\n')
                client.sendall((json.dumps(response)+'\n').encode())
    thread=threading.Thread(target=serve);thread.start()
    try:yield
    finally:
        stop.set();thread.join();server.close();path.unlink()
        for sockets in reserved.values():
            for sock in sockets:sock.close()


def port_request(root,action,**data):
    with socket.socket(socket.AF_UNIX) as client:
        client.connect(str(root/'ports.sock'))
        client.sendall((json.dumps(dict(action=action,**data))+'\n').encode())
        return json.loads(client.makefile('rb').readline())


@contextmanager
def invocation(root,jobs,candidates=False):
    from harness import check_port_range
    check_port_range()
    if os.environ.get('PROVISIOND_HARNESS_INVOCATION'):
        yield
        return
    base=int(os.environ['PROVISIOND_HARNESS_PORT_BASE'])
    assert jobs>=1 and base%10==0 and 25500<=base and base+10*jobs<=26000,'worker slots exceed the allowed port range'
    root.mkdir(parents=True,exist_ok=True)
    owned=Path(tempfile.mkdtemp(prefix='invocation-',dir=root))
    previous=dict(os.environ)
    os.environ.update(PROVISIOND_HARNESS_INVOCATION=str(owned),PROVISIOND_HARNESS_JOBS=str(jobs),
                      PROVISIOND_HARNESS_SLOT_BASE=os.environ['PROVISIOND_HARNESS_PORT_BASE'])
    records=[]
    try:
        # Compile sequentially, sharing dependency artifacts; freeze each feature
        # variant before building the next. Workers never write this build tree.
        for variant,features in [('hooks',['--features','store-test-hooks']),('default',[])]:
            command=['cargo','build','--bins',*(['--examples'] if features else []),*features,'--locked','--jobs','2']
            started=time.monotonic()
            with (owned/(variant+'.stdout')).open('w') as out,(owned/(variant+'.stderr')).open('w') as err:
                child=subprocess.run(command,env=dict(os.environ,CARGO_TARGET_DIR=str(owned/'target')),stdout=out,stderr=err)
            records.append(dict(command=command,exit_status=child.returncode,wall_seconds=time.monotonic()-started,variant=variant))
            (owned/'builds.json').write_text(json.dumps(records,indent=2)+'\n')
            if child.returncode:
                raise RuntimeError(f'invocation build failed: {owned}/{variant}.stderr')
            names=['api','engine','migrate','recovery']
            if features:names += ['examples/'+name for name in EXAMPLES]
            for name in names:
                source=owned/'target/debug'/name
                dest=owned/variant/name
                dest.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(source,dest)
        if candidates:
            from candidate_reds import BASE
            export=owned/'candidate-source';export.mkdir()
            paths=subprocess.check_output(['git','ls-tree','-r','--name-only',BASE],text=True).splitlines()
            paths=[p for p in paths if p.startswith(('src/','migrations/')) or p in ('Cargo.toml','Cargo.lock') or p.startswith('tests/postgres/') and p.endswith('.rs')]
            archive=subprocess.check_output(['git','archive',BASE,*paths])
            with tarfile.open(fileobj=io.BytesIO(archive)) as tree:tree.extractall(export,filter='data')
            protected={p:hashlib.sha256((export/p).read_bytes()).hexdigest() for p in paths}
            command=['cargo','build','--manifest-path',str(export/'Cargo.toml'),'--bins','--examples','--features','store-test-hooks','--locked','--jobs','2']
            started=time.monotonic()
            with (owned/'candidate.stdout').open('w') as out,(owned/'candidate.stderr').open('w') as err:
                child=subprocess.run(command,env=dict(os.environ,CARGO_TARGET_DIR=str(owned/'candidate-target')),stdout=out,stderr=err)
            records.append(dict(command=command,exit_status=child.returncode,wall_seconds=time.monotonic()-started,variant='candidate',baseline=BASE))
            (owned/'builds.json').write_text(json.dumps(records,indent=2)+'\n')
            after={p:hashlib.sha256((export/p).read_bytes()).hexdigest() for p in paths}
            assert protected==after,'candidate baseline source changed during build'
            (owned/'candidate-source-proof.json').write_text(json.dumps(dict(baseline=BASE,before=protected,after=after),indent=2)+'\n')
            if child.returncode:raise RuntimeError(f'candidate build failed: {owned}/candidate.stderr')
            for name in ['api','engine','migrate','recovery',*['examples/'+name for name in EXAMPLES]]:
                dest=owned/'candidate'/name;dest.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(owned/'candidate-target/debug'/name,dest)
            assert (owned/'candidate/api').read_bytes()!=(owned/'hooks/api').read_bytes(),'candidate build reused current runtime'
            shutil.rmtree(export)
        with port_pool(owned,base):yield
    finally:
        # Keep raw build logs and identities; discard only this invocation's binaries.
        for name in ('target','hooks','default','candidate','candidate-source','candidate-target'):
            shutil.rmtree(owned/name,ignore_errors=True)
        os.environ.clear();os.environ.update(previous)


@contextmanager
def worker_slot(exclusive=False):
    """A single bound across phases, W7 children and expected-negative controls."""
    root=Path(os.environ['PROVISIOND_HARNESS_INVOCATION'])
    jobs=int(os.environ['PROVISIOND_HARNESS_JOBS'])
    base=int(os.environ['PROVISIOND_HARNESS_SLOT_BASE'])
    assert 25500<=base and base+10*jobs<=26000 and base%10==0
    waiting=time.monotonic()
    while True:
        for index in range(1 if exclusive else jobs):
            locks=[]
            for token in (range(jobs) if exclusive else [index]):
                lock=(root/f'slot-{token}.lock').open('w')
                try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:
                    lock.close()
                    for held in locks:held.close()
                    locks=[];break
                locks.append(lock)
            if not locks:continue
            selected=port_request(root,'allocate')['base']
            if selected is None:
                for held in locks:held.close()
                raise RuntimeError('no free private ten-port allocation')
            old=os.environ['PROVISIOND_HARNESS_PORT_BASE']
            os.environ['PROVISIOND_HARNESS_PORT_BASE']=str(selected)
            record=dict(pid=os.getpid(),port_base=selected,tokens=len(locks),exclusive=exclusive)
            def event(kind,**data):
                with (root/'workers.jsonl').open('a') as stream:
                    stream.write(json.dumps(dict(record,event=kind,timestamp=time.time(),**data))+'\n')
            event('worker_started',wait_seconds=time.monotonic()-waiting)
            @contextmanager
            def alone():
                # W6 retains one physical history across its cases. Keep that
                # history's token and port lease while its timed case drains
                # competing workers. Only this serial phase upgrades a slot.
                extra=[]
                try:
                    if not exclusive:
                        while True:
                            for token in range(jobs):
                                if token==index:continue
                                lock=(root/f'slot-{token}.lock').open('w')
                                try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                                except BlockingIOError:
                                    lock.close()
                                    for held in extra:held.close()
                                    extra=[];break
                                extra.append(lock)
                            else:break
                            time.sleep(.05)
                    event('worker_exclusive_started',tokens=jobs,exclusive=True)
                    try:yield
                    finally:event('worker_exclusive_finished',tokens=jobs,exclusive=True)
                finally:
                    for held in extra:held.close()
            try:yield alone
            finally:
                event('worker_finished')
                port_request(root,'release',base=selected)
                os.environ['PROVISIOND_HARNESS_PORT_BASE']=old
                for held in locks:held.close()
            return
        time.sleep(.05)


def reuse_build(h,command):
    if command[:2]!=['cargo','build']:return None
    owned=Path(os.environ['PROVISIOND_HARNESS_INVOCATION'])
    variant=os.environ.get('PROVISIOND_HARNESS_BUILD_VARIANT') or ('hooks' if '--features' in command else 'default')
    target=Path(command[command.index('--target-dir')+1]) if '--target-dir' in command else h.target
    names=[]
    if '--bins' in command:names+=['api','engine','migrate','recovery']
    if '--bin' in command:names.append(command[command.index('--bin')+1])
    if '--examples' in command:names+=['examples/'+name for name in EXAMPLES]
    if '--example' in command:names.append('examples/'+command[command.index('--example')+1])
    assert names,command
    identities={}
    for name in names:
        source=owned/variant/name;dest=target/'debug'/name
        dest.parent.mkdir(parents=True,exist_ok=True)
        if dest.exists():dest.unlink()
        os.link(source,dest)
        identities[name]=hashlib.sha256(dest.read_bytes()).hexdigest()
    h.event('invocation_build_reused',requested_command=command,variant=variant,
            build_record=str(owned/'builds.json'),executables=identities)
    return subprocess.CompletedProcess(['invocation-build-reuse',variant,*names],0,'','')


def children(jobs):
    """Return in inventory order, retaining every true exit and private stream."""
    def run(job):
        command,path,env=job
        started=time.monotonic()
        with path.with_suffix('.stdout').open('w') as out,path.with_suffix('.stderr').open('w') as err:
            result=subprocess.run(command,env=env,stdout=out,stderr=err,check=False)
        return dict(command=command,exit_status=result.returncode,
                    wall_seconds=time.monotonic()-started,path=str(path))
    with ThreadPoolExecutor(max_workers=int(os.environ['PROVISIOND_HARNESS_JOBS'])) as pool:
        return list(pool.map(run,jobs))

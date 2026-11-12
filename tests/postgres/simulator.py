"""Independent loopback test provider/rail process. Durable effects live outside PG."""
import argparse
import calendar
import json
import os
from pathlib import Path
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--port',type=int,required=True)
    ap.add_argument('--root',type=Path,required=True)
    ap.add_argument('--kind',choices=['provider','rail'],required=True)
    ap.add_argument('--concurrent-order-barrier',action='store_true',
                    help='test-only: overlap two acceptances and allocations')
    ap.add_argument('--search-read-barrier',action='store_true',
                    help='test-only: pause an active search iterator for a concurrent order')
    args=ap.parse_args()
    args.root.mkdir(exist_ok=True)
    events=args.root/'effects.jsonl'
    lock=threading.RLock()
    state=dict(mode='normal',offers={},settlements=[],resources={},orders={},keys={},cancelled=[],evidence={})
    sequence=0
    if events.exists():
        for line in events.read_text().splitlines():
            e=json.loads(line);sequence=e['sequence']
            if e['stage']=='resource_created':state['resources'][e['resource']]=e['value']
            if e['stage']=='resource_created' and not e.get('test_fixture'):state['orders'][e['resource']]=e['value']
            if e['stage']=='key_registered':state['keys'][e['command']]={'name':e['command'],'owner':e['command'],'account':e['account']}
            if e['stage']=='key_deleted':state['keys'].pop(e['command'],None)
            if e['stage']=='resource_deleted':state['resources'].pop(e['resource'],None)
            if e['stage']=='rail_observation':state['settlements'].append(e['value'])
            if e['stage']=='invoice_cancelled':state['cancelled'].append(e['destination'])
    def record(stage,**value):
        nonlocal sequence
        with lock:
            sequence+=1
            e=dict(sequence=sequence,timestamp=time.time(),monotonic_ns=time.monotonic_ns(),stage=stage,**value)
            with events.open('a') as f:
                f.write(json.dumps(e,sort_keys=True)+'\n');f.flush();os.fsync(f.fileno())
            return e
    # W4 independent-effect evidence for OPS-27/PRV-26: force both orders
    # through acceptance before allocation, then both allocations before creation.
    # No lock is held while waiting; ordinary simulator invocations have no barrier.
    accepted_pair=threading.Barrier(2,timeout=5,action=lambda:record('acceptance_overlap',parties=2))
    allocated_pair=threading.Barrier(2,timeout=5,action=lambda:record('allocation_overlap',parties=2))
    def search_values(resources,identity):
        # PRV-26/OPS-27 tooling regression: pause after the iterator has yielded
        # its first value. The writer can finish while the detached read waits.
        for resource in resources:
            hold=args.root/'hold-search-iteration'
            if args.search_read_barrier and hold.exists():
                record('search_iterator_held',resource=resource['external_id'],**identity)
                while hold.exists():time.sleep(.002)
                record('search_iterator_resumed',resource=resource['external_id'],**identity)
            yield resource
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*_):pass
        def do_GET(self):self.dispatch()
        def do_POST(self):self.dispatch()
        def reply(self,value):
            data=json.dumps(value).encode()
            self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(data)));self.send_header('Connection','close');self.end_headers()
            try:self.wfile.write(data);self.wfile.flush();return True
            except (BrokenPipeError,ConnectionResetError):return False
        def dispatch(self):
            body=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))) or b'{}')
            if self.path=='/control':
                with lock:
                    if body.get('clear'):state['settlements']=[]
                    if 'evidence' in body:
                        proof=body['evidence'];state['evidence'][proof['operation']]=proof
                        record('evidence_control_activated',value=proof)
                    if 'remove_resource' in body:
                        external=body['remove_resource'];state['resources'].pop(external,None)
                        record('resource_deleted',resource=external,test_fixture=True)
                    if 'fixture_resource' in body:
                        resource=body['fixture_resource'];state['resources'][resource['external_id']]=resource
                        record('resource_created',resource=resource['external_id'],value=resource,test_fixture=True)
                    if 'offers' in body:state['offers']=body['offers']
                    if 'mode' in body:state['mode']=body['mode']
                    if 'deadline_delay' in body:state['deadline_delay']=body['deadline_delay']
                    if 'verification_resource' in body:state['verification_resource']=body['verification_resource']
                    if 'settlement' in body:
                        value=body['settlement']
                        if value.get('rail')=='lightning' and value.get('settled'):
                            expiry=int(value['destination'].rsplit('/',1)[1])
                            settled=calendar.timegm(time.strptime(value['settled_at'],'%Y-%m-%dT%H:%M:%SZ'))
                            if value['destination'] in state['cancelled'] or settled>expiry:
                                self.reply({'refused':'simulator invoice is not payable'});return
                        state['settlements'].append(value);record('rail_observation',value=value)
                self.reply({'ok':True});return
            if self.path=='/inspect':
                with lock:
                    snapshot={'events':list(map(json.loads,events.read_text().splitlines())) if events.exists() else [],'resources':dict(state['resources']),'keys':dict(state['keys'])}
                self.reply(snapshot);return
            if self.path=='/cancel':
                invoice=body['invoice']
                settled=any(v.get('destination')==invoice and v.get('rail')=='lightning' and v.get('settled') and v.get('preimage_known') for v in state['settlements'])
                if not settled:
                    if invoice not in state['cancelled']:state['cancelled'].append(invoice);record('invoice_cancelled',destination=invoice)
                self.reply({'cancelled':not settled});return
            if self.path=='/settlements':
                # Test-only malformed trusted-adapter response exercises the
                # watcher's actual error return, including in default binaries.
                if state['mode']=='invalid_settlements':
                    record('invalid_settlements_returned');self.reply({});return
                self.reply(state['settlements']);return
            operation=body.get('operation')
            identity=dict(command=operation,account=body.get('account'),correlator=body.get('correlator'),path=self.path,resolution=body.get('resolution',False))
            record('request_received',**identity)
            # PRV-13b/OVR-19 test tooling only: one attributed request, after
            # readiness. Observe peer EOF while withholding the response, so the
            # measured reply wait excludes database and process-startup work.
            with lock:
                delay=state.get('deadline_delay')
                if delay and delay['operation']==operation and delay['path']==self.path:
                    state['deadline_delay']=None
                else:delay=None
            if delay:
                started=time.monotonic()
                record('deadline_delay_started',seconds=delay['seconds'],**identity)
                closed=None
                while time.monotonic()-started<delay['seconds']:
                    if closed is None and select.select([self.connection],[],[],.002)[0]:
                        if self.connection.recv(1,socket.MSG_PEEK)==b'':
                            closed=time.monotonic()-started
                            record('deadline_peer_closed',wait_seconds=closed,**identity)
                    else:time.sleep(.002)
                record('deadline_delay_finished',wait_seconds=closed if closed is not None else time.monotonic()-started,
                       peer_closed=closed is not None,measurement='peer EOF' if closed is not None else 'lower bound before reply',**identity)
                if closed is not None:return
            hold=args.root/('hold-'+self.path.strip('/'))
            held=hold.exists()
            if held:record('request_held',**identity)
            while hold.exists():time.sleep(.002)
            if held:record('request_released',**identity)
            if body.get('resolution'):
                barrier=args.root/'hold-reconciliation'
                if barrier.exists():record('resolution_read_held',**identity)
                while barrier.exists():time.sleep(.002)
            proof=state['evidence'].get(operation,{})
            if self.path=='/enumerate':
                with lock:rows=[r for r in state['resources'].values() if r['account']==body['account']]
                offset=int(body.get('cursor') or 0);page=rows[offset:offset+2]
                cursor=str(offset+2) if offset+2<len(rows) else None
                if proof.get('enumeration')=='rate_limited':
                    record('enumeration_refused',**identity)
                    self.reply({'error':{'kind':'rate_limited','message':'controlled enumeration throttle','retryable':True,'details':{'accepted':None,'retry_after_ms':1000}}});return
                if proof.get('enumeration')=='incomplete':
                    self.reply({'resources':page,'complete':False,'next':None});return
                self.reply({'resources':page,'complete':cursor is None,'next':cursor});return
            if self.path=='/keys/list':
                self.reply([dict(v,operation=k) for k,v in state['keys'].items() if v['account']==body['account']]);return
            mode=state['mode']
            if self.path=='/offers':record('read_only',**identity);self.reply(state['offers'].get(body['account'],[]));return
            if self.path=='/keys':
                if mode=='duplicate_key':
                    self.reply({'error':{'kind':'conflict','message':'duplicate simulator key','retryable':False,'details':{'reason':'state','accepted':False}}});return
                state['keys'][operation]={'name':operation,'owner':operation,'account':body['account']};record('key_registered',**identity)
                if mode=='drop_key_reply':
                    self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                self.reply({'id':operation,'owned':True});return
            if self.path=='/keys/delete':
                state['keys'].pop(operation,None);record('key_deleted',**identity);self.reply({'ok':True});return
            if self.path=='/order':
                # PRV-5/OPS-11: errors from the mutating request itself, with
                # durable receipt/response facts and no invented acceptance.
                if mode in ('provider_400','provider_500','provider_no_status'):
                    details={'accepted':False if mode=='provider_400' else None}
                    if mode!='provider_no_status':details['status']=400 if mode=='provider_400' else 500
                    error={'kind':'provider','message':'controlled provider order refusal','retryable':True,'details':details}
                    record('order_error_selected',error=error,**identity)
                    if self.reply({'error':error}):record('response_returned',error=error,**identity)
                    return
                if mode=='secret_error':
                    self.reply({'error':{'kind':'authentication','message':'TEST_ONLY_SECRET_MARKER','retryable':False,'details':{'accepted':False,'api_key':'TEST_ONLY_SECRET_MARKER','nested':[{'password':'TEST_ONLY_SECRET_MARKER'}]}}});return
                if mode=='reject_order':
                    self.reply({'error':{'kind':'authentication','message':'controlled order rejection','retryable':False,'details':{'accepted':False}}});return
                accepted=record('accepted',**identity)
                # OPS-47/50: independent remote effect can outlive its caller.
                effect=args.root/'hold-before-effect'
                if effect.exists():record('effect_held',**identity)
                while effect.exists():time.sleep(.002)
                if args.concurrent_order_barrier:accepted_pair.wait()
                # PRV-26/OPS-27 test evidence: the fsync'd acceptance owns this
                # identity. A later global sequence can belong to another order.
                external='SIMULATOR-RESOURCE-'+str(accepted['sequence'])
                if args.concurrent_order_barrier:allocated_pair.wait()
                resource={'external_id':external,'account':body['account'],'correlator':body['correlator'],'channel':body['channel'],'name':body['request']['hostname'],'region':body['request'].get('region'),'state':'running','public_ips':[],'present':True}
                with lock:
                    state['resources'][external]=resource
                    state['orders'][external]=resource
                    record('resource_created',resource=external,value=resource,**identity)
                after=args.root/'hold-after-create'
                while after.exists():time.sleep(.002)
                if mode=='drop_reply':
                    record('reply_dropped',resource=external,**identity);self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                if self.reply({'accepted':True,'external_id':external}):record('response_returned',resource=external,**identity)
                return
            if self.path=='/search':
                record('read_only',**identity)
                if body.get('resolution') and proof:
                    search=proof.get('search','normal')
                    with lock:found=[r for r in state['orders'].values() if r['account']==body['account'] and r['channel']==body['channel'] and r['correlator']==body['correlator']]
                    if search=='empty':found=[]
                    if search=='wrong':found=[dict(r,correlator='unrelated') for r in found]
                    if search=='many':found=found+[dict(r,external_id=r['external_id']+'-duplicate') for r in found]
                    if search=='in_progress':found=[]
                    if 'resources' in proof:found=proof['resources']
                    if 'channels' in proof:found=proof['channels'].get(body['channel'],[])
                    authoritative=search!='incomplete' and body['channel']!=proof.get('incomplete_channel')
                    record('resolution_evidence_returned',search=search,authoritative=authoritative,resources=found,**identity)
                    self.reply({'authoritative':authoritative,'resources':found,'in_progress':search=='in_progress'});return
                if mode=='search_provider_400':
                    error={'kind':'provider','message':'controlled verification refusal','retryable':True,'details':{'accepted':False,'status':400}}
                    record('verification_fault_returned',mode=mode,error=error,**identity)
                    self.reply({'error':error});return
                if mode in ('search_throttle','search_network'):
                    record('verification_fault_returned',mode=mode,**identity)
                    if mode=='search_network':
                        self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                    self.reply({'error':{'kind':'rate_limited','message':'controlled verification throttle','retryable':True,'details':{'accepted':False,'retry_after_ms':1000}}});return
                # PRV-26/OPS-27: concurrent orders must not invalidate a search
                # iterator. Resources are replaced, not mutated, after insertion.
                with lock:resources=tuple(state['resources'].values())
                found=[r for r in search_values(resources,identity) if r['account']==body['account'] and r['channel']==body['channel'] and r['correlator']==body['correlator']]
                if mode=='search_wrong_exact':
                    found=[dict(r,correlator='unrelated') for r in found]
                    record('non_exact_candidate_returned',resources=found,**identity)
                if mode in ('search_non_authoritative','search_many','get_error','get_absent','search_malformed'):
                    other=state['resources'][state['verification_resource']]
                    if mode=='search_malformed':found=found[:1]
                    elif mode not in ('get_error','get_absent'):found.append(other)
                    record('verification_identifiers_returned',source='search',identifiers=[r['external_id'] for r in found],**identity)
                    if mode=='search_malformed':found.append(dict(account=body['account'],channel=body['channel'],correlator=body['correlator']))
                    if mode.startswith('search_'):record('verification_fault_returned',mode=mode,**identity)
                if mode=='normal' and (args.root/'hold-get').exists():
                    # OPS-21/13: publish the selected IDs before this reply can
                    # license get. The subsequent held get proves receipt; this
                    # event alone does not claim successful response delivery.
                    record('verification_identifiers_selected',source='search',identifiers=[r['external_id'] for r in found],**identity)
                returned=self.reply({'authoritative':mode!='search_non_authoritative','resources':found})
                if held:record('verification_read_finished',reply_returned=returned,**identity)
                return
            if self.path in ['/get','/constraints']:
                record('read_only',**identity)
                resource=state['resources'].get(body['external_id'])
                if mode=='delete_gone_stale' and self.path=='/get':resource=state.get('stale_deleted',{}).get(body['external_id'],resource)
                if mode=='search_wrong_exact' and self.path=='/get':resource=dict(resource,correlator='unrelated')
                if body.get('resolution') and self.path=='/get' and proof.get('search')=='wrong':
                    resource=dict(resource,correlator='unrelated')
                if body.get('resolution') and self.path=='/get' and proof.get('direct'):
                    record('resolution_direct_activated',direct=proof['direct'],**identity)
                    if proof['direct']=='failed':
                        self.reply({'error':{'kind':'network','message':'controlled read failure','retryable':True,'details':{'accepted':None}}});return
                    if proof['direct']=='absent':self.reply(None);return
                    if proof['direct']=='wrong_account':self.reply(dict(resource,account='unrelated'));return

                if self.path=='/get' and mode in ('get_error','get_absent'):
                    record('verification_fault_returned',mode=mode,**identity)
                    if mode=='get_error':
                        self.reply({'error':{'kind':'not_found','message':'controlled read failure','retryable':False,'details':{'accepted':False,'status':404}}});return
                    record('verification_identifiers_returned',source='get',identifiers=[resource['external_id']],**identity)
                    self.reply(dict(resource,present=False));return
                returned=self.reply(resource if self.path=='/get' else {'earliest_cancellation_date':time.strftime('%Y-%m-%dT00:00:00Z',time.gmtime())})
                if returned:record('read_response_returned',**identity)
                if self.path=='/get' and held:record('verification_read_finished',reply_returned=returned,**identity)
                return
            if self.path=='/delete':
                decision=proof.get('delete',mode)
                if decision=='reject':
                    record('delete_rejected',**identity)
                    self.reply({'error':{'kind':'authentication','message':'controlled delete rejection','retryable':False,'details':{'accepted':False}}});return
                if decision=='unknown':
                    record('delete_reply_dropped',accepted=None,**identity);self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                accepted=record('delete_accepted',**identity)
                if decision=='acceptance_not_applied':
                    if self.reply({'accepted':True}):record('delete_response_returned',gone=False,**identity)
                    return
                if mode=='delete_gone_stale':state.setdefault('stale_deleted',{})[body['external_id']]=state['resources'].get(body['external_id'])
                state['resources'].pop(body['external_id'],None);record('resource_deleted',resource=body['external_id'],**identity)
                if decision=='drop_reply':
                    record('delete_reply_dropped',accepted=True,**identity);self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                reply={'accepted':True} if decision=='acceptance_applied' else {'gone':True}
                if self.reply(reply):record('delete_response_returned',gone=reply.get('gone',False),**identity)
                return
            self.reply({'error':{'kind':'unsupported','message':'test route unsupported','retryable':False,'details':{'accepted':False}}})
    server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    print(json.dumps({'event':'simulator_listening','port':args.port,'kind':args.kind}),flush=True)
    server.serve_forever()

if __name__=='__main__':main()

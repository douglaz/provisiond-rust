"""Independent loopback test provider/rail process. Durable effects live outside PG."""
import argparse
import json
import os
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--port',type=int,required=True)
    ap.add_argument('--root',type=Path,required=True)
    ap.add_argument('--kind',choices=['provider','rail'],required=True)
    args=ap.parse_args()
    args.root.mkdir(exist_ok=True)
    events=args.root/'effects.jsonl'
    lock=threading.RLock()
    state=dict(mode='normal',offers={},settlements=[],resources={},keys={})
    sequence=0
    if events.exists():
        for line in events.read_text().splitlines():
            e=json.loads(line);sequence=e['sequence']
            if e['stage']=='resource_created':state['resources'][e['resource']]=e['value']
            if e['stage']=='resource_deleted':state['resources'].pop(e['resource'],None)
            if e['stage']=='rail_observation':state['settlements'].append(e['value'])
    def record(stage,**value):
        nonlocal sequence
        with lock:
            sequence+=1
            e=dict(sequence=sequence,stage=stage,**value)
            with events.open('a') as f:
                f.write(json.dumps(e,sort_keys=True)+'\n');f.flush();os.fsync(f.fileno())
            return e
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
                    if 'offers' in body:state['offers']=body['offers']
                    if 'mode' in body:state['mode']=body['mode']
                    if 'settlement' in body:
                        state['settlements'].append(body['settlement']);record('rail_observation',value=body['settlement'])
                self.reply({'ok':True});return
            if self.path=='/inspect':self.reply({'events':list(map(json.loads,events.read_text().splitlines())) if events.exists() else [],'resources':state['resources'],'keys':state['keys']});return
            if self.path=='/settlements':self.reply(state['settlements']);return
            operation=body.get('operation')
            identity=dict(command=operation,account=body.get('account'),correlator=body.get('correlator'),path=self.path)
            record('request_received',**identity)
            hold=args.root/('hold-'+self.path.strip('/'))
            while hold.exists():time.sleep(.002)
            mode=state['mode']
            if self.path=='/offers':record('read_only',**identity);self.reply(state['offers'].get(body['account'],[]));return
            if self.path=='/keys':
                if mode=='duplicate_key':
                    self.reply({'error':{'kind':'conflict','message':'duplicate simulator key','retryable':False,'details':{'reason':'state','accepted':False}}});return
                state['keys'][operation]={'name':operation,'owner':operation};record('key_registered',**identity)
                if mode=='drop_key_reply':
                    self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                self.reply({'id':operation,'owned':True});return
            if self.path=='/keys/delete':
                state['keys'].pop(operation,None);record('key_deleted',**identity);self.reply({'ok':True});return
            if self.path=='/order':
                if mode=='reject_order':
                    self.reply({'error':{'kind':'authentication','message':'controlled order rejection','retryable':False,'details':{'accepted':False}}});return
                record('accepted',**identity)
                external='SIMULATOR-RESOURCE-'+str(sequence)
                resource={'external_id':external,'account':body['account'],'correlator':body['correlator'],'channel':body['channel'],'name':body['request']['hostname'],'region':body['request'].get('region'),'state':'running','public_ips':[],'present':True}
                with lock:
                    state['resources'][external]=resource
                    record('resource_created',resource=external,value=resource,**identity)
                after=args.root/'hold-after-create'
                while after.exists():time.sleep(.002)
                if mode=='drop_reply':
                    record('reply_dropped',resource=external,**identity);self.close_connection=True;self.connection.shutdown(2);self.connection.close();return
                if self.reply({'accepted':True,'external_id':external}):record('response_returned',resource=external,**identity)
                return
            if self.path=='/search':
                record('read_only',**identity)
                if mode=='search_throttle':
                    self.reply({'error':{'kind':'rate_limited','message':'controlled verification throttle','retryable':True,'details':{'accepted':False,'retry_after_ms':1000}}});return
                found=[r for r in state['resources'].values() if r['account']==body['account'] and r['channel']==body['channel'] and r['correlator']==body['correlator']]
                self.reply({'authoritative':True,'resources':found});return
            if self.path in ['/get','/constraints']:
                record('read_only',**identity)
                resource=state['resources'].get(body['external_id'])
                self.reply(resource if self.path=='/get' else {'earliest_cancellation_date':time.strftime('%Y-%m-%dT00:00:00Z',time.gmtime())});return
            if self.path=='/delete':
                state['resources'].pop(body['external_id'],None);record('resource_deleted',resource=body['external_id'],**identity);self.reply({'gone':True});return
            self.reply({'error':{'kind':'unsupported','message':'test route unsupported','retryable':False,'details':{'accepted':False}}})
    server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
    print(json.dumps({'event':'simulator_listening','port':args.port,'kind':args.kind}),flush=True)
    server.serve_forever()

if __name__=='__main__':main()

"""Plaintext loopback PostgreSQL protocol observations and deterministic faults.

Test transport only: never used by the store. Authentication payloads are forwarded
without recording them. A requested fault is evidence only after activated.wait().
"""
import socket
import struct
import threading


def receive(sock, count):
    data = b''
    while len(data) < count:
        part = sock.recv(count-len(data))
        if not part:
            raise EOFError()
        data += part
    return data


class Gate:
    def __init__(self, pattern, direction):
        self.pattern, self.direction = pattern, direction
        self.activated = threading.Event()
        self.released = threading.Event()
        self.connection = None
        self.on_release = None

    def wait(self):
        assert self.activated.wait(10), f'fault never activated: {self.pattern}'
        return self.connection

    def release(self):
        if not self.released.is_set() and self.on_release:
            self.on_release()
        self.released.set()


class Proxy:
    def __init__(self, harness):
        port = harness.ports[3]
        from harness import free_port
        harness.release_port(port)
        free_port(port)
        self.h = harness
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
        self.listener.bind(('127.0.0.1',port))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.closed = threading.Event()
        self.connections, self.threads, self.gates = [], [], []
        self.lock = threading.Lock()
        self.cancel_requests = 0
        self.nodelay = True
        self.refuse_after = None
        # Focused worker preflight regression: kill only matching read transports.
        self.fail_query = None
        self.observer_identity_mismatch = False
        self.spawn(self.accept)

    def spawn(self, fn, *args):
        t = threading.Thread(target=fn,args=args)
        self.threads.append(t)
        t.start()

    def arm(self, pattern, direction='reply', session=None):
        gate = Gate(pattern,direction)
        gate.session_filter=session
        gate.on_release=lambda:self.h.event('fault_released',fault='transport_gate',pattern=pattern,direction=direction,activated=gate.activated.is_set())
        self.gates.append(gate)
        self.h.event('fault_requested', fault='transport_gate', pattern=pattern,direction=direction)
        return gate

    def accept(self):
        while not self.closed.is_set():
            try:
                client,_ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            server = socket.create_connection(('127.0.0.1',self.h.ports[0]))
            for transport in (client, server):
                transport.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, int(self.nodelay))
            c = dict(client=client,server=server,pid=None,query='',gate=None,closed=False)
            self.connections.append(c)
            if self.refuse_after is not None and len(self.connections)>self.refuse_after:
                self.h.event('fault_activated',fault='connection_refused',connection=len(self.connections))
                self.close_connection(c,'refused_connection_fixture')
                continue
            self.spawn(self.front,c)

    def close_connection(self,c,source='harness'):
        c.setdefault('close_source',source)
        c['closed']=True
        for sock in [c['client'],c['server']]:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.h.event('transport_closed',session=c['pid'],source=source)

    def gate(self,c,direction,tag=None):
        for g in self.gates:
            if (not g.released.is_set() and (g.session_filter is None or g.session_filter==c['pid']) and g.direction==direction and g.pattern in c['query'] and
                (not g.activated.is_set() or g.connection is c) and
                (direction not in ('commit_reply','execute_reply') or tag==b'C')):
                if not g.activated.is_set():
                    g.connection=c
                    self.h.event('fault_activated',fault='transport_gate',direction=direction,
                                 query=c['query'],session=c['pid'])
                    g.activated.set()
                g.released.wait()
                return

    def front(self,c):
        try:
            length = receive(c['client'],4)
            body = receive(c['client'],struct.unpack('!I',length)[0]-4)
            if body[:4] == struct.pack('!I',80877102):
                with self.lock:
                    self.cancel_requests += 1
                self.h.event('cancel_request_observed')
            c['server'].sendall(length+body)
            self.spawn(self.back,c)
            while True:
                tag = receive(c['client'],1)
                length = receive(c['client'],4)
                body = receive(c['client'],struct.unpack('!I',length)[0]-4)
                if tag in (b'Q',b'P'):
                    c['query']=(body.rstrip(b'\0') if tag==b'Q' else body.split(b'\0',2)[1]).decode()
                    self.h.event('send_requested',query=c['query'],session=c['pid'],protocol=tag.decode())
                    self.gate(c,'send')
                    if self.fail_query and self.fail_query in c['query']:
                        self.h.event('fault_activated',fault='preflight_read_transport',query=c['query'],session=c['pid'])
                        self.close_connection(c,'preflight_read_transport')
                        return
                try:
                    c['server'].sendall(tag+length+body)
                except OSError:
                    # A withheld response also withholds upstream death. Keep
                    # reading the client so only its own EOF proves closure;
                    # e.g. dropping a prepared query may send Close after the
                    # server's independent idle-transaction timeout.
                    if not any(g.connection is c and g.activated.is_set() and
                               not g.released.is_set() and g.direction in ('reply','execute_reply')
                               for g in self.gates):
                        raise
                    self.h.event('gated_upstream_write_failed',session=c['pid'])
                    continue
                if tag in (b'Q',b'P'):
                    self.h.event('bytes_transmitted',query=c['query'],session=c['pid'],protocol=tag.decode())
        except EOFError:
            self.close_connection(c,'client_eof')
        except OSError:
            self.close_connection(c,'client_error')

    def back(self,c):
        try:
            while True:
                tag = receive(c['server'],1)
                length = receive(c['server'],4)
                body = receive(c['server'],struct.unpack('!I',length)[0]-4)
                if tag==b'K':
                    c['pid']=struct.unpack('!I',body[:4])[0]
                self.gate(c,'reply',tag)
                self.gate(c,'commit_reply',tag)
                self.gate(c,'execute_reply',tag)
                if tag==b'D' and self.observer_identity_mismatch and 'OPS-49 observer' in c['query']:
                    # OPS-49 exact identity sensitivity: alter only backend_start
                    # in an actual observer DataRow, preserving all protocol types.
                    count=struct.unpack('!H',body[:2])[0];offset=2;fields=[]
                    for index in range(count):
                        size=struct.unpack('!i',body[offset:offset+4])[0];offset+=4
                        value=body[offset:offset+size] if size>=0 else None
                        if size>=0:offset+=size
                        if index==1 and value is not None:value=b'identity-mismatch'
                        fields.append(struct.pack('!i',len(value))+value if value is not None else struct.pack('!i',-1))
                    body=body[:2]+b''.join(fields);length=struct.pack('!I',len(body)+4)
                    self.h.event('fault_activated',fault='observer_identity_mismatch',session=c['pid'])
                c['client'].sendall(tag+length+body)
                if tag==b'E':
                    # Attribute an idle lock's fatal response without recording
                    # provider data, SQL parameters or PostgreSQL error prose.
                    fields={v[:1]:v[1:].decode() for v in body.split(b'\0') if v}
                    self.h.event('server_error_forwarded',session=c['pid'],
                                 sqlstate=fields.get(b'C'),severity=fields.get(b'V'))
                if tag in (b'C',b'Z',b'N',b'E'):
                    self.h.event('protocol_response',tag=tag.decode(),session=c['pid'],query=c['query'],command_tag=body.rstrip(b'\0').decode() if tag==b'C' else None,ready_status=body.decode() if tag==b'Z' else None)
        except (EOFError,OSError):
            self.close_connection(c,'server_eof_or_error')

    def close(self):
        self.closed.set()
        self.listener.close()
        for g in self.gates:
            g.release()
        for c in self.connections:
            self.close_connection(c)
        # accept/front may append workers until their sockets close.
        for t in self.threads:
            t.join(timeout=3)
            assert not t.is_alive(), 'proxy worker did not stop'
        self.h.reserve_port(self.h.ports[3])
        self.h.event('proxy_cleanup',cancel_requests=self.cancel_requests)

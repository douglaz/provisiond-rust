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

    def wait(self):
        assert self.activated.wait(10), f'fault never activated: {self.pattern}'
        return self.connection

    def release(self):
        self.released.set()


class Proxy:
    def __init__(self, harness, port=55413):
        from harness import free_port
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
        self.spawn(self.accept)

    def spawn(self, fn, *args):
        t = threading.Thread(target=fn,args=args)
        self.threads.append(t)
        t.start()

    def arm(self, pattern, direction='reply'):
        gate = Gate(pattern,direction)
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
            server = socket.create_connection(('127.0.0.1',55410))
            c = dict(client=client,server=server,pid=None,query='',gate=None)
            self.connections.append(c)
            self.spawn(self.front,c)

    def close_connection(self,c):
        for sock in [c['client'],c['server']]:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        self.h.event('transport_closed',session=c['pid'])

    def gate(self,c,direction,tag=None):
        for g in self.gates:
            if (g.direction==direction and g.pattern in c['query'] and
                (not g.activated.is_set() or g.connection is c) and
                (direction!='commit_reply' or tag==b'C')):
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
                c['server'].sendall(tag+length+body)
                if tag in (b'Q',b'P'):
                    self.h.event('bytes_transmitted',query=c['query'],session=c['pid'],protocol=tag.decode())
        except (EOFError,OSError):
            self.close_connection(c)

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
                c['client'].sendall(tag+length+body)
                if tag in (b'C',b'Z',b'N',b'E'):
                    self.h.event('protocol_response',tag=tag.decode(),session=c['pid'],query=c['query'])
        except (EOFError,OSError):
            self.close_connection(c)

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
        self.h.event('proxy_cleanup',cancel_requests=self.cancel_requests)

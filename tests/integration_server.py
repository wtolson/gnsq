import errno
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile


def with_all(head, *tail):
    def decorator(fn, *args):
        with head as arg:
            args = args + (arg,)
            if not tail:
                return fn(*args)
            return with_all(*tail)(fn, *args)
    return decorator


class BaseIntegrationServer(object):
    protocols = ('TCP', 'HTTP')

    protocol_re = re.compile(' '.join([
        r'(?P<protocol>[A-Z]+):',
        r'listening on',
        r'(?P<address>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]+)',
    ]))

    def __init__(self, address='127.0.0.1'):
        self.address = address
        self.protocol_ports = {}
        self.data_path = tempfile.mkdtemp()

    @property
    def version(self):
        version = os.environ.get('NSQ_VERSION', '0.2.30')
        # .partition("-")[0] handles the 1.0.0-compat case.
        return tuple([int(v.partition('-')[0]) for v in version.split('.')])

    @property
    def cmd(self):
        raise NotImplementedError('cmd not implemented')

    @property
    def tcp_port(self):
        return self.protocol_ports['TCP']

    @property
    def tcp_address(self):
        return '%s:%d' % (self.address, self.tcp_port)

    @property
    def http_port(self):
        return self.protocol_ports['HTTP']

    @property
    def http_address(self):
        return '%s:%d' % (self.address, self.http_port)

    def _random_port(self):
        if self.version < (0, 3, 5):
            return random.randint(10000, 65535)
        return 0

    def _random_address(self):
        return '%s:%d' % (self.address, self._random_port())

    def _parse_protocol_ports(self):
        while len(self.protocol_ports) < len(self.protocols):
            line = self.child.stderr.readline()
            sys.stderr.write(line)

            if not line:
                raise Exception('server exited prematurely')

            line = line.decode('utf-8')
            if 'listening on' not in line:
                continue

            match = self.protocol_re.search(line)
            if not match:
                raise Exception('unexpected line: %r' % line)

            protocol = match.group('protocol')
            if protocol not in self.protocols:
                continue

            port = int(match.group('port'), 10)
            self.protocol_ports[protocol] = port

    def __enter__(self):
        sys.stderr.write('running: %s\n' % ' '.join(self.cmd))
        self.child = subprocess.Popen(self.cmd, stderr=subprocess.PIPE)
        self._parse_protocol_ports()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.child.terminate()
        except OSError as error:
            if error.errno == errno.ESRCH:
                return
            raise

        while True:
            line = self.child.stderr.readline()
            if not line:
                break
            sys.stderr.write(line)

        self.child.wait()
        shutil.rmtree(self.data_path)


class NsqdIntegrationServer(BaseIntegrationServer):
    tls_cert = os.path.join(os.path.dirname(__file__), 'cert.pem')
    tls_key = os.path.join(os.path.dirname(__file__), 'key.pem')

    def __init__(self, lookupd=None, **kwargs):
        if self.has_https():
            self.protocols = ('TCP', 'HTTP', 'HTTPS')

        self.lookupd = lookupd
        super(NsqdIntegrationServer, self).__init__(**kwargs)

    def has_https(self):
        return self.version >= (0, 2, 28)

    @property
    def https_port(self):
        return self.protocol_ports['HTTPS']

    @property
    def https_address(self):
        return '%s:%d' % (self.address, self.https_port)

    @property
    def cmd(self):
        cmd = [
            'nsqd',
            '--broadcast-address', self.address,
            '--tcp-address', self._random_address(),
            '--http-address', self._random_address(),
            '--data-path', self.data_path,
            '--tls-cert', self.tls_cert,
            '--tls-key', self.tls_key,
        ]

        if self.has_https():
            cmd.extend(['--https-address', self._random_address()])

        if self.lookupd:
            cmd.extend(['--lookupd-tcp-address', self.lookupd])

        return cmd


class LookupdIntegrationServer(BaseIntegrationServer):
    @property
    def cmd(self):
        return [
            'nsqlookupd',
            '--broadcast-address', self.address,
            '--tcp-address', self._random_address(),
            '--http-address', self._random_address(),
        ]

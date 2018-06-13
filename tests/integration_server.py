import errno
import io
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile


class BaseIntegrationServer(object):
    protocols = ('TCP', 'HTTP')

    protocol_re = re.compile(' '.join([
        r'(?P<protocol>[A-Z]+):',
        r'listening on',
        r'(?P<address>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]+)',
    ]))

    version_re = re.compile(' '.join([
        r'(?P<name>[a-z]+)',
        r'v(?P<version>[0-9]+\.[0-9]+\.[0-9]+)(?:-[a-z]+)?',
        r'\(built w\/(?P<go_version>[a-z0-9.-]+)\)'
    ]))

    def __init__(self, address='127.0.0.1'):
        self.address = address
        self.protocol_ports = {}
        self.data_path = tempfile.mkdtemp()
        self.name, self.version, self.go_version = self._parse_version()

    def _parse_version(self):
        output = subprocess.check_output([self.executable, '--version'])
        match = self.version_re.match(output.decode('utf-8'))
        name = match.group('name')
        version = tuple(int(v) for v in match.group('version').split('.'))
        go_version = match.group('go_version')
        return name, version, go_version

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
        if sys.version_info[0] == 3:
            self.child.stderr = io.TextIOWrapper(self.child.stderr, 'utf-8')
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
    executable = 'nsqd'

    tls_cert = os.path.join(os.path.dirname(__file__), 'cert.pem')
    tls_key = os.path.join(os.path.dirname(__file__), 'key.pem')

    def __init__(self, lookupd=None, **kwargs):
        super(NsqdIntegrationServer, self).__init__(**kwargs)

        if self.has_https():
            self.protocols = ('TCP', 'HTTP', 'HTTPS')

        self.lookupd = lookupd

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
            self.executable,
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
    executable = 'nsqlookupd'

    @property
    def cmd(self):
        return [
            self.executable,
            '--broadcast-address', self.address,
            '--tcp-address', self._random_address(),
            '--http-address', self._random_address(),
        ]

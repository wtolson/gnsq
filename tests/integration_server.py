import os
import random
import time
import shutil
import subprocess
import tempfile
import os.path
import urllib3


def with_all(head, *tail):
    def decorator(fn, *args):
        with head as arg:
            args = args + (arg,)
            if not tail:
                return fn(*args)
            return with_all(*tail)(fn, *args)
    return decorator


class BaseIntegrationServer(object):
    http = urllib3.PoolManager()

    def __init__(self, address=None, tcp_port=None, http_port=None):
        if address is None:
            address = '127.0.0.1'

        if tcp_port is None:
            tcp_port = random.randint(10000, 65535)

        if http_port is None:
            http_port = tcp_port + 1

        self.address = address
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.data_path = tempfile.mkdtemp()

    @property
    def version(self):
        version = os.environ.get('NSQ_VERSION', '0.2.28')
        return tuple([int(v) for v in version.split('.')])

    @property
    def cmd(self):
        raise NotImplementedError('cmd not implemented')

    @property
    def tcp_address(self):
        return '%s:%d' % (self.address, self.tcp_port)

    @property
    def http_address(self):
        return '%s:%d' % (self.address, self.http_port)

    def is_running(self):
        try:
            url = 'http://%s/ping' % self.http_address
            return self.http.request('GET', url).data == 'OK'
        except urllib3.exceptions.HTTPError:
            return False

    def wait(self):
        for attempt in xrange(12):
            if self.is_running():
                return
            time.sleep(0.01 * pow(2, attempt))
        raise RuntimeError('unable to start: %r' % ' '.join(self.cmd))

    def __enter__(self):
        self.subp = subprocess.Popen(self.cmd)
        self.wait()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.subp.terminate()
        self.subp.wait()
        shutil.rmtree(self.data_path)


class NsqdIntegrationServer(BaseIntegrationServer):
    tls_cert = os.path.join(os.path.dirname(__file__), 'cert.pem')
    tls_key = os.path.join(os.path.dirname(__file__), 'key.pem')

    def __init__(self, lookupd=None, **kwargs):
        self.lookupd = lookupd
        super(NsqdIntegrationServer, self).__init__(**kwargs)

    @property
    def https_port(self):
        return self.http_port + 1

    @property
    def https_address(self):
        return '%s:%d' % (self.address, self.https_port)

    @property
    def cmd(self):
        cmd = [
            'nsqd',
            '--tcp-address', self.tcp_address,
            '--http-address', self.http_address,
            '--data-path', self.data_path,
            '--tls-cert', self.tls_cert,
            '--tls-key', self.tls_key,
        ]

        if self.version >= (0, 2, 28):
            cmd.extend(['--https-address', self.https_address])

        if self.lookupd:
            cmd.extend(['--lookupd-tcp-address', self.lookupd])

        return cmd


class LookupdIntegrationServer(BaseIntegrationServer):
    @property
    def cmd(self):
        return [
            'nsqlookupd',
            '--tcp-address', self.tcp_address,
            '--http-address', self.http_address,
        ]

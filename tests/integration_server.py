import random
import time
import shutil
import subprocess
import tempfile
import requests
import os.path


class IntegrationNsqdServer(object):
    tls_cert = os.path.join(os.path.dirname(__file__), 'cert.pem')
    tls_key = os.path.join(os.path.dirname(__file__), 'key.pem')

    def __init__(self, port=None):
        if port is None:
            port = random.randint(10000, 65535)
        self.port = port
        self.data_path = tempfile.mkdtemp()

    @property
    def tcp_address(self):
        return '127.0.0.1:{}'.format(self.port)

    @property
    def http_address(self):
        return '127.0.0.1:{}'.format(self.port + 1)

    def is_running(self):
        try:
            resp = requests.get('http://{}/ping'.format(self.http_address))
            return resp.text == 'OK'
        except requests.ConnectionError:
            return False

    def wait(self):
        while True:
            if self.is_running():
                break
            time.sleep(0.1)

    def __enter__(self):
        print 'running:', ' '.join([
            'nsqd',
            '--tcp-address={}'.format(self.tcp_address),
            '--http-address={}'.format(self.http_address),
            '--data-path={}'.format(self.data_path),
            '--tls-cert={}'.format(self.tls_cert),
            '--tls-key={}'.format(self.tls_key),
        ])

        self.nsqd = subprocess.Popen([
            'nsqd',
            '--tcp-address={}'.format(self.tcp_address),
            '--http-address={}'.format(self.http_address),
            '--data-path={}'.format(self.data_path),
            '--tls-cert={}'.format(self.tls_cert),
            '--tls-key={}'.format(self.tls_key),
        ])
        self.wait()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.nsqd.terminate()
        self.nsqd.wait()
        shutil.rmtree(self.data_path)

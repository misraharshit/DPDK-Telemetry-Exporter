# Sourced from:
# https://git.dpdk.org/dpdk-stable/tree/usertools/dpdk-telemetry.py?h=22.11
"""
Script to be used with V2 Telemetry.
Using this Prometheus read the Telemetry response.
"""
import json
import os
import socket
import subprocess
import time

from prometheus_client import Gauge
from prometheus_client import start_http_server


# Global constants
metrics_api_port = int(os.environ.get('METRICS_API_PORT', 9138))
METRICS_PORT_PREFIX = 'dpdk_port_'
NODE_NAME = os.environ.get('NODE_NAME', '')
metrics = {}
metric = Gauge('dpdk_network_bytes_total',
               'List of metrics related to DPDK Interface',
               ['pod_name', 'pci_address', 'namespace',
                'type', 'node_name'])


def parse_socketpath():
    """
    Parse telemetry socketpaths
    : return : Socket path
    """
    directory = '/tmp/touchstone'
    filename = 'dpdk_telemetry.v2'
    command = ['find', directory, '-name', filename]
    output = subprocess.check_output(command).decode('utf-8')

    socket_paths = output.strip().split('\n')
    return socket_paths


def start_http(metrics_api_port):
    """
    Start http server on metrics_api_port 8000
    : return : http server object on metrics_api_port 8000
    """
    start_http_server(metrics_api_port)


def read_socket(sock, buf_len, echo=True, pretty=False):
    """ Read data from socket and return it in JSON format """
    reply = sock.recv(buf_len).decode()
    try:
        ret = json.loads(reply)
    except json.JSONDecodeError:
        print('Error in reply: ', reply)
        sock.close()
        raise
    return ret


def handle_socket(path, namespace, pod_name):
    """ Connect to socket and parse the telemetry data """
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)

    try:
        sock.connect(path)
    except OSError:
        print('Error connecting to ' + path)
        sock.close()
        return
    try:
        print(f'Connected to socket: {path}')

        commands = ['/ethdev/info,0', '/ethdev/xstats,0',  '/ethdev/list']
        pod_name = pod_name
        namespace = namespace
        node_name = NODE_NAME
        telemetry_stats = {}

        for cmd in commands:
            sock.send(cmd.encode())
            metrics_data = read_socket(sock, 1024)
            port_info = metrics_data.get('/ethdev/info')

            metric_stats = metrics_data.get('/ethdev/xstats')
            if port_info is not None:
                pci_address = port_info['name']
                print('PCI_ADDRESS:', pci_address)
            if metric_stats is not None:
                for stat, value in metric_stats.items():
                    telemetry_stats[stat] = value
        for telemetry_stat, telemetry_value in telemetry_stats.items():
            full_metric_name = METRICS_PORT_PREFIX + telemetry_stat
            metric_key = '.'.join([pod_name, pci_address, full_metric_name])
            metric.labels(pod_name, pci_address, namespace,
                          full_metric_name, node_name).set(telemetry_value)
            metrics[metric_key] = telemetry_value

    finally:
        sock.close()


if __name__ == '__main__':
    start_http(metrics_api_port)
    while True:
        socket_paths = parse_socketpath()
        try:
            for socket_path in socket_paths:
                # Pick 3th and 4th element after split from
                # /tmp/touchstone/<namespace>/<podname>/rte/...
                namespace, pod_name = socket_path.split('/')[3:5]
                handle_socket(socket_path, namespace, pod_name)
        except Exception as e:
            print(f'An error occurred: {e}')
        time.sleep(5)

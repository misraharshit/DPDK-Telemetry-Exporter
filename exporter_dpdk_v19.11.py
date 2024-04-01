# Sourced from:
# http://git.dpdk.org/dpdk-stable/tree/usertools/dpdk-telemetry-client.py?h=19.11
import json
import os
import socket
import subprocess
import time

from prometheus_client import Gauge
from prometheus_client import start_http_server


BUFFER_SIZE = 200000
# metrics_api_port is port number over which http server will be runnning
metrics_api_port = int(os.environ.get('METRICS_API_PORT', 9138))
NODE_NAME = os.environ.get('NODE_NAME', '')
METRICS_PORT_PREFIX = 'dpdk_port_'
metrics = {}

METRICS_REQ = "{\"action\":0,\"command\":\"ports_all_stat_values\",\
                \"data\":null}"
API_REG = "{\"action\":1,\"command\":\"clients\",\"data\":{\"client_path\":\""
API_UNREG = "{\"action\":2,\"command\":\"clients\",\"data\":{\"client_path\":\""

metric = Gauge('dpdk_network_bytes_total',
               'List of metrics related to DPDK Interface',
               ['pod_name', 'pci_address', 'namespace',
                'type', 'node_name'])

clients = []


def parse_socketpath():
    """
    Parse telemetry socketpaths
    : return : Socket path
    """
    directory = '/tmp/touchstone'
    filename = 'telemetry'
    command = ['find', directory, '-name', filename]
    output = subprocess.check_output(command).decode('utf-8')

    socket_paths = output.strip().split('\n')
    return socket_paths


def create_directory(directory):
    """
    Create a directory on host OS
    : return : Directory
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        pass


def get_clientpath(socketpath):
    """
    Create client socket path for each telemetry socket path
    : return : List of client socket path.
    """
    client_socket_paths = []
    for telemetry_socket_path in socketpath:
        if telemetry_socket_path:
            print('Creating client socket for telemetry socket: '
                  f'{telemetry_socket_path}.')
            telemetry_socket_path = telemetry_socket_path.split('/')
            result = '-'.join(telemetry_socket_path[-4:-2])
            create_directory('/tmp/touchstone/' + result)
            client_socket_path = os.path.join(
                os.sep, 'tmp', 'touchstone', result, '.client')
            client_socket_paths.append(client_socket_path)

    return client_socket_paths


class Exporter:
    """
    Exporter Class start http server and register and fetch metrices from the
    each telemetry sockets of dpdk applications.
    """

    def __init__(self, metrics_api_port):
        """
        Constructor of Exporter class
        : param metrics_api_port: METRICS_API_PORT
        number on which http server will run.
        """
        self.metrics_api_port = metrics_api_port

    def setup_clients(self, socketpaths, clientpaths):
        """
        Setup Client objects and store in global clients list
        : return : List of client object which
        stored in the global clients list
        """
        global clients

        exists = [i.clientpath for i in clients]

        for client_socket_path, telemetry_socket_path in zip(
                clientpaths, socketpaths):
            if client_socket_path not in exists:
                print(f"Used client socket: '{client_socket_path}' with "
                      f"telemetry socket: '{telemetry_socket_path}'.")
                clients.append(
                    Client(client_socket_path, telemetry_socket_path)
                )

    def register_and_fetch_metrics(self):
        """
        Register each client with telemetry socket and fetch data
        : return : Nothing
        """
        global clients

        while True:
            socketpaths = parse_socketpath()
            print(f'Scanned socket paths: {socketpaths}')

            clientpaths = get_clientpath(socketpaths)

            self.setup_clients(socketpaths, clientpaths)

            for client in clients:

                if not client.is_socket_bound(client.clientpath):
                    client.register()

                try:
                    data = client.requestmetrics()
                    self.parse_metrics_response(
                        data, client.pod_name, client.namespace)

                except Exception as e:
                    print(e)
            print(metrics)
            time.sleep(5)

    def start_http(self):
        """
        Start http server on metrics_api_port 8000
        : return : http server object on metrics_api_port 8000
        """
        start_http_server(self.metrics_api_port)

    def parse_metrics_response(self, response, pod_name, namespace):
        """
        Parse data recieved from telemetry socket and pass to gauge metric
        : return : Gauge Metrics to http server on metrics_api_port 8000.
        Which further can transmit to prometheus.
        """
        metrics_data = json.loads(response)

        stats = metrics_data['data'][0]['stats']

        pci_address = metrics_data['data'][0]['pci_address']
        for stat in stats:
            full_metric_name = METRICS_PORT_PREFIX + stat['name']
            metric_key = '.'.join([pod_name, pci_address, full_metric_name])

            metric.labels(pod_name, pci_address, namespace,
                          full_metric_name, NODE_NAME).set(stat['value'])
            metrics[metric_key] = stat['value']


class Socket:
    """
    Socket class is used to create client socket for Client Class
    """

    def __init__(self):
        """
        Constructor which create socket.
        """
        self.send_fd = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.recv_fd = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.client_fd = None

    def __del__(self):
        """
        Destructor which delete socket.
        """
        try:
            if self.send_fd:
                self.send_fd.close()
            if self.recv_fd:
                self.recv_fd.close()
            if self.client_fd:
                self.client_fd.close()
        except Exception:
            print('Error - Sockets could not be closed')
            raise


class Client:
    """
    Client class is used to create client object and used to register,
    unregister and requestmetrics from the telemetry socket.
    """

    def __init__(self, clientpath, socketpath):
        """
        Creates a client instance
        : param clientpath,socketpath : Takes input a client
        and telemetry socket path.
        """
        self.socket = Socket()
        self.clientpath = clientpath
        self.socketpath = socketpath
        # Pick 4th and 5th element after split from
        # /tmp/touchstone/<execution-uuid>/<namespace>/<podname>/rte/...
        self.namespace, self.pod_name = socketpath.split('/')[3:5]

    def __del__(self):
        """
        It is destructor which call unregister()
        : return : Nothing
        """
        try:
            self.unregister()
        except Exception:
            print('Error - Client could not be destroyed')
            raise

    def is_socket_bound(self, client_socket_paths):
        """
        Check Socket Path exist or not
        : return : Client socket path if it is exist on the OS
        """
        return os.path.exists(client_socket_paths)

    def register(self):
        """
        Connects a client to DPDK-instance
        : return : client socket which used to make
        connection with telemetry socket
        """
        if os.path.exists(self.clientpath):
            os.unlink(self.clientpath)
        while True:
            try:

                self.socket.recv_fd.bind(self.clientpath)
                break
            except socket.error as msg:
                print('Error - Socket binding error: ' + str(msg) + '\n')
                time.sleep(5)
        try:
            self.socket.recv_fd.settimeout(2)
            self.socket.send_fd.connect(self.socketpath)
            data = (API_REG + self.clientpath + "\"}}")
            self.socket.send_fd.sendall(data.encode())

            self.socket.recv_fd.listen(1)
            self.socket.client_fd = self.socket.recv_fd.accept()[0]
        except Exception:
            pass

    def unregister(self):
        """
        Unregister a given client
        :return : Nothing
        """

        try:
            self.socket.client_fd.send((API_UNREG +
                                        self.clientpath + "\"}}").encode())
        except Exception:
            pass
        finally:
            try:
                del self.socket

                os.unlink(self.clientpath)
                clients.remove(self)

            except Exception as e:
                pass
                print(e)

    def requestmetrics(self):
        """
        Requests metrics for given client
        :return : A json response which further used to
        parse data to into metrics
        """
        try:
            self.socket.client_fd.send(METRICS_REQ.encode())
            data = self.socket.client_fd.recv(BUFFER_SIZE).decode()
            return data
        except Exception as e:
            print(e)
            self.unregister()


if __name__ == '__main__':
    exporter = Exporter(metrics_api_port)
    exporter.start_http()
    exporter.register_and_fetch_metrics()

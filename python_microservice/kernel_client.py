

import os
import time
from tornado.escape import json_encode, json_decode, url_escape
import requests

DEFAULT_TIMEOUT = 60
DEFAULT_USERNAME = 'anonymous'


class KernelLauncher:

    def __init__(self, eg_host):
        self.http_api_endpoint = 'http://{}/api/kernels'.format(eg_host)

    def launch(self, kernelspec_name, username=DEFAULT_USERNAME):
        print('Launching up a {} kernel on the Gateway....'.format(kernelspec_name))
        kernel_id = None
        json_data = {'name': kernelspec_name}
        if username is not None:
            json_data['env'] = {'KERNEL_USERNAME': username}
        response = requests.post(self.http_api_endpoint, data=json_encode(json_data))
        if response.status_code == 201:
            json_data = response.json()
            kernel_id = json_data.get("id")
            print('Launched kernel {}'.format(kernel_id))
        else:
            raise RuntimeError('Error creating kernel : {} response code'.format(response.status_code))

        return kernel_id

    def shutdown(self, kernel_id):
        print("Shutting down kernel : {}".format(kernel_id))
        if not kernel_id:
            return False
        url = "{}/{}".format(self.http_api_endpoint, kernel_id)
        response = requests.delete(url)
        if response.status_code == 204:
            print('Kernel {} shutdown'.format(kernel_id))
            return True
        else:
            raise RuntimeError('Error shutting down kernel {}'.format(kernel_id))

"""
class KernelClient:

    def __init__(self, client):
        self.


    def eval(self, code, timeout=DEFAULT_TIMEOUT):
"""

kid = []
n = 10

launcher = KernelLauncher('lresende-elyra:8888')
for i in range(0,n):
    print('Starting kernel {}'.format(i))
    id = launcher.launch('spark_python_yarn_cluster')
    kid.append(id)
    print('Kernel {} started'.format(kid[i]))
    print('')

time.sleep(30)

for i in range(0,n):
    print('Stopping kernel {}'.format(i))
    launcher.shutdown(kid[i])
    print('Kernel {} stopped'.format(kid[i]))





import threading
import datetime

kid = []
n = 10
delay_s = 10

launcher = KernelLauncher('lresende-elyra:8888')

def start_kernel():
    try:
        id = launcher.launch('spark_python_yarn_cluster')
        print('Kernel {} started'.format(id))
        kid.append(id)
    except RuntimeError as re:
        print('Failed to start kernel {}'.format(id, re))
        print('')

def stop_kernel(id):
    try:
        print('Stopping kernel {}'.format(id))
        launcher.shutdown(id)
    except RuntimeError as re:
        print('Failed to stop kernel {}'.format(id, re))
        print('')


def log_with_time(message):
    time = datetime.datetime.now().strftime("[%d-%m-%Y %I:%M:%S.%f %p]")
    print("{} {}".format(time, message))

threads = []

log_with_time("Starting")
while True:
    threads.clear()
    kid.clear()

    for i in range(0,n):
        t = threading.Thread(target=start_kernel)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    threads.clear()

    print()
    log_with_time("All kernels started...")
    print()

    time.sleep(delay_s)
    requests.get('http://lresende-elyra:8888/api/kernels')
    log_with_time("Starting kernel shutdown...")
    print()

    for i in range(0,n):
        t = threading.Thread(target=stop_kernel, args=(kid[i],))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()


    print()
    log_with_time("All kernels stopped...")
    print()
    time.sleep(delay_s)

log_with_time("ending")

exit(0)

for i in range(0,n):
    print('Starting kernel {}'.format(i))
    try:
        id = launcher.launch('spark_python_yarn_cluster')
        kid.append(id)
        print('Kernel {} started'.format(kid[i]))
    except:
        print('Failed to start kernel {}'.format(i))
    print('')

time.sleep(30)

for i in range(0,n):
    print('Stopping kernel {}'.format(i))
    try:
        launcher.shutdown(kid[i])
        print('Kernel {} stopped'.format(kid[i]))
    except:
        print('Failed to stop kernel {}'.format(i))
    print('')



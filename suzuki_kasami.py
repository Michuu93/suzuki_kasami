import random
import sys
import threading
from collections import deque
from datetime import datetime
from threading import Thread
from time import sleep

from mpi4py import MPI

comm = MPI.COMM_WORLD
tid = comm.Get_rank()
N = comm.Get_size()

cs_lock = threading.Lock()
token_lock = threading.Lock()
rn_lock = threading.Lock()
release_lock = threading.Lock()
request_lock = threading.Lock()
send_lock = threading.Lock()

Q = deque()
has_token = 0
in_cs = 0
waiting_for_token = 0

RN = []
LN = []
for i in range(0, N):
    LN.append(0)
    RN.append(0)

# wreczenie tokenu na start procesowi 0
if tid == 0:
    print("%s: Jestem %d i posiadam token startowy." % (datetime.now().strftime('%M:%S'), tid))
    sys.stdout.flush()
    has_token = 1
RN[0] = 1


def receive_request():
    global LN
    global RN
    global Q
    global in_cs
    global waiting_for_token
    global has_token
    while True:
        message = comm.recv(source=MPI.ANY_SOURCE)
        if message[0] == 'RN':
            with rn_lock:
                requester_id = message[1]
                cs_value = message[2]
                RN[requester_id] = max([cs_value, RN[requester_id]])
                if cs_value < RN[requester_id]:
                    print(
                        "%s: Request od %d jest przedawniony." % (datetime.now().strftime('%M:%S'), requester_id))
                    sys.stdout.flush()

                if (has_token == 1) and (in_cs == 0) and (RN[requester_id] == (LN[requester_id] + 1)):
                    has_token = 0
                    send_token(requester_id)

        elif message[0] == 'token':
            with token_lock:
                print("%s: Jestem %d i otrzymalem token." % (datetime.now().strftime('%M:%S'), tid))
                sys.stdout.flush()
                has_token = 1
                waiting_for_token = 0
                LN = message[1]
                Q = message[2]
                critical_section()


def send_request(message):
    for i in range(N):
        if tid != i:
            to_send = ['RN', tid, message]
            comm.send(to_send, dest=i)


def send_token(recipent):
    global Q
    with send_lock:
        print("%s: Jestem %d i wysylam token do %d." % (datetime.now().strftime('%M:%S'), tid, recipent))
        sys.stdout.flush()
        global in_cs
        to_send = ['token', LN, Q]
        comm.send(to_send, dest=recipent)


def request_cs():
    global RN
    global in_cs
    global waiting_for_token
    global has_token
    with request_lock:
        if has_token == 0:
            RN[tid] += 1
            print("%s: Jestem %d i chce token po raz %d." % (datetime.now().strftime('%M:%S'), tid, RN[tid]))
            sys.stdout.flush()
            waiting_for_token = 1
            send_request(RN[tid])


def release_cs():
    global in_cs
    global LN
    global RN
    global Q
    global has_token
    with release_lock:
        LN[tid] = RN[tid]
        for k in range(N):
            if k not in Q:
                if RN[k] == (LN[k] + 1):
                    Q.append(k)
                    print("%s: Jestem %d i dodaje %d do kolejki. Kolejka po dodaniu: %s." % (
                        datetime.now().strftime('%M:%S'), tid, k, str(Q)))
                    sys.stdout.flush()
        if len(Q) != 0:
            has_token = 0
            send_token(Q.popleft())


def critical_section():
    global in_cs
    global has_token
    with cs_lock:
        if has_token == 1:
            in_cs = 1
            print("%s: Jestem %d i wykonuje %d CS." % (datetime.now().strftime('%M:%S'), tid, RN[tid]))
            sys.stdout.flush()
            sleep(random.uniform(2, 5))
            print("%s: Jestem %d i skonczylem wykonywac %d CS." % (datetime.now().strftime('%M:%S'), tid, RN[tid]))
            sys.stdout.flush()
            in_cs = 0
            release_cs()


try:
    thread_receiver = Thread(target=receive_request)
    thread_receiver.start()
except:
    print("Error: unable to start thread!   ")
    sys.stdout.flush()

while True:
    if has_token == 0:
        sleep(random.uniform(1, 3))
        request_cs()
    elif in_cs == 0:
        critical_section()
    while waiting_for_token:
        sleep(0.5)

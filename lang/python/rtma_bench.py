import sys
import ctypes
import time
import os
import multiprocessing
import PyRTMA3 as PyRTMA
from PyRTMA3 import copy_from_msg, copy_to_msg
import ctypes

MT_TEST_MSG = 1234
MT_SUBSCRIBER_READY = 5678

# Note: this should match the header defined in RTMA.h
MODULE_ID = ctypes.c_short
HOST_ID = ctypes.c_short
MSG_TYPE = ctypes.c_int
class RTMA_MSG_HEADER(ctypes.Structure):
    _fields_ = [ 
        ('msg_type', ctypes.c_int),
        ('msg_count', ctypes.c_int),
        ('send_time', ctypes.c_double),
        ('recv_time', ctypes.c_double),
        ('src_host_id', HOST_ID),
        ('src_mod_id', MODULE_ID),
        ('dest_host_id', HOST_ID),
        ('dest_mod_id', MODULE_ID),
        ('num_data_bytes', ctypes.c_int),
        ('remaining_bytes', ctypes.c_int),
        ('is_dynamic', ctypes.c_int),
        ('reserved', ctypes.c_int)
    ]

HEADER_SIZE = ctypes.sizeof(RTMA_MSG_HEADER)

def create_test_msg(msg_size):
    class TEST(ctypes.Structure):
        _pack_ = True # source:False # JW this can probably be removed, the optional field is actually supposed to be a small integer
        _fields_ = [('data', ctypes.c_byte * msg_size)]
    return TEST

#sys.path.append('../')

def publisher_loop(pub_id=0, num_msgs=10000, msg_size=512, num_subscribers=1, ready_flag=None, server='127.0.0.1:7111'):
    # Setup Client
    mod = PyRTMA.RTMA_Module(0, 0)
    mod.ConnectToMMM(server)
    mod.Subscribe(MT_SUBSCRIBER_READY)

    # Signal that publisher is ready
    if ready_flag is not None:
        ready_flag.set()

    # Wait for the subscribers to be ready
    num_subscribers_ready = 0
    msg = PyRTMA.CMessage()
    while num_subscribers_ready < num_subscribers:
        rcv = mod.ReadMessage(msg, timeout=-1)
        if rcv == 1:
            msg_type = msg.GetHeader().msg_type
            if msg_type == MT_SUBSCRIBER_READY:
                num_subscribers_ready += 1

    # Create TEST message with dummy data
    msg = PyRTMA.CMessage(MT_TEST_MSG)
    data = create_test_msg(msg_size)()
    data.data[:] = list(range(msg_size))
    copy_to_msg(data, msg)

    # Send loop
    tic = time.perf_counter()
    for n in range(num_msgs):
        mod.SendMessage(msg)
    toc = time.perf_counter()

    # Stats
    test_msg_size = HEADER_SIZE + ctypes.sizeof(data)
    dur = (toc-tic)
    data_rate = test_msg_size * num_msgs / float(1048576) / dur
    print(f"Publisher[{pub_id}] -> {num_msgs} messages | {int((num_msgs)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")

    mod.DisconnectFromMMM()

def subscriber_loop(sub_id, num_msgs, msg_size=512, server='127.0.0.1:7111'):
    # Setup Client
    mod = PyRTMA.RTMA_Module(0, 0)
    mod.ConnectToMMM(server)
    mod.Subscribe(MT_TEST_MSG)
    mod.Subscribe(PyRTMA.MT_EXIT)
    mod.SendSignal(MT_SUBSCRIBER_READY)

    # Read Loop (Start clock after first TEST msg received)
    abort_timeout = max(num_msgs/10000, 10) #seconds
    abort_start = time.perf_counter()

    msg_count = 0
    msg = PyRTMA.CMessage()
    while msg_count < num_msgs:
        rcv = mod.ReadMessage(msg, timeout=1)
        if rcv == 1:
            msg_type = msg.GetHeader().msg_type
            if msg_type == MT_TEST_MSG:
                if msg_count == 0:

                    tic = time.perf_counter()
                toc = time.perf_counter()
                msg_count += 1
            elif msg_type == PyRTMA.MT_EXIT:
                print('Got EXIT')
                break

        if time.perf_counter() - abort_start > abort_timeout: 
            print(f"Subscriber [{sub_id:d}] Timed out.")
            break

    # Stats
    msg_data = create_test_msg(msg_size)()
    test_msg_size = HEADER_SIZE + ctypes.sizeof(msg_data)
    dur = toc - tic
    data_rate = (test_msg_size * num_msgs) / float(1048576) / dur
    if msg_count == num_msgs:
        print(f"Subscriber [{sub_id:d}] -> {msg_count} messages | {int((msg_count-1)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")
    else:
        print(f"Subscriber [{sub_id:d}] -> {msg_count} ({int(msg_count/num_msgs *100):0d}%) messages | {int((msg_count-1)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")

    mod.DisconnectFromMMM()

if __name__ == '__main__':
    import argparse

    # Configuration flags for bench utility
    parser = argparse.ArgumentParser(description='rtmaClient bench test utility')
    parser.add_argument('-ms', default=128, type=int, dest='msg_size', help='Messge size in bytes.')
    parser.add_argument('-n', default=10000, type=int, dest='num_msgs', help='Number of messages.')
    parser.add_argument('-np', default=1, type=int, dest='num_publishers', help='Number of concurrent publishers.')
    parser.add_argument('-ns', default=1, type=int, dest='num_subscribers', help='Number of concurrent subscribers.')
    parser.add_argument('-s',default='127.0.0.1:7111', dest='server', help='RTMA message manager ip address (default: 127.0.0.1:7111)')
    args = parser.parse_args()

    #Main Thread RTMA client
    mod = PyRTMA.RTMA_Module(0, 0)
    mod.ConnectToMMM(args.server)

    print("Initializing publisher processses...")
    publisher_ready = []
    publishers = []
    for n in range(args.num_publishers):
        publisher_ready.append(multiprocessing.Event())
        publishers.append(
                multiprocessing.Process(
                    target=publisher_loop,
                    kwargs={
                        'pub_id': n+1,
                        'num_msgs': int(args.num_msgs/args.num_publishers),
                        'msg_size': args.msg_size, 
                        'num_subscribers': args.num_subscribers,
                        'ready_flag': publisher_ready[n],
                        'server': args.server})
                    )
        publishers[n].start()

    # Wait for publisher processes to be established
    for flag in publisher_ready:
        flag.wait()

    print('Waiting for subscriber processes...')
    subscribers = []
    for n in range(args.num_subscribers):
        subscribers.append(
                multiprocessing.Process(
                    target=subscriber_loop,
                    args=(n+1, args.num_msgs, args.msg_size),
                    kwargs={'server':args.server}))
        subscribers[n].start()

    print("Starting Test...")
    print(f"RTMA packet size: {HEADER_SIZE + args.msg_size}")
    print(f'Sending {args.num_msgs} messages...')

    # Wait for publishers to finish
    for publisher in publishers:
        publisher.join()

    # Wait for subscribers to finish
    abort_timeout = max(args.num_msgs/10000, 10) #seconds
    abort_start = time.perf_counter()
    abort = False

    while not abort:
        subscribers_finished = 0
        for subscriber in subscribers:
            if subscriber.exitcode is not None:
                subscribers_finished += 1

        if subscribers_finished == len(subscribers):
            break

        if time.perf_counter() - abort_start > abort_timeout: 
            mod.SendSignal(PyRTMA.MT_EXIT)
            print('Test Timeout! Sending Exit Signal...')
            abort = True

    for subscriber in subscribers:
        subscriber.join()
    
    print('Done!')

import sys
import ctypes
import time
import os
import multiprocessing
import PyRTMA3 as PyRTMA
from PyRTMA3 import copy_from_msg, copy_to_msg
import ctypes

MT_TEST_MSG = 1234
MT_PUBLISHER_READY = 5677
MT_PUBLISHER_DONE = 5678
MT_SUBSCRIBER_READY = 5679
MT_SUBSCRIBER_DONE = 5680

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

def publisher_loop(pub_id=0, num_msgs=10000, msg_size=128, num_subscribers=1, server='127.0.0.1:7111'):
    # Setup Client
    mod = PyRTMA.RTMA_Module(0, 0)
    mod.ConnectToMMM(server)
    mod.SendModuleReady()
    mod.Subscribe(MT_SUBSCRIBER_READY)

    # Signal that publisher is ready
    mod.SendSignal(MT_PUBLISHER_READY)

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

    mod.SendSignal(MT_PUBLISHER_DONE)

    # Stats
    test_msg_size = HEADER_SIZE + ctypes.sizeof(data)
    dur = toc-tic
    data_rate = test_msg_size * num_msgs / 1e6 / dur
    print(f"Publisher[{pub_id}] -> {num_msgs} messages | {int((num_msgs)/dur)} messages/sec | {data_rate:0.1f} MB/sec | {dur:0.6f} sec ")

    mod.DisconnectFromMMM()

def subscriber_loop(sub_id, num_msgs, msg_size=128, server='127.0.0.1:7111'):
    # Setup Client
    mod = PyRTMA.RTMA_Module(0, 0)
    mod.ConnectToMMM(server)
    mod.SendModuleReady()
    mod.Subscribe(MT_TEST_MSG)
    mod.Subscribe(PyRTMA.MT_EXIT)
    mod.SendSignal(MT_SUBSCRIBER_READY)

    # Read Loop (Start clock after first TEST msg received)
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

    mod.SendSignal(MT_SUBSCRIBER_DONE)

    # Stats
    msg_data = create_test_msg(msg_size)()
    test_msg_size = HEADER_SIZE + ctypes.sizeof(msg_data)
    dur = toc - tic
    data_rate = (test_msg_size * num_msgs) / 1e6 / dur
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
    mod.SendModuleReady()
    mod.Subscribe(MT_PUBLISHER_READY)
    mod.Subscribe(MT_PUBLISHER_DONE)
    mod.Subscribe(MT_SUBSCRIBER_READY)
    mod.Subscribe(MT_SUBSCRIBER_DONE)
    
    sys.stdout.write(f"Packet size: {args.msg_size} bytes\n")
    sys.stdout.write(f"Sending {args.num_msgs} messages...\n")
    sys.stdout.flush()

    #print("Initializing publisher processses...")
    publishers = []
    for n in range(args.num_publishers):
        #publisher_ready.append(multiprocessing.Event())
        publishers.append(
                multiprocessing.Process(
                    target=publisher_loop,
                    kwargs={
                        'pub_id': n+1,
                        'num_msgs': int(args.num_msgs/args.num_publishers),
                        'msg_size': args.msg_size, 
                        'num_subscribers': args.num_subscribers,
                        'server': args.server})
                    )
        publishers[n].start()

    # Wait for publisher processes to be established
    publishers_ready = 0
    msg = PyRTMA.CMessage()
    while publishers_ready < args.num_publishers:
        rcv = mod.ReadMessage(msg, timeout=-1)
        if rcv == 1:
            msg_type = msg.GetHeader().msg_type
            if msg_type == MT_PUBLISHER_READY:
                publishers_ready += 1

    #print('Waiting for subscriber processes...')
    subscribers = []
    for n in range(args.num_subscribers):
        subscribers.append(
                multiprocessing.Process(
                    target=subscriber_loop,
                    kwargs={
                        'sub_id': n+1,
                        'num_msgs': args.num_msgs,
                        'msg_size': args.msg_size, 
                        'server': args.server})
                    )
        subscribers[n].start()

    #print("Starting Test...")
    #print(f"RTMA packet size: {HEADER_SIZE + args.msg_size}")
    #print(f'Sending {args.num_msgs} messages...')

    # Wait for subscribers to finish
    abort_timeout = 120 #seconds
    abort_start = time.perf_counter()

    subscribers_done = 0
    publishers_done = 0
    while (subscribers_done < args.num_subscribers) or (publishers_done < args.num_publishers):
        rcv = mod.ReadMessage(msg, timeout=-1)
        if rcv == 1:
            msg_type = msg.GetHeader().msg_type
            if msg_type == MT_SUBSCRIBER_DONE:
                subscribers_done += 1
            if msg_type == MT_PUBLISHER_DONE:
                publishers_done += 1

        if (time.perf_counter() - abort_start) > abort_timeout: 
            mod.SendSignal(PyRTMA.MT_EXIT)
            sys.stdout.write('Test Timeout! Sending Exit Signal...\n')
            sys.stdout.flush()

    for publisher in publishers:
        publisher.join()

    for subscriber in subscribers:
        subscriber.join()

    #print('Done!')

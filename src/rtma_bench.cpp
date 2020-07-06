#include "rtma.h"

#include <vector>
#include <thread>
#include <chrono>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>


#define MT_TEST_MSG 1234
#define MT_SUBSCRIBER_READY 5678

typedef struct {
	char data[];
}MDF_TEST_MSG;


int subscriber_loop(int id, char* server, int num_msgs, int msg_size) {
	RTMA_Module mod;

	mod.InitVariables(0, 0);
	mod.ConnectToMMM(server);
	mod.Subscribe(MT_TEST_MSG);
	mod.Subscribe(MT_EXIT);
	mod.SendModuleReady();
	
	int msg_rcvd = 0;
	std::chrono::time_point<std::chrono::high_resolution_clock> start;

	mod.SendSignal(MT_SUBSCRIBER_READY);

	while (msg_rcvd < num_msgs) {
		CMessage M;
		int status = mod.ReadMessage(&M, -1);

		if (status) {
			switch (M.msg_type) {
			case MT_TEST_MSG:
				if (msg_rcvd == 0)
					start = std::chrono::high_resolution_clock::now();
				msg_rcvd++;
				break;
			case MT_EXIT:
				goto quit;
			}
		}
	}

quit:
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> diff = end - start;
	double data_transfer = (double(msg_rcvd) - 1.0) * double(msg_size + sizeof(RTMA_MSG_HEADER)) / double(1024) / double(1024) / diff.count();

	mod.DisconnectFromMMM();

	printf("Subscriber[%d] -> %d messages | %d messages/sec | %0.1lf MB/sec | %0.6lf sec\n",
		id,
		msg_rcvd,
		int((double(msg_rcvd) - 1.0)/diff.count()),
		data_transfer,
		diff.count());

	return 0;
}

int publisher_loop(int id, char* server, int num_msgs, int msg_size, int num_subscribers) {
	RTMA_Module mod;

	mod.InitVariables(0, 0);
	mod.ConnectToMMM(server);
	mod.Subscribe(MT_EXIT);
	mod.Subscribe(MT_SUBSCRIBER_READY);
	mod.SendModuleReady();

	int subscribers_ready = 0;
	while (subscribers_ready < num_subscribers) {
		CMessage M;
		int status = mod.ReadMessage(&M, -1);

		if (status) {
			switch (M.msg_type) {
			case MT_SUBSCRIBER_READY:
				subscribers_ready++;
				break;
			}
		}
	}

	size_t packet_size = msg_size * sizeof(char);
	MDF_TEST_MSG *msg = (MDF_TEST_MSG *)malloc(packet_size);

	// Add some dummy data to send
	for (int i = 0; i < msg_size; i++) {
		msg->data[i] = i % 128;
	}

	CMessage M(MT_TEST_MSG);
	M.SetData(msg, packet_size);

	auto start = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < num_msgs; i++) {

		int status = mod.SendMessageRTMA(&M, (MODULE_ID)0 ,(HOST_ID)0);
	}
	
	auto end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double> diff = end - start;
	double data_transfer = double(num_msgs) * double(msg_size + sizeof(RTMA_MSG_HEADER)) / double(1024) / double(1024) / diff.count();

	mod.DisconnectFromMMM();

	printf("Publisher[%d] -> %d messages | %d messages/sec | %0.1lf MB/sec | %0.6lf sec\n",
		id,
		num_msgs,
		int(double(num_msgs) / diff.count()),
		data_transfer,
		diff.count());

	return 0;
}

void usage(void) {
	printf("Usage: rtma-bench [-s server(127.0.0.1:7111)] [-np NUM_PUBLISHERS] [-ns NUM_SUBSCRIBERS] [-n NUM_MSGS] [-ms MESSAGE_SIZE]\n");

	printf("- h\n\tShow help message\n");
	printf("- ms int\n\tSize of the message. (default 128)\n");
	printf("- n int\n\tNumber of Messages to Publish(default 100000)\n");
	printf("- np int\n\tNumber of Concurrent Publishers(default 1)\n");
	printf("- ns int\n\tNumber of Concurrent Subscribers\n");
	printf("- s string\n\tRTMA message manager ip address (default 127.0.0.1:7111)\n");
}

int main(int argc, char** argv) {

	char server[] = "localhost:7111";
	int num_publishers = 1;
	int num_subscribers = 1;
	int num_msgs = 100000;
	int msg_size = 128;

	char* flag;

	const char* prog_name = argv[0];

	while (--argc > 0 && (*++argv)[0] == '-') {
		flag = &((*argv)[1]);

		if (strcmp(flag, "np") == 0) {
			num_publishers = atoi((*++argv));
			argc--;
		}
		else if (strcmp(flag, "ns") == 0) {
			num_subscribers = atoi((*++argv));
			argc--;
		}
		else if (strcmp(flag, "n") == 0) {
			num_msgs = atoi((*++argv));
			argc--;
		}
		else if (strcmp(flag, "ms") == 0) {
			msg_size = atoi((*++argv));
			argc--;
		}
		else if (strcmp(flag, "h") == 0) {
			usage();
			return 0;
		}
		else {
			fprintf(stderr, "%s: unknown arg %s\n", prog_name, *argv);
			usage();
			return -1;
		}
	}

	std::vector<std::thread> publishers;
	std::vector<std::thread> subscribers;

	printf("Initializing publisher threads...\n");

	for (int i = 0; i < num_publishers; i++)
		publishers.push_back(std::thread(publisher_loop, i + 1, server, num_msgs / num_publishers, msg_size, num_subscribers));

	std::this_thread::sleep_for(std::chrono::seconds(1));

	for (int i = 0; i < num_subscribers; i++)
		subscribers.push_back(std::thread(subscriber_loop, i + 1, server, num_msgs, msg_size));

	printf("Waiting for subscriber threads...\n");

	printf("Starting Test...\n");

	printf("Total RTMA Packet Size: %d bytes\n", sizeof(RTMA_MSG_HEADER) + msg_size);
	for (auto& publisher : publishers)
		publisher.join();

	for (auto& subscriber : subscribers)
		subscriber.join();

	printf("Done!\n");
	return 0;
}
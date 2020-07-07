UNAME := $(shell uname)
CC := gcc
CXX := g++
LIBS=-lRTMA

ifeq ($(UNAME), Linux)
COMMON=-O2 -I$(RTMA)/include -DUSE_LINUX -std=c++11 -pthread
LIBEXT=.so
endif

ifeq ($(UNAME), Darwin)
COMMON=-O2 -I$(RTMA)/include -DUSE_LINUX -std=c++11 -stdlib=libc++ -pthread
LIBEXT=.dylib
endif

.PHONY: all
all: bin/rtma_bench
	rm build/*.o

bin/rtma_bench: build/rtma_bench.o
	$(CXX) $(COMMON) -L$(RTMA)/lib build/rtma_bench.o $(LIBS) -o bin/rtma_bench
build/rtma_bench.o: src/rtma_bench.cpp
	$(CXX) -c $(COMMON) src/rtma_bench.cpp -o build/rtma_bench.o
clean:
	rm build/*.o
	rm bin/rtma_bench

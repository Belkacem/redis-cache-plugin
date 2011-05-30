CC=cc
LD=ld
CFLAGS= -g -D_REENTRANT=1 -m32 -O2 -Wall
LDFLAGS= -g -shared -m32

TS_BASE=../../../..
API_INC= -I. -I$(TS_BASE)/proxy/api/include

default: redis_cache.so

credis.o: credis.c
	$(CC) $(CFLAGS) $(API_INC) -o credis.o -c credis.c

redis_cache.o: redis_cache.cc
	$(CXX) $(CFLAGS) $(API_INC) -o redis_cache.o -c redis_cache.cc

redis_cache.so: redis_cache.o credis.o
	$(CXX) $(LDFLAGS) -o redis_cache.so redis_cache.o credis.o

clean:
	rm -f redis_cache.so redis_cache.o credis.o

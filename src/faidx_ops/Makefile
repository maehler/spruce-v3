INCLUDES=-I. -I/crex/proj/uppstore2017145/V3/software/include/fastxio -I/sw/apps/build/boost/1.63.0_gcc6.3.0/rackham/lib
LIBRARIES=-L/crex/proj/uppstore2017145/V3/software/lib
FLAGS=-O3 -std=c++11
LIBS=-lfastxio -lbz2

default: all

all: build_index print_index

MurmurHash3.o: MurmurHash3.cpp
	g++ $(INCLUDES) $(FLAGS) -c MurmurHash3.cpp

index.o: index.cpp
	g++ $(INCLUDES) $(LIBRARIES) $(FLAGS) -c index.cpp

build_index.o: build_index.cpp
	 g++ $(INCLUDES) $(LIBRARIES) $(FLAGS) -c build_index.cpp

print_index.o: print_index.cpp
	g++ $(INCLUDES) $(LIBRARIES) $(FLAGS) -c print_index.cpp

build_index: build_index.o MurmurHash3.o index.o
	g++ $(INCLUDES) $(LIBRARIES) $(FLAGS) -o build_index index.o MurmurHash3.o build_index.o /sw/apps/build/boost/1.63.0_gcc6.3.0/rackham/lib/libboost_serialization.so.1.63.0 $(LIBS)

print_index: print_index.o MurmurHash3.o index.o
	g++ $(INCLUDES) $(LIBRARIES) $(FLAGS) -o print_index index.o MurmurHash3.o print_index.o /sw/apps/build/boost/1.63.0_gcc6.3.0/rackham/lib/libboost_serialization.so.1.63.0 $(LIBS)

clean:
	rm -f *.o
	rm -f build_index print_index

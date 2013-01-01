GFLAGS_LIB = /home/andi/opensources/gflags/.libs/libgflags.a
GLOG_LIB = /home/andi/opensources/glog/.libs/libglog.a
SUPERSONIC_LIB = /home/andi/opensources/supersonic/.libs/libsupersonic.a
PROBOBUF_LIB = /home/andi/opensources/protobuf/src/.libs/libprotobuf.a
RE2_LIB = /home/andi/opensources/re2/obj/libre2.a

CFLAGS = -I /home/andi/opensources/supersonic/\
	 -I /home/andi/mystudio/cpp/gtest_learn/gmock/gtest/include\
	 -g

all: main.cc
	g++ -o supersonic_learn $(LIBS) $(CFLAGS) \
	 main.cc $(SUPERSONIC_LIB) $(GFLAGS_LIB) $(GLOG_LIB) $(PROBOBUF_LIB) $(RE2_LIB) -lpthread -lrt -lboost_timer

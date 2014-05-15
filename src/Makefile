.PHONY:	all

.cpp.o:
	$(CXX) -c -g -o $@ $(CXXFLAGS) -pthread -Imongo -I. -I/usr/pkg/include -I/usr/include $<

all: mongodb-stats mongodb-dump

mongo_client_lib.o: mongo/client/mongo_client_lib.cpp

common.o: watch/common.cpp mongo_pw.cpp
dump_mongodb.o: watch/dump_mongodb.cpp
watch_mongodb.o: watch/watch_mongodb.cpp

mongo_pw.cpp: mongo_client_lib.o
	$(PERL5) ../script/obfuscatepw.pl --nm-file mongo_client_lib.o --password $(MONGO_PW) --filter mongo\\d >mongo_pw.cpp

mongodb-dump: mongo_client_lib.o dump_mongodb.o common.o
	$(CXX) -o $@ -L/usr/pkg/lib -Wl,-R/usr/pkg/lib -pthread -lboost_thread -lboost_filesystem -lboost_program_options -lboost_locale -lboost_timer $>

mongodb-stats: mongo_client_lib.o watch_mongodb.o common.o
	$(CXX) -o $@ -L/usr/pkg/lib -Wl,-R/usr/pkg/lib -pthread -lboost_thread -lboost_filesystem -lboost_program_options -lboost_locale -lboost_timer $>

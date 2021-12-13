# ROOT = ../
# CC = g++
# BINDIR		?= $(ROOT)/bin
# BUILDIR		?= $(ROOT)/build
# BINS = $(BINDIR)
# CFLAGS += -std=c++11
# include $(ROOT)/common/Makefile.common

# .PHONY:	all clean
# all:	main

# sequential-db.o: 
# 	$(CC) $(CFLAGS) -c $(BUILDIR)/sequential-db.o SequentialDB.cpp

# parallel-db.o: 
# 	$(CC) $(CFLAGS) -c $(BUILDIR)/parallel-db.o ParallelDB.cpp

# test-db.o:
# 	$(CC) $(CFLAGS) -c -o $(BUILDIR)/test-db.o ../tests/test_db.cpp

# main: sequential-db.o test-db.o
# 	$(CC) $(CFLAGS) $(BUILDIR)/sequential-db.o -o -c $(BINDIR)/sequential-db $(LDFLAGS)
# 	$(CC) $(CFLAGS) $(BUILDIR)/testdb.o -o -c $(BINDIR)/test-db $(LDFLAGS)

# clean:
# 	rm -f $(BINS)

# *****************************************************
# Variables to control Makefile operation
CC = g++
CXXFLAGS = -O3 -std=c++11 -lpthread -L -fgnu-tm -ltbb
LDFLAGS = -fgnu-tmld -lzlib --verbose
# ****************************************************
# Targets needed to bring the executable up to date
test_db: test_db.o SequentialDB.o DataParallelDB.o
	$(CC) $(CXXFLAGS) -o test_db test_db.o SequentialDB.o DataParallelDB.o

# The main.o target can be written more simply
test_db.o: test_db.cpp SequentialDB.cpp  DataParallelDB.cpp
	$(CC) $(CXXFLAGS) -c test_db.cpp 

SequentialDB.o: SequentialDB.cpp

DataParallelDB.o: DataParallelDB.cpp

clean:
	rm -f test_db.o test_db DataParallelDB.o SequentialDB.o
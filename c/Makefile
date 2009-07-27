
CC = cc
PREFIX = /usr/local
ERLLIB = $(PREFIX)/lib/erlang
CPPFLAGS = -I$(ERLLIB)/erts-5.7.1/include/ -I$(ERLLIB)/lib/erl_interface-3.6.1/include -I./ -I$(PREFIX)/include/ \
  -L$(ERLLIB)/erts-5.7.1/lib/ -L$(PREFIX)/lib/ \
  -Wall -g -O2 -fPIC

# linux
#LDFLAGS = -shared
# macosx
LDFLAGS = -dynamic -bundle -undefined suppress -flat_namespace $(ERLLIB)/lib/erl_interface-3.6.1/lib/libei.a -ltokyocabinet

OUT = ../lib/tcdb.so

all: $(OUT)

$(OUT): tcdb.c
		mkdir -p ../lib/
		$(CC) $(CPPFLAGS) -c -o tcdb.o tcdb.c
		$(CC) $(CPPFLAGS) $(LDFLAGS) -o $(OUT) tcdb.o

clean:
		rm -rf *.o *.so
		rm -f $(OUT)


all: lib/tcdb.o ebin/gen_server2.beam ebin/mdb.app
	erlc -o ebin/ -pa ebin/ elibs/*.erl

lib/tcdb.o:
	$(MAKE) -C c

ebin/gen_server2.beam: elibs/gen_server2.erl
	erlc -o ebin elibs/gen_server2.erl

ebin/mdb.app: elibs/mdb.app
	cp elibs/mdb.app ebin/mdb.app

clean:
	$(MAKE) -C c clean
	rm -f ebin/*
	rm -f erl_crash.dump

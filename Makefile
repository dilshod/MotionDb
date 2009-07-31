
all: lib/tcdb.o ebin/gen_server2.beam ebin/motiondb.app
	erlc -o ebin/ -pa ebin/ elibs/*.erl

lib/tcdb.o:
	$(MAKE) -C c

ebin/gen_server2.beam: elibs/gen_server2.erl
	erlc -o ebin elibs/gen_server2.erl

ebin/motiondb.app: elibs/motiondb.app
	cp elibs/motiondb.app ebin/motiondb.app

clean:
	$(MAKE) -C c clean
	rm -f ebin/*
	rm -f erl_crash.dump

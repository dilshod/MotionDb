
INCLUDES = -I deps/yaws-1.84/include -I deps/eunit/include
APPLICATION = motiondb
DOC_OPTS = {dir, \"doc\"}

all: lib/tc.o ebin/gen_server2.beam ebin/motiondb.app
	erlc +hipe -o ebin/ $(INCLUDES) -pa ebin/ elibs/*.erl

lib/tc.o:
	$(MAKE) -C c

ebin/gen_server2.beam: elibs/gen_server2.erl
	erlc -o ebin elibs/gen_server2.erl

ebin/motiondb.app: elibs/motiondb.app
	cp elibs/motiondb.app ebin/motiondb.app

test: all
	erlc -I elibs -o ebin/ -pa ebin/ etest/*.erl
	erl -pa ebin/ $(INCLUDES) -noshell -sname test@localhost -s test test -run init stop

coverage: all
	erlc -I elibs -o ebin/ -pa ebin/ etest/*.erl
	erl -pa ebin/ $(INCLUDES) -noshell -sname test@localhost -s test coverage -run init stop

edoc:
	erl -noshell -pa ebin -eval "edoc:application($(APPLICATION), \"elibs\", [$(DOC_OPTS)])" -s init stop

clean:
	$(MAKE) -C c clean
	rm -rf doc/*
	rm -f ebin/*
	rm -f erl_crash.dump

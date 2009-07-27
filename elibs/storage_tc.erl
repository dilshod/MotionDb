-module(storage_tc).
-export([start/1, close/1, sync/1, put/4, get/3, remove/2, remove/3]).

-define(OPEN, 0).
-define(CLOSE, 1).
-define(PUT, 2).
-define(PUTDUP, 3).
-define(PUTCAT, 4).
-define(PUTKEEP, 5).
-define(GET, 6).
-define(GETDUP, 7).
-define(REMOVE, 8).
-define(RANGE, 9).
-define(SYNC, 10).
-define(INFO, 11).
-define(READ, 12).
-define(VANISH, 13).

start(Path) ->
  Dir = filename:join([filename:dirname(code:which(storage)), "..", "lib"]),
  case erl_ddll:load(Dir, "tcdb") of
    ok ->
      Port = open_port({spawn, tcdb}, [binary]),
      ok = call(Port, ?OPEN, filename:join([Path, "db.tch"])),
      {ok, Port};
    {error, Err} ->
      Msg = erl_ddll:format_error(Err),
      {error, Msg}
  end.

close(Port) ->
  call(Port, ?CLOSE, []).

sync(Port) ->
  call(Port, ?SYNC, []).

put(Port, Key, Value, false) ->
  call(Port, ?PUT, {binarize(Key), term_to_binary(Value)});

put(Port, Key, Value, true) ->
  call(Port, ?PUTDUP, {binarize(Key), term_to_binary(Value)}).

get(Port, Key, false) ->
  case call(Port, ?GET, binarize(Key)) of
    {ok, Value} -> {ok, binary_to_term(Value)};
    Res -> Res
  end;

get(Port, Key, true) ->
  {ok, Result} = call(Port, ?GETDUP, binarize(Key)),
  {ok, lists:map(fun(Res) -> binary_to_term(Res) end, Result)}.

remove(Port, Key) ->
  call(Port, ?REMOVE, binarize(Key)).

remove(Port, Key, Value) ->
  call(Port, ?REMOVE, {binarize(Key), term_to_binary(Value)}).

%%
%% intrnal functions
%%
call(Port, Command, Arguments) ->
  binary_to_term(list_to_binary(port_control(Port, Command, term_to_binary(Arguments)))).

binarize(Val) when is_binary(Val) -> Val;
binarize(Val) -> term_to_binary(Val).

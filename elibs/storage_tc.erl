-module(storage_tc).
-export([start/1, close/1, info/1, sync/1]).
-export([put/3, append/3, putvalues/3, remove/2, remove/3, truncate/1]).
-export([get/2, getvalues/2, first/1, next/2, backup/2]).

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
-define(ITERATE, 12).
-define(VANISH, 13).
-define(BACKUP, 14).

%%
%% @spec start(Path) -> {ok, Port} | {error, Reason}
%% @doc Start new storage instance
%% @end
%%
start(Path) ->
  case erl_ddll:load(lib_misc:root(lib), "tc") of
    ok ->
      Port = open_port({spawn, tc}, [binary]),
      ok = call(Port, ?OPEN, filename:join([Path, "db.tc"])),
      {ok, Port};
    {error, Err} ->
      Msg = erl_ddll:format_error(Err),
      {error, Msg}
  end.

%%
%% @spec close(Port) -> ok | {error, Reason}
%% @doc Close storage instance
%% @end
%%
close(Port) ->
  call(Port, ?CLOSE, []).

%%
%% @spec info(Port) -> {ok, RecordsCount, FileSize} | {error, Reason}
%% @doc Get info
%% @end
%%
info(Port) ->
  call(Port, ?INFO, []).

%%
%% @spec sync(Port) -> ok | {error, Reason}
%% @doc Sync to disc
%% @end
%%
sync(Port) ->
  call(Port, ?SYNC, []).

%%
%% @spec put(Port, Key, Value) -> ok | {error, Reason}
%% @doc Put
%% @end
%%
put(Port, Key, Value) ->
  call(Port, ?PUT, {term_to_binary(Key), term_to_binary(Value)}).

%%
%% @spec append(Port, Key, Value) -> ok | {error, Reason}
%% @doc Append value
%% @end
%%
append(Port, Key, Value) ->
  call(Port, ?PUTDUP, {term_to_binary(Key), term_to_binary(Value)}).

%%
%% @spec putvalues(Port, Key, Value) -> ok | {error, Reason}
%% @doc Put values
%% @end
%%
putvalues(Port, Key, Values) ->
  call(Port, ?REMOVE, term_to_binary(Key)),
  putvalues2(Port, Key, Values).

putvalues2(Port, Key, [Value | Values]) ->
  call(Port, ?PUTDUP, {term_to_binary(Key), term_to_binary(Value)}),
  putvalues2(Port, Key, Values);

putvalues2(_Port, _Key, []) -> ok.

%%
%% @spec remove(Port, Key) -> ok | {error, Reason}
%% @doc Remove item
%% @end
%%
remove(Port, Key) ->
  call(Port, ?REMOVE, term_to_binary(Key)).

%%
%% @spec remove(Port, Key, Value) -> ok | {error, Reason}
%% @doc Remove value from item
%% @end
%%
remove(Port, Key, Value) ->
  call(Port, ?REMOVE, {term_to_binary(Key), term_to_binary(Value)}).

%%
%% @spec truncate(Port) -> ok | {error, Reason}
%% @doc Remove all items
%% @end
%%
truncate(Port) ->
  call(Port, ?VANISH, {}).

%%
%% @spec get(Port, Key) -> {ok, Value} | {error, not_found}
%% @doc Get value for key
%% @end
%%
get(Port, Key) ->
  case call(Port, ?GET, term_to_binary(Key)) of
    {ok, Value} -> {ok, binary_to_term(Value)};
    Res -> Res
  end.

%%
%% @spec getvalues(Port, Key) -> {ok, Values}
%% @doc Get values for key
%% @end
%%
getvalues(Port, Key) ->
  {ok, Result} = call(Port, ?GETDUP, term_to_binary(Key)),
  {ok, lists:map(fun(Res) -> binary_to_term(Res) end, Result)}.

%%
%% @spec first(Port) -> {ok, Key} | {error, not_found}
%% @doc Get first key
%% @end
%%
first(Port) ->
  case call(Port, ?ITERATE, none) of
    {ok, NextKey} -> {ok, binary_to_term(NextKey)};
    Result -> Result
  end.

%%
%% @spec next(Port, Key) -> {ok, Key} | {error, not_found}
%% @doc Get next key
%% @end
%%
next(Port, Key) ->
  case call(Port, ?ITERATE, term_to_binary(Key)) of
    {ok, NextKey} -> {ok, binary_to_term(NextKey)};
    Result -> Result
  end.

%%
%% @spec backup(Port, Filename) -> ok | {error, Reason}
%% @doc Backup database
%% @end
%%
backup(Port, Filename) ->
  call(Port, ?BACKUP, Filename).

%% =====================================================================================================
%% intrnal functions
%% =====================================================================================================
call(Port, Command, Arguments) ->
  binary_to_term(list_to_binary(port_control(Port, Command, term_to_binary(Arguments)))).

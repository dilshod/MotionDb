-module(motiondb).
-behaviour(application).
-include("motiondb.hrl").

-export([start/0, start/2, stop/1]).
-export([put/2, append/2, putvalues/2, delete/1, delete/2, get/1, getvalues/1]).

%%
%% @spec start() -> ok
%% @doc Start motiondb application
%% @end
%%
start() ->
  application:start(sasl),
  application:start(crypto),
  application:start(motiondb).

%%
%% @spec start(Type, StartArgs) -> {ok, Pid} | {ok, Pid, State} | {error, Reason}
%% @doc Start motiondb application
%% @end
%%
start(_Type, []) ->
  case application:get_env(pidfile) of
    {ok, Location} ->
      Pid = os:getpid(),
      ok = file:write_file(Location, list_to_binary(Pid));
    undefined -> ok
  end,
  motiondb_sup:start_link().

%%
%% @spec stop(State) -> void()
%% @doc Stop motiondb
%% @end
%%
stop({_, Sup}) ->
  exit(Sup, shutdown),
  ok.


%%
%% @spec put(Key, Value) -> ok | {error, Reason}
%% @doc Put value for key
%% @end
%%
put(Key, Value) ->
  message(Key, {put, Key, Value}, fun lists:sort/1).

%%
%% @spec append(Key, Value) -> ok | {error, Reason}
%% @doc Append value for key
%% @end
%%
append(Key, Value) ->
  message(Key, {append, Key, Value}, fun lists:sort/1).

%%
%% @spec putvalues(Key, Values) -> ok | {error, Reason}
%% @doc Put value for key
%% @end
%%
putvalues(Key, Values) ->
  message(Key, {putvalues, Key, Values}, fun lists:sort/1).

%%
%% @spec delete(Key) -> {ok, Value} | {error, not_found} | {error, Reason}
%% @doc Delete all values for key
%% @end
%%
delete(Key) ->
  message(Key, {remove, Key}, fun lists:sort/1).

%%
%% @spec delete(Key, Value) -> {ok, Value} | {error, not_found} | {error, Reason}
%% @doc Delete value for key
%% @end
%%
delete(Key, Value) ->
  message(Key, {remove, Key, Value}, fun lists:sort/1).

%%
%% @spec get(Key) -> {ok, Value} | {error, not_found} | {error, Reason}
%% @doc Get value for key
%% @end
%%
get(Key) ->
  message(Key, {get, Key}, fun lib_misc:sort_nodes/1).

%%
%% @spec getvalues(Key) -> {ok, Value} | {error, Reason}
%% @doc Get value for key
%% @end
%%
getvalues(Key) ->
  message(Key, {getvalues, Key}, fun lib_misc:sort_nodes/1).





%% ===========================================================================================================
%% Internal functions
%% ===========================================================================================================
message(Key, Message, SortFun) ->
  Schema = schema:get(),
  if Schema#schema.state == up ->
    Hash = erlang:phash2(Key, (1 bsl 32) - 1),
    {ok, Partition} = schema:partition_for_hash(Hash, Schema#schema.partitions),
    case Partition of
      [] -> {error, partition_not_found};
      _ ->
        LeftNodes = SortFun(lib_misc:multiply(Partition#partition.nodes, nodes([this, visible]))),
        case Partition#partition.right_partition of
          #partition{key_bits=KeyBits, key=Key, nodes=Nodes} when (Hash band ((1 bsl KeyBits) - 1)) == Key ->
            RightNodes = SortFun(lib_misc:multiply(Nodes, nodes([this, visible]))),
            case call(Hash, RightNodes, Message) of
              {error, _} ->
                call(Hash, LeftNodes, Message);
              Result -> Result
            end;
          _ ->
            call(Hash, LeftNodes, Message)
        end
    end;
  true -> {error, node_down}
  end.

call(Hash, [Node | Nodes], Message) ->
  Server = {storage:storage(Hash bsr 28), Node},
  case (catch gen_server2:call(Server, Message)) of
    {'EXIT', _} -> call(Hash, Nodes, Message);
    {error, node_down} -> call(Hash, Nodes, Message);
    Result -> Result
  end;

call(_, [], _) ->
  {error, node_down}.

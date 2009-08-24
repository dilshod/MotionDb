-module(motiondb).
-include("motiondb.hrl").
-export([start/0, put/2, get/1, delete/1, putdup/2, getdup/1, delete/2, mapreduce/2]).
-export([sort_nodes/1]).

%%
%% @spec start() -> ok.
%% @doc Start motiondb application
%% @end
%%
start() ->
  application:start(sasl),
  application:start(crypto),
  application:start(motiondb).

%%
%% @spec put(Key, Value) -> ok | {error, Reason}.
%% @doc Put value for key
%% @end
%%
put(Key, Value) ->
  message(Key, {put, Key, Value, false}, fun lists:sort/1).

%%
%% @spec get(Key) -> {ok, Value} | {error, not_found} | {error, Reason}.
%% @doc Get value for key
%% @end
%%
get(Key) ->
  message(Key, {get, Key, false}, fun motiondb:sort_nodes/1).

%%
%% @spec delete(Key) -> {ok, Value} | {error, not_found} | {error, Reason}.
%% @doc Delete all values for key
%% @end
%%
delete(Key) ->
  message(Key, {remove, Key}, fun lists:sort/1).

%%
%% @spec putdup(Key, Value) -> ok | {error, Reason}.
%% @doc Append value for key
%% @end
%%
putdup(Key, Value) ->
  message(Key, {put, Key, Value, true}, fun lists:sort/1).

%%
%% @spec get(Key) -> {ok, Value} | {error, not_found} | {error, Reason}.
%% @doc Get all values for key
%% @end
%%
getdup(Key) ->
  message(Key, {get, Key, tru}, fun motiondb:sort_nodes/1).

%%
%% @spec delete(Key, Value) -> {ok, Value} | {error, not_found} | {error, Reason}.
%% @doc Delete value for key
%% @end
%%
delete(Key, Value) ->
  message(Key, {remove, Key, Value}, fun lists:sort/1).

%%
%% @spec mapreduce(Map, Reduce) -> {ok, Result} | {error, Reason}.
%% @doc Map reduce
%% @end
%%
mapreduce(_Map, _Reduce) ->
  {ok, []}.




%%
%% Internal functions
%%
message(Key, Message, SortFun) ->
  [Schema] = ets:lookup(motiondb, schema),
  if Schema#schema.state == up ->
    Hash = erlang:phash2(Key, (1 bsl 32) - 1),
    Partition = partition_for_hash(Hash, Schema#schema.partitions),
    LeftNodes = SortFun(motiondb_schema:multiply(Partition#partition.nodes, nodes([this, visible]))),
    case Partition#partition.right_partition of
      #partition{key_bits=KeyBits, key=Key, nodes=Nodes} when (Hash band ((1 bsl KeyBits) - 1)) == Key ->
        RightNodes = SortFun(motiondb_schema:multiply(Nodes, nodes([this, visible]))),
        case call(Hash, RightNodes, Message) of
          {error, _} ->
            call(Hash, LeftNodes, Message);
          Result -> Result
        end;
      _ ->
        call(Hash, LeftNodes, Message)
    end;
    true ->
      {error, node_down}
  end.

call(Hash, [Node | Nodes], Message) ->
  Server = {list_to_atom("storage" ++ integer_to_list(Hash bsr 28)), Node},
  case catch(gen_server2:call(Server, Message)) of
    {'EXIT', _} -> call(Hash, Nodes, Message);
    {error, nodedown} -> call(Hash, Nodes, Message);
    Result -> Result
  end;

call(_, [], _) ->
  {error, nodedown}.

sort_nodes(Nodes) ->
  lists:sort(
    fun(L, R) ->
      (L == node() orelse (L /= node() andalso random:uniform(2) == 1)) andalso R /= node()
    end,
    Nodes
  ).

partition_for_hash(Hash, [Partition | Partitions]) ->
  if (Hash band ((1 bsl Partition#partition.key_bits) - 1)) == Partition#partition.key ->
    Partition;
    true -> partition_for_hash(Hash, Partitions)
  end;

partition_for_hash(_, []) ->
  [].

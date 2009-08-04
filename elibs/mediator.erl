-module(mediator).
-include("motiondb.hrl").
-export([put/2, get/1, sort_nodes/1]).

%%
%% @spec put(Key, Value) -> ok | {error, Reason}.
%% @doc Put item to storage
%% @end
%%
put(Key, Value) ->
  message(Key, {put, Key, Value, false}, fun lists:sort/1).

%%
%% @spec get(Key) -> {ok, Value} | {error, not_found} | {error, Reason}.
%% @doc Get item to storage
%% @end
%%
get(Key) ->
  message(Key, {get, Key, false}, fun mediator:sort_nodes/1).

%%
%% Internal functions
%%
message(Key, Message, SortFun) ->
  [{state, State}] = ets:lookup(motiondb, state),
  Hash = erlang:phash2(Key, (1 bsl 32) - 1),
  Partition = partition_for_hash(Hash, State#config.partitions),
  LeftNodes = SortFun(motiondb:multiply(Partition#partition.nodes, nodes([this, visible]))),
  case Partition#partition.right_partition of
    #partition{key_bits=KeyBits, key=Key, nodes=Nodes} when (Hash band ((1 bsl KeyBits) - 1)) == Key ->
      RightNodes = SortFun(motiondb:multiply(Nodes, nodes([this, visible]))),
      case call(Hash, RightNodes, Message) of
        {error, _} ->
          call(Hash, LeftNodes, Message);
        Result -> Result
      end;
    _ ->
      call(Hash, LeftNodes, Message)
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

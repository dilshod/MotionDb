-module(mediator).
-include("motiondb.hrl").
-export([put/2, get/1]).

%%
%% @spec put(Key, Value) -> ok | {error, Reason}.
%% @doc Put item to storage
%% @end
%%
put(Key, Value) ->
  [{state, State}] = ets:lookup(motiondb, state),
  Hash = erlang:phash2(Key),
  Partition = partition_for_hash(Key, State#config.partitions),
  Nodes = motiondb:multiply(lists:sort(Partition#partition.nodes), nodes([this, visible])),
  call(Hash, Nodes, {put, Key, Value, false}).

%%
%% @spec get(Key) -> {ok, Value} | {error, not_found} | {error, Reason}.
%% @doc Get item to storage
%% @end
%%
get(Key) ->
  [{state, State}] = ets:lookup(motiondb, state),
  Hash = erlang:phash2(Key),
  Partition = partition_for_hash(Key, State#config.partitions),
  Nodes = motiondb:multiply(lists:sort(Partition#partition.nodes), nodes([this, visible])),
  call(Hash, Nodes, {get, Key, false}).

%%
%% Internal functions
%%
call(Hash, [Node | Nodes], Message) ->
  case catch(gen_server2:call(server_for_hash(Hash, Node), Message)) of
    {'EXIT', {{nodedown, _}, _}} -> call(Hash, Nodes, Message);
    Result -> Result
  end;

call(_, [], _) ->
  {error, nodedown}.

server_for_hash(Hash, Node) ->
  {list_to_atom("storage" ++ integer_to_list(Hash bsr 26)), Node}.

partition_for_hash(Hash, [Partition | Partitions]) ->
  if (Hash band ((1 bsl Partition#partition.key_bits) - 1)) == Partition#partition.key ->
    Partition;
    true -> partition_for_hash(Hash, Partitions)
  end;

partition_for_hash(_, []) ->
  [].

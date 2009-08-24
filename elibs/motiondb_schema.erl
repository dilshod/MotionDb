-module(motiondb_schema).
-behaviour(gen_server2).
-include("motiondb.hrl").

-export([start_link/0, multiply/2, current_partition/1, node_partition/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
  gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

%%
%% @spec multiply(Left::array(), Right::array(), []) -> Multiplyed::array().
%% @doc Multiply one array with another
%% @end
%%
multiply(Left, Right) -> multiply(Left, Right, []).
multiply([], _, Uniqs) -> Uniqs;
multiply(_, [], Uniqs) -> Uniqs;
multiply([Left | Lefts], Rights, Uniqs) ->
  case Rights -- [Left] of
    Rights -> multiply(Lefts, Rights, Uniqs);
    _ -> multiply(Lefts, Rights, [Left | Uniqs])
  end.

%%
%% @spec current_partition(Schema) -> Partition.
%% @doc Get current partition
%%
%%
current_partition(Schema) ->
  node_partition(Schema#schema.partitions, node()).

node_partition([Partition | Partitions], Node) ->
  case lists:any(fun(N) -> Node == N end, Partition#partition.nodes) of
    true -> Partition;
    false -> node_partition(Partitions, Node)
  end;

node_partition([], _) ->
  none.

%%
%% @spec save_schema(Schema) -> ok.
%% @doc Save schema
%% @end
%%
save_schema(Schema) ->
  global:trans(
    {motiondb, schema},
    fun() ->
      [LocalSchema] = ets:lookup(motiondb, schema),
      if LocalSchema#schema.version < Schema#schema.version ->
        ets:insert(motiondb, Schema),
        file:write_file(
          filename:join([filename:dirname(code:which(motiondb)), "..", "config", atom_to_list(node()) ++ ".bin"]),
          term_to_binary(Schema)
        ),
        ok;
        true -> false
      end
    end
  ).

turn_schema(State) ->
  global:trans(
    {motiondb, schema},
    fun() ->
      [Schema] = ets:lookup(motiondb, schema),
      ets:insert(motiondb, Schema#schema{state=State})
    end
  ).

%%
%% Server functions
%%
init([]) ->
  ets:new(motiondb, [named_table, public, set]),
  Path = filename:join([filename:dirname(code:which(motiondb)), "..", "config"]),
  file:make_dir(Path),
  Nodes = case file:read_file(filename:join([Path, atom_to_list(node()) ++ ".bin"])) of
    {ok, Content} ->
      InitSchema = binary_to_term(Content),
      ets:insert(motiondb, InitSchema#schema{state=down}),
      lists:flatten(lists:map(
        fun(Partition) -> lists:delete(node(), Partition#partition.nodes) end,
        InitSchema#schema.partitions
      ));
    {error, enoent} ->
      Partitions = [#partition{key=0, key_bits=0, nodes=[node()]}],
      InitSchema = #schema{version=0, partitions=Partitions, storage=storage_tc, datapath="database", state=down},
      ets:insert(motiondb, InitSchema#schema{state=down}),
      save_schema(InitSchema),
      []
  end,

  % Connect each node
  Result = lists:map(
    fun(Node) ->
      [Schema] = ets:lookup(motiondb, schema),
      case gen_server2:call({motiondb, Node}, {sync_schema, Schema}) of
        {ok, RemoteSchema} when RemoteSchema#schema.version > Schema#schema.version ->
          save_schema(RemoteSchema),
          up;
        _ -> down
      end
    end,
    Nodes
  ),
  case lists:any(fun(R) -> R == down end, Result) of
    true -> ok;
    false ->
      % if all nodes running
      turn_schema(up),
      % turn all remote nodes up
      lists:foreach(
        fun(Node) ->
          [Schema] = ets:lookup(motiondb, schema),
          case gen_server2:call({motiondb, Node}, {sync_schema, Schema}) of
            {ok, RemoteSchema} when RemoteSchema#schema.version > Schema#schema.version ->
              save_schema(RemoteSchema),
              up;
            _ -> down
          end
        end,
        Nodes
      )
    end,
  {ok, {}}.

handle_call({new_replica, Nodes}, _From, State) ->
  Result = global:trans(
    {motiondb, schema},
    fun() ->
      [Schema] = ets:lookup(motiondb, schema),
      SortedPartitions = lists:sort(
        fun(#partition{key_bits=Left}, #partition{key_bits=Right}) -> Left < Right end,
        lists:filter(fun(Partition) -> Partition#partition.right_partition == none end, Schema#schema.partitions)
      ),
      case SortedPartitions of
        [Partition | _] ->
          RightPartition = #partition{
            key = Partition#partition.key + (1 bsl Partition#partition.key_bits),
            key_bits = Partition#partition.key_bits + 1,
            nodes = Nodes
          },
          Partitions = lists:keyreplace(
            Partition#partition.key, 2,
            Schema#schema.partitions,
            Partition#partition{right_partition = RightPartition}
          ),
          NewSchema = Schema#schema{partitions=Partitions, version=Schema#schema.version + 1},
          io:format("~p~n", [NewSchema]),
          save_schema(NewSchema),

          NewNodes = lists:flatten(lists:map(
            fun(P) -> lists:delete(node(), P#partition.nodes) end,
            NewSchema#schema.partitions
          )),

          lists:map(
            fun(Node) ->
              gen_server2:call({motiondb_schema, Node}, {sync_schema, NewSchema})
            end,
            NewNodes
          ),
          ok;
        _ when length(Schema#partition.nodes) == 0 ->
          ok;
        _ -> {error, locked}
      end
    end
  ),
  {reply, Result, State};

handle_call({sync_schema, RemoteSchema}, _From, State) ->
  [Schema] = ets:lookup(motiondb, schema),
  io:format("Sync l: ~p == r: ~p", [Schema, RemoteSchema]),
  if Schema#schema.version < RemoteSchema#schema.version ->
      save_schema(RemoteSchema);
    Schema#schema.state == down andalso RemoteSchema#schema.state == up ->
      turn_schema(up);
    true ->
      ok
  end,
  {reply, ok, State};

handle_call(is_replica_nodes_up, _From, State) ->
  [Schema] = ets:lookup(motiondb, schema),
  case current_partition(Schema) of
    none ->
      {reply, false, State};
    Partition ->
      Result = lists:all(
        fun(Node) ->
          lists:all(
            fun(I) ->
              gen_server2:call({list_to_atom("storage" ++ integer_to_list(I)), Node}, is_up) == {ok, true}
            end,
            lists:seq(0, 15)
          ) == true
        end,
        Partition#partition.nodes
      ),
      {reply, Result, State}
  end;

handle_call(_Msg, _From, State) ->
  {reply, {error, invalid_message}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

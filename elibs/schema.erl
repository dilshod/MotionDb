-module(schema).
-include("motiondb.hrl").
-behaviour(gen_server2).

-export([start_link/0, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([info/0, join/1, new_partition/1, remove_partition/1, remove_node/1]).
-export([init/0, destroy/0, get/0, set/1]).
-export([current_partition/0, node_partition/1, all_nodes/0, all_remote_nodes/0, partition_for_hash/2]).

-record(state, {gossip}).

%%
%% @spec start_link() -> {ok, Pid}
%% @doc Start schema server
%% @end
%%
start_link() ->
  Pid = gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []),
  gossip_with_all(),
  Pid.

%%
%% @spec stop() -> ok
%% @doc Stop schema server
%% @end
%%
stop() ->
  unlink(?MODULE),
  gen_server2:call(?MODULE, stop).

%%
%% @spec info() -> {ok, PartitionsInfo}
%% @doc Get server info
%% PartitionInfo -> {RecordsCount, Size} | down
%% @end
%%
info() ->
  Schema = schema:get(),
  lists:map(
    fun(P) ->
      RunningNodes = lib_misc:multiply(P#partition.nodes, nodes([this, visible])),
      [
        {key, P#partition.key},
        {bits, P#partition.key_bits},
        {info, info(0, P#partition.nodes, P#partition.nodes, {0, 0})},
        {nodes, P#partition.nodes},
        {down_nodes, P#partition.nodes -- RunningNodes}
      ]
    end,
    Schema#schema.partitions
  ).

info(16, _Nodes, _AllNodes, {Records, Size}) ->
  {up, Records, Size};

info(Storage, [Node | Nodes], AllNodes, {Records, Size}) ->
  case storage:info({storage:storage(Storage), Node}) of
    {ok, StorageRecords, StorageSize} -> info(Storage + 1, AllNodes, AllNodes, {Records + StorageRecords, Size + StorageSize});
    _ -> info(Storage, Nodes, AllNodes, {Records, Size})
  end;

info(_Storage, [], _AllNodes, _) ->
  down.

%%
%% @spec join(Node) -> ok
%% @doc Join node
%% @end
%%
join(Node) ->
  global:trans({{motiondb, schema}, self()},
    fun() ->
      gossip_with_all(),
      Schema = schema:get(),
      SortedPartitions = lists:sort(
        fun(#partition{nodes=Left}, #partition{nodes=Right}) -> length(Left) < length(Right) end,
        lists:filter(
          fun(Partition) ->
            Partition#partition.right_partition == none andalso Partition#partition.joining_node == none
          end,
          Schema#schema.partitions
        )
      ),
      case SortedPartitions of
        [Partition | _] when length(Partition#partition.nodes) < 3 -> join(Node, Partition);
        _ -> new_partition([Node])
      end
    end
  ).

join(Node, Partition) ->
  Mult = lib_misc:multiply([Node], all_nodes()),
  if length(Mult) > 0 -> {error, already_in_schema, Mult};
  true ->
    case (catch gen_server2:call({?MODULE, Node}, ping)) of
      pong ->
        case (catch gen_server2:call({?MODULE, Node}, prepare_to_join)) of
          ok ->
            % join partition
            Schema = schema:get(),
            Partitions = lists:keyreplace(
              Partition#partition.key, 2,
              Schema#schema.partitions,
              Partition#partition{joining_node=Node}
            ),
            schema:set(Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}),
            global:sync(),
            gossip_with_all(),
            ok;
          false -> {error, prepare_failed}
        end;
      _ -> {error, node_down, Node}
    end
  end.

%%
%% @spec remove_node(Node) -> ok | {error, node_not_found}
%% @doc Remove node
%% @end
%%
remove_node(Node) ->
  global:trans({{motiondb, schema}, self()},
    fun() ->
      gossip_with_all(),
      Schema = schema:get(),
      case node_partition(Schema#schema.partitions, Node) of
        {ok, Partition} when length(Partition#partition.nodes) == 0 ->
          % remove partition
          remove_partition(Partition#partition.key);
        {ok, Partition} when Partition#partition.joining_node /= Node ->
          % remove node from partition
          Partitions = lists:keyreplace(
            Partition#partition.key, 2,
            Schema#schema.partitions,
            Partition#partition{nodes=lists:delete(Node, Partition#partition.nodes)}
          ),
          schema:set(Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}),
          global:sync(),
          gossip_with_all(),
          ok;
        _ -> {error, node_not_found}
      end
    end
  ).

%%
%% @spec new_partition([Nodes]) -> ok
%% @doc Create new partition
%% @end
%%
new_partition(Nodes) ->
  global:trans({{motiondb, schema}, self()},
    fun() ->
      gossip_with_all(),
      Schema = schema:get(),
      SortedPartitions = lists:sort(
        fun(#partition{key_bits=Left}, #partition{key_bits=Right}) -> Left < Right end,
        lists:filter(
          fun(Partition) ->
            Partition#partition.right_partition == none andalso Partition#partition.joining_node == none
          end,
          Schema#schema.partitions
        )
      ),
      case new_partition(SortedPartitions, Schema, Nodes) of
        {ok, NewSchema} ->
          schema:set(NewSchema),
          global:sync(),
          gossip_with_all(),
          ok;
        Res -> Res
      end
    end
  ).

new_partition(Partitions, Schema, Nodes) ->
  case length(lists:usort(Nodes)) == length(Nodes) of
    true ->
      Mult = lib_misc:multiply(Nodes, all_nodes()),
      if length(Mult) > 0 -> {error, already_in_schema, Mult};
      true ->
        case lists:filter(fun(Node) -> (catch gen_server2:call({?MODULE, Node}, ping)) /= pong end, Nodes) of
          [] ->
            case lists:all(fun(Node) -> (catch gen_server2:call({?MODULE, Node}, prepare_to_join)) == ok end, Nodes) of
              true -> create_partition(Partitions, Schema, Nodes);
              false -> {error, prepare_failed}
            end;
          DownNodes -> {error, node_down, DownNodes}
        end
      end;
    false -> {error, node_duplication}
  end.

create_partition([Partition | _], Schema, Nodes) ->
  RightPartition = #partition{
    key = Partition#partition.key + (1 bsl Partition#partition.key_bits),
    key_bits = Partition#partition.key_bits + 1,
    nodes = Nodes
  },
  Partitions = lists:keyreplace(
    Partition#partition.key, 2,
    Schema#schema.partitions,
    Partition#partition{right_operation = new_partition, right_partition = RightPartition}
  ),
  {ok, Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}};

create_partition([], Schema, Nodes) when length(Schema#schema.partitions) == 0 ->
  Partitions = [#partition{key=0, key_bits=0, nodes=Nodes}],
  {ok, Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}};

create_partition([], _Schema, _Nodes) ->
  {error, locked}.

%%
%% @spec remove_partition(Key) -> ok | {error, locked} | {error, partition_not_found}
%% @doc Remove partition
%% @end
%%
remove_partition(Key) ->
  global:trans({{motiondb, schema}, self()},
    fun() ->
      gossip_with_all(),
      Schema = schema:get(),
      case lists:keysearch(Key, 2, Schema#schema.partitions) of
        {value, Partition} when Partition#partition.right_partition == none ->
          LeftKey = Key band (1 bsl (Partition#partition.key_bits - 1) - 1),
          % find left partition
          case lists:keysearch(LeftKey, 2, Schema#schema.partitions) of
            {value, LeftPartition} when LeftPartition#partition.right_partition == none ->
              merge_partitions(Schema, LeftPartition, Partition);
            {value, _} -> {error, locked};
            _ -> {error, partition_not_found}
          end;
        {value, _} -> {error, locked};
        _ -> {error, partition_not_found}
      end
    end
  ).

merge_partitions(Schema, LeftPartition, RightPartition) ->
  Partitions = lists:keydelete(
    RightPartition#partition.key, 2,
    lists:keyreplace(
      LeftPartition#partition.key, 2,
      Schema#schema.partitions,
      LeftPartition#partition{right_operation = remove_partition, right_partition = RightPartition}
    )
  ),
  NewSchema = Schema#schema{partitions=Partitions, version=Schema#schema.version + 1},
  schema:set(NewSchema),
  global:sync(),
  gossip_with_all(),
  ok.  

%%
%% @spec gossip_with(Node) -> ok | sync_back | node_down
%% @doc Gossip with node
%% @end
%%
gossip_with(Node) ->
  Schema = schema:get(),
  case (catch gen_server2:call({?MODULE, Node}, {sync_schema, Schema})) of
    {ok, _} -> ok;
    {invalid_version, RemoteSchema} ->
      gen_server2:call(?MODULE, {sync_schema, RemoteSchema}),
      sync_back;
    _ -> node_down
  end.

gossip_with([Node | Nodes], AllNodes) ->
  case gossip_with(Node) of
    sync_back -> gossip_with(AllNodes, AllNodes);
    _ -> gossip_with(Nodes, AllNodes)
  end;

gossip_with([], _AllNodes) -> ok.

%%
%% @spec gossip_with_all() -> ok
%% @doc Gossip with all remote node
%% @end
%%
gossip_with_all() ->
  AllNodes = all_remote_nodes(),
  gossip_with(AllNodes, AllNodes),
  Schema = schema:get(),
  if Schema#schema.state == down andalso length(Schema#schema.partitions) > 0 ->
    change_state(up),
    gossip_with_all();
  true -> ok
  end.
    


%%
%% @spec init() -> {ok, Schema}
%% @doc Initialize schema
%% @end
%%
init() ->
  ets:new(motiondb, [public, set, named_table]),
  Schema = case file:read_file(lib_misc:root(["database", atom_to_list(node()), "schema.bin"])) of
    {ok, Content} -> binary_to_term(Content);
    {error, enoent} -> #schema{version=0, partitions=[]}
  end,
  ets:insert(motiondb, Schema#schema{state=down}),
  {ok, Schema}.

%%
%% @spec destroy() -> ok
%% @doc Remove schema
%% @end
%%
destroy() ->
  file:delete(lib_misc:root(["database", atom_to_list(node()), "schema.bin"])),
  case ets:info(motiondb) of
    undefined -> ok;
    _ ->
      ets:delete(motiondb),
      ok
  end.

%%
%% @spec get() -> Schema
%% @doc Get schema
%% @end
%%
get() ->
  [Schema] = ets:lookup(motiondb, schema),
  Schema.

%%
%% @spec set(Schema) -> {ok, Schema} | {invalid_version, LocalSchema}
%% @doc Set schema
%% @end
%%
set(Schema) ->
  BeforePartition = current_partition(),
  Result = global:trans({{motiondb, schema}, self()},
    fun() ->
      case schema:get() of
        #schema{version=Version} when Schema#schema.version > Version ->
          ets:insert(motiondb, Schema),
          file:write_file(lib_misc:root(["database", atom_to_list(node()), "schema.bin"]), term_to_binary(Schema)),
          {ok, Schema};
        #schema{version=Version} when Schema#schema.version == Version ->
          {unchanged, Schema};
        LocalSchema -> {invalid_version, LocalSchema}
      end
    end
  ),
  case Result of
    {ok, _} ->
      case current_partition() of
        {ok, Partition} when {ok, Partition} /= BeforePartition ->
          [spawn_link(fun() -> storage:set_nodes(storage:storage(I), Partition#partition.nodes) end) || I <- lists:seq(0, 15)];
        _ -> ok
      end;
    _ -> ok
  end,
  Result.

%%
%% @spec change_state(State) -> ok
%% @doc Change state
%% @end
%%
change_state(State) ->
  global:trans({{motiondb, schema}, self()},
    fun() ->
      Schema = schema:get(),
      ets:insert(motiondb, Schema#schema{state=State})
    end
  ).

%%
%% @spec current_partition() -> {ok, Partition} | {error, not_found}
%% @doc Get current partition
%% @end
%%
current_partition() ->
  node_partition(node()).

%%
%% @spec node_partition(Node) -> {ok, Partition} | {error, not_found}
%% @doc Get node partition
%% @end
%%
node_partition(Node) ->
  Schema = schema:get(),
  node_partition(Schema#schema.partitions, Node).

node_partition([Partition | Partitions], Node) ->
  case lists:any(fun(N) -> Node == N end, Partition#partition.nodes) of
    true -> {ok, Partition};
    false ->
      case Partition#partition.right_partition of
        none -> node_partition(Partitions, Node);
        RightPartition ->
          case lists:any(fun(N) -> Node == N end, RightPartition#partition.nodes) of
            true -> {ok, RightPartition};
            false -> node_partition(Partitions, Node)
          end
      end
  end;

node_partition([], _) ->
  {error, not_found}.

%%
%% @spec all_nodes() -> Nodes
%% @doc Get all nodes list
%% @end
%%
all_nodes() ->
  Schema = schema:get(),
  lists:flatten(lists:map(
    fun(Partition) ->
      Partition#partition.nodes ++
      case Partition#partition.right_partition of
        none -> [];
        RightPartition -> RightPartition#partition.nodes
      end
    end,
    Schema#schema.partitions
  )).

%%
%% @spec all_remote_nodes() -> Nodes
%% @doc Get all nodes list exclude local node
%% @end
%%
all_remote_nodes() ->
  lists:delete(node(), all_nodes()).

%%
%% @spec partition_for_hash(Hash, Partitions) -> {ok, Partition} | {error, not_found}
%% @doc Get hash partition
%% @end
%%
partition_for_hash(Hash, [Partition | Partitions]) ->
  if (Hash band ((1 bsl Partition#partition.key_bits) - 1)) == Partition#partition.key ->
    {ok, Partition};
    true -> partition_for_hash(Hash, Partitions)
  end;

partition_for_hash(_, []) ->
  {error, not_found}.


%% ================================================================================================
%% Genserver callback functions
%% ================================================================================================
init([]) ->
  {ok, #state{gossip=start_gossip()}}.

handle_call(info, _From, State) ->
  {reply, {ok}, State};

handle_call({sync_schema, Schema}, _From, State) ->
  Result = schema:set(Schema),
  case Result of
    _ when Schema#schema.state == up ->
      LocalSchema = schema:get(),
      if LocalSchema#schema.state == down ->
        change_state(up);
      true -> ok
      end;
    _ -> ok
  end,
  {reply, Result, State};

handle_call(prepare_to_join, _From, State = #state{gossip=Gossip}) ->
  stop_gossip(Gossip),
  schema:destroy(),
  timer:sleep(2),
  lists:foreach(fun(I) -> storage:vanish(storage:storage(I)) end, lists:seq(0, 15)),
  schema:init(),
  {reply, ok, State#state{gossip=start_gossip()}};

handle_call(ping, _From, State) ->
  {reply, pong, State};

handle_call(stop, _From, State) ->
  {stop, shutdown, stopped, State};

handle_call(_Msg, _From, State) ->
  {reply, {error, invalid_message}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{gossip=Gossip}) ->
  stop_gossip(Gossip),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ================================================================================================
%% Gossip
%% ================================================================================================
start_gossip() ->
  Pid = spawn_link(fun() -> gossip_loop() end),
  register(gossip, Pid),
  Pid.

stop_gossip(Pid) ->
  Pid ! stop.

gossip_loop() ->
  Nodes = all_remote_nodes(),
  if length(Nodes) > 0 ->
    Node = lists:nth(random:uniform(length(Nodes)), Nodes),
    gossip_with(Node);
  true -> ok
  end,
  balancer(),
  receive
    hold -> receive start -> gossip_loop(); stop -> ok end;
    stop -> ok
  after random:uniform(5000) + 5000 -> gossip_loop()
  end.

%% ================================================================================================
%% Balancer
%% ================================================================================================
balancer() ->
  case whereis(balancer) of
    undefined ->
      Schema = schema:get(),
      if Schema#schema.state == up ->
        case lists:filter(fun(Partition) -> Partition#partition.right_partition /= none orelse Partition#partition.joining_node /= none end, Schema#schema.partitions) of
          [] -> ok;
          Partitions ->
            Lock = {{motiondb, balancer}, self()},
            case global:set_lock(Lock, all_nodes(), 0) of
              true -> register(balancer, spawn_link(fun() -> balancer(Partitions), global:del_lock(Lock) end));
              false -> ok
            end
        end;
      true -> ok
    end;
    _ -> ok
  end.

balancer(Partitions) ->
  Parent = self(),
  Pids = [
    balancer_start(
      Parent,
      case Partition#partition.right_operation of
        remove_partition ->
          % run balancer on right partition nodes on remove
          RightPartition = Partition#partition.right_partition,
          lib_misc:multiply(RightPartition#partition.nodes, nodes([this, visible]));
        _ ->
          % run balancer on left partition nodes on join
          lib_misc:multiply(Partition#partition.nodes, nodes([this, visible]))
      end,
      Partition
    ) || Partition <- Partitions
  ],
  [receive {Pid, done} -> ok end || Pid <- Pids].

balancer_start(Parent, [Node | Nodes], Partition) ->
  case lists:all(fun(I) -> storage:is_up({storage:storage(I), Node}) == true end, lists:seq(0, 15)) of
    true -> spawn_link(Node, fun() -> balancer(Parent, Partition) end);
    false -> balancer_start(Parent, Nodes, Partition)
  end;

balancer_start(Parent, [], _Partition) ->
  Parent ! done.

balancer(Parent, Partition = #partition{right_partition=RightPartition}) when Partition#partition.right_operation == new_partition ->
  % migrate partition (running on left partition nodes)
  Result = lists:all(
    fun(I) ->
      RightPartition = Partition#partition.right_partition,
      storage:migrate(
        storage:storage(I),
        RightPartition#partition.nodes,
        RightPartition#partition.key,
        RightPartition#partition.key_bits
      ) == {ok, done}
    end,
    lists:seq(0, 15)
  ),
  % done migrating
  case Result of
    true ->
      global:trans({motiondb, schema},
        fun() ->
          gossip_with_all(),
          Schema = schema:get(),
          Partitions = lists:keyreplace(
            Partition#partition.key, 2,
            Schema#schema.partitions,
            Partition#partition{
              key_bits = Partition#partition.key_bits + 1,
              right_operation = none,
              right_partition = none
            }
          ) ++ [Partition#partition.right_partition],
          schema:set(Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}),
          gossip_with_all()
        end
      );
    _ -> ok
  end,
  Parent ! {self(), done};

balancer(Parent, Partition) when Partition#partition.right_operation == remove_partition ->
  % remove partition (running on right partition nodes)
  Result = lists:all(
    fun(I) ->
      RightPartition = Partition#partition.right_partition,
      storage:migrate_without_truncate(
        storage:storage(I),
        Partition#partition.nodes,
        RightPartition#partition.key,
        RightPartition#partition.key_bits
      ) == {ok, done}
    end,
    lists:seq(0, 15)
  ),
  % done migrating
  case Result of
    true ->
      global:trans({motiondb, schema},
        fun() ->
          gossip_with_all(),
          Schema = schema:get(),
          io:format("Removed...~p~n", [Schema]),
          Partitions = lists:keyreplace(
            Partition#partition.key, 2,
            Schema#schema.partitions,
            Partition#partition{
              key_bits = Partition#partition.key_bits - 1,
              right_operation = none,
              right_partition = none
            }
          ),
          schema:set(Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}),
          gossip_with_all()
        end
      );
    _ -> ok
  end,
  Parent ! {self(), done};

balancer(Parent, Partition) when Partition#partition.joining_node /= none ->
  % join partition
  Result = lists:all(
    fun(I) ->
      storage:join_node(storage:storage(I), Partition#partition.joining_node) == {ok, done}
    end,
    lists:seq(0, 15)
  ),
  % done migrating
  case Result of
    true ->
      global:trans({motiondb, schema},
        fun() ->
          gossip_with_all(),
          Schema = schema:get(),
          Partitions = lists:keyreplace(
            Partition#partition.key, 2,
            Schema#schema.partitions,
            Partition#partition{
              nodes = [Partition#partition.joining_node | Partition#partition.nodes],
              joining_node = none
            }
          ),
          schema:set(Schema#schema{partitions=Partitions, version=Schema#schema.version + 1}),
          gossip_with_all()
        end
      );
    _ -> ok
  end,
  Parent ! {self(), done};

balancer(Parent, _Partition) ->
  Parent ! {self(), done}.

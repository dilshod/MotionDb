-module(storage).
-include("motiondb.hrl").
-behaviour(gen_server2).

-export([start_link/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([sync_schema/1, sync/2, truncate/1, vanish/1, set_nodes/2, backup/1, load/3, join_node/2]).
-export([migrate/4, migrate_without_truncate/4]).
-export([storage/1, info/1, is_up/1]).
-export([get/2, getvalues/2, first/1, next/2]).
-export([put/3, putvalues/3, append/3, remove/2, remove/3]).

-define(SYNC_TIMEOUT, 5000).

-record(ulog, {file, position}).
-record(state, {name, nodes, storage, storage_instance, path, ulog, timeout, state}).

%%
%% @spec start_link(Storage, ServerName) -> Server
%% @doc Start server
%% @end
%%
start_link(Storage, ServerName) ->
  RemoteNodes = case schema:current_partition() of
    {ok, #partition{nodes=Nodes}} -> lists:delete(node(), Nodes);
    _ -> []
  end,
  Pid = gen_server2:start_link({local, ServerName}, ?MODULE, [Storage, ServerName, RemoteNodes], []),
  sync(ServerName, RemoteNodes),
  Pid.

%%
%% @spec stop(Server) -> ok
%% @doc
%% @end
%%
stop(Server) ->
  unlink(Server),
  gen_server2:call(Server, stop).

%%
%% @spec storage(Num) -> ServerName
%% @doc Get storage's ServerName by number
%% @end
%%
storage(Num) ->
  list_to_atom("storage" ++ integer_to_list(Num)).

%%
%% @spec sync_schema(Server) -> ok
%% @doc
%% @end
%%
sync_schema(Server) ->
  gen_server2:call(Server, sync_schema).

%%
%% @spec sync(Server, RemoteNodes) -> ok
%% @doc Sync with global
%% @end
%%
sync(Server, RemoteNodes) ->
  sync_global(Server, RemoteNodes).

%%
%% @spec info(Server) -> {ok, Records, Size} | {error, node_down}
%% @doc Get records count and storage size
%% @end
%%
info(Server) ->
  case (catch gen_server2:call(Server, info)) of
    {ok, Records, Size} -> {ok, Records, Size};
    _ -> {error, node_down}
  end.

%%
%% @spec truncate(Server) -> {ok, Records, Size}
%% @doc Truncate storage
%% @end
%%
truncate(Server) ->
  gen_server2:call(Server, truncate).

%%
%% @spec vanish(Server) -> ok
%% @doc Truncate storage, forget linked nodes, and turn down
%% @end
%%
vanish(Server) ->
  gen_server2:call(Server, truncate),
  gen_server2:call(Server, {set_nodes, []}),
  gen_server2:call(Server, turn_down).

%%
%% @spec set_nodes(Server, Nodes) -> ok
%% @doc Set nodes
%% @end
%%
set_nodes(Server, Nodes) ->
  gen_server2:call(Server, {set_nodes, Nodes}),
  sync(Server, Nodes).

%%
%% @spec get(Server, Key) -> {ok, Value} | {error, not_found} | {error, node_down}
%% @doc
%% @end
%%
get(Server, Key) ->
  gen_server2:call(Server, {get, Key}).

%%
%% @spec getvalues(Server, Key) -> {ok, Values} | {error, node_down}
%% @doc
%% @end
%%
getvalues(Server, Key) ->
  gen_server2:call(Server, {getvalues, Key}).

%%
%% @spec first(Server) -> {ok, Key, Value} | {error, not_found} | {error, node_down}
%% @doc
%% @end
%%
first(Server) ->
  Result = gen_server2:call(Server, first),
  case Result of
    {ok, {'__motiondb__', _}} -> next(Server, Result);
    _ -> Result
  end.

%%
%% @spec next(Server, Key) -> {ok, Key, Value} | {error, not_found} | {error, node_down}
%% @doc
%% @end
%%
next(Server, Key) ->
  Result = gen_server2:call(Server, {next, Key}),
  case Result of
    {ok, {'__motiondb__', _}} -> next(Server, Result);
    _ -> Result
  end.

%%
%% @spec put(Server, Key, Value) -> ok | failed
%% @doc
%% @end
%%
put(Server, Key, Value) ->
  gen_server2:call(Server, {put, Key, Value}).

%%
%% @spec putvalues(Server, Key, Values) -> ok | failed
%% @doc
%% @end
%%
putvalues(Server, Key, Values) ->
  gen_server2:call(Server, {putvalues, Key, Values}).

%%
%% @spec append(Server, Key, Value) -> ok | failed
%% @doc
%% @end
%%
append(Server, Key, Value) ->
  gen_server2:call(Server, {append, Key, Value}).

%%
%% @spec remove(Server, Key) -> ok | failed
%% @doc
%% @end
%%
remove(Server, Key) ->
  gen_server2:call(Server, {remove, Key}).

%%
%% @spec remove(Server, Key, Value) -> ok | failed
%% @doc
%% @end
%%
remove(Server, Key, Value) ->
  gen_server2:call(Server, {remove, Key, Value}).

%%
%% @spec backup(Server) -> {ok, Filename, Position} | {error, node_down}
%% @doc Backup storage
%% @end
%%
backup(Server) ->
  gen_server2:call(Server, backup).

%%
%% @spec load(Server, Filename, Position) -> ok
%% @doc
%% @end
%%
load(Server, Filename, Position) ->
  gen_server2:call(Server, {load, Filename, Position}).

%%
%% @spec is_up(Server) -> true | false
%% @doc
%% @end
%%
is_up(Server) ->
  case (catch gen_server2:call(Server, is_up)) of
    {ok, Result} -> Result;
    _ -> node_down
  end.

%%
%% @spec migrate(Server, IntoNodes, Key, KeyBits) -> ok | {error, node_down}
%% @doc Migrates part of items into new partition
%% @end
%%
migrate(ServerName, IntoNodes, Key, KeyBits) ->
  case lists:all(fun(Node) -> (catch storage:truncate({ServerName, Node})) == ok end, IntoNodes) of
    true -> migrate_without_truncate(ServerName, IntoNodes, Key, KeyBits);
    false -> {error, node_down}
  end.

migrate_without_truncate(ServerName, IntoNodes, Key, KeyBits) ->
  migrate(ServerName, lists:sort(IntoNodes), Key, KeyBits, first(ServerName), []).

migrate(ServerName, _IntoNodes, _Key, _KeyBits, {error, not_found}, KeysToRemove) ->
  lists:foreach(fun(Key) -> remove(ServerName, Key) end, KeysToRemove),
  {ok, done};

migrate(ServerName, IntoNodes, Key, KeyBits, _, KeysToRemove) when length(KeysToRemove) > 1000000 ->
  lists:foreach(fun(K) -> remove(ServerName, K) end, KeysToRemove),
  migrate(ServerName, IntoNodes, Key, KeyBits, first(ServerName), []);

migrate(ServerName, IntoNodes, Key, KeyBits, {ok, FoundKey}, KeysToRemove) ->
  %io:format("Testing from: ~p, into: ~p, key: ~p, ~n", [node(), IntoNodes, FoundKey]),
  Hash = erlang:phash2(FoundKey, (1 bsl 32) - 1),
  if (Hash band ((1 bsl KeyBits) - 1)) == Key ->
    %io:format("Migrating~n"),
    {ok, Values} = getvalues(ServerName, FoundKey),
    case migrate_item(ServerName, IntoNodes, FoundKey, Values) of
      ok -> migrate(ServerName, IntoNodes, Key, KeyBits, next(ServerName, FoundKey), [FoundKey | KeysToRemove]);
      Err -> Err
    end;
  true ->
    migrate(ServerName, IntoNodes, Key, KeyBits, next(ServerName, FoundKey), KeysToRemove)
  end.

migrate_item(ServerName, [Node | Nodes], Key, Values) ->
  case putvalues({ServerName, Node}, Key, Values) of
    ok -> ok;
    _ -> migrate_item(ServerName, Nodes, Key, Values)
  end;

migrate_item(_ServerName, [], _Key, _Values) ->
  {error, node_down}.

%%
%% @spec join_node(Server, Node) -> ok | {error, node_down} | {error, Reason}
%% @doc Join new node into partition
%% @end
%%
join_node(ServerName, Node) ->
  case {is_up(ServerName), is_up({ServerName, Node})} of
    {_, node_down} -> {error, node_down};
    {true, _} ->
      {ok, Filename, Position} = backup(ServerName),
      case lib_misc:send_file(Node, Filename) of
        {ok, SentFilename} ->
          file:delete(Filename),
          load({ServerName, Node}, SentFilename, Position),
          {ok, done};
        Result ->
          file:delete(Filename),
          Result
      end;
    _ -> {error, node_down}
  end.


%% ================================================================================================
%% Genserver callback functions
%% ================================================================================================
init([Storage, ServerName, RemoteNodes]) ->
  Path = lib_misc:root(["database", atom_to_list(node()), atom_to_list(ServerName)]),
  file:make_dir(Path),
  case Storage:start(Path) of
    {ok, StorageInstance} ->
      ULog = ulog_init(Path),
      sync_local(ServerName, Path, Storage, StorageInstance, ULog#ulog.position),
      {ok, #state{
        name=ServerName, path=Path, ulog=ULog, storage=Storage, storage_instance=StorageInstance,
        nodes=RemoteNodes, timeout=infinity, state=down
      }};
    {error, Msg} ->
      {stop, Msg}
  end.

handle_call(info, _From, State = #state{storage=Storage, storage_instance=StorageInstance, state=up}) ->
  Result = case Storage:info(StorageInstance) of
    {ok, 0, Size} -> {ok, 0, Size};
    {ok, Records, Size} -> {ok, Records - 1, Size};
    Res -> Res
  end,
  {reply, Result, State, State#state.timeout};

handle_call(info, _From, State) ->
  {reply, {error, node_down}, State, State#state.timeout};

handle_call(truncate, _From, State) ->
  {reply, ok, State#state{ulog=ulog_truncate(0, State)}};

handle_call({get, Key}, _From, State = #state{storage=Storage, storage_instance=StorageInstance, state=up}) ->
  {reply, Storage:get(StorageInstance, Key), State, State#state.timeout};

handle_call({get, _Key}, _From, State) ->
  {reply, {error, node_down}, State, State#state.timeout};

handle_call({getvalues, Key}, _From, State = #state{storage=Storage, storage_instance=StorageInstance, state=up}) ->
  {reply, Storage:getvalues(StorageInstance, Key), State, State#state.timeout};

handle_call({getvalues, _Key}, _From, State) ->
  {reply, {error, node_down}, State, State#state.timeout};

handle_call(first, _From, State = #state{storage=Storage, storage_instance=StorageInstance, state=up}) ->
  {reply, Storage:first(StorageInstance), State, State#state.timeout};

handle_call(first, _From, State) ->
  {reply, {error, node_down}, State, State#state.timeout};

handle_call({next, Key}, _From, State = #state{storage=Storage, storage_instance=StorageInstance, state=up}) ->
  {reply, Storage:next(StorageInstance, Key), State, State#state.timeout};

handle_call({next, _Key}, _From, State) ->
  {reply, {error, node_down}, State, State#state.timeout};

handle_call(Event = {put, _Key, _Value}, _From, State) ->
  event_call(Event, State);

handle_call(Event = {append, _Key, _Value}, _From, State) ->
  event_call(Event, State);

handle_call(Event = {putvalues, _Key, _Values}, _From, State) ->
  event_call(Event, State);

handle_call(Event = {remove, _Key}, _From, State) ->
  event_call(Event, State);

handle_call(Event = {remove, _Key, _Value}, _From, State) ->
  event_call(Event, State);

handle_call({event, Position, BinaryEvent}, _From, State = #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance}) ->
  if ULog#ulog.position == Position ->
    NewULog = ulog_append(ULog, BinaryEvent, State),
    event_int(BinaryEvent, Storage, StorageInstance, NewULog#ulog.position),
    {reply, ok, State#state{ulog=NewULog, timeout=?SYNC_TIMEOUT}, ?SYNC_TIMEOUT};
  true ->
    if ULog#ulog.position < Position -> {reply, {error, inconsistent}, State, State#state.timeout};
    true -> {reply, ok, State, State#state.timeout}
    end
  end;

handle_call(backup, _From, State = #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance, path=Path, state=up}) ->
  Filename = filename:join([Path, "backup" ++ integer_to_list(ULog#ulog.position) ++ ".tc"]),
  Storage:sync(StorageInstance),
  case Storage:backup(StorageInstance, Filename) of
    ok -> {reply, {ok, Filename, ULog#ulog.position}, State, State#state.timeout};
    Err -> {reply, {error, Err}, State, State#state.timeout}
  end;

handle_call(backup, _From, State) ->
  {reply, {error, node_down}, State, State#state.timeout};

handle_call({load, Filename, Position}, _From, State = #state{path=Path, storage=Storage, storage_instance=StorageInstance}) ->
  Storage:close(StorageInstance),
  case file:rename(lib_misc:root(["database", Filename]), filename:join([Path, "db.tc"])) of
    ok ->
      case Storage:start(Path) of
        {ok, NewStorageInstance} ->
          {reply, ok, State#state{ulog=ulog_truncate(Position, State), storage_instance=NewStorageInstance, state=down}};
        Result ->
          {stop, Result}
      end;
    _ -> {error, file_not_found}
  end;

handle_call(turn_up, _From, State) ->
  {reply, ok, State#state{state=up}, State#state.timeout};

handle_call(turn_down, _From, State) ->
  {reply, ok, State#state{state=down}, State#state.timeout};

handle_call(is_up, _From, State) ->
  {reply, {ok, State#state.state == up}, State, State#state.timeout};

handle_call(log_position, _From, State = #state{ulog=ULog}) ->
  {reply, {ok, ULog#ulog.position}, State, State#state.timeout};

handle_call({get_events, Position}, _From, State) ->
  {reply, ulog_get_events(Position, State), State, State#state.timeout};

handle_call(sync, _From, State) ->
  {reply, ok, State#state{timeout=0}, 0};

handle_call({set_nodes, Nodes}, _From, State) ->
  {reply, ok, State#state{nodes=lists:delete(node(), Nodes)}, State#state.timeout};

handle_call(stop, _From, State) ->
  {stop, shutdown, stopped, State};

handle_call(_Msg, _From, State) ->
  {reply, {error, invalid_message}, State, State#state.timeout}.

handle_cast({event, Position, BinaryEvent}, State = #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance}) ->
  if ULog#ulog.position == Position ->
    NewULog = ulog_append(ULog, BinaryEvent, State),
    event_int(BinaryEvent, Storage, StorageInstance, NewULog#ulog.position),
    {noreply, State#state{ulog=NewULog, timeout=?SYNC_TIMEOUT}, ?SYNC_TIMEOUT};
  true -> {noreply, State, State#state.timeout}
  end;

handle_cast(_Msg, State) ->
  {noreply, State, State#state.timeout}.

handle_info(timeout, State = #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance}) ->
  spawn(
    fun() ->
      file:sync(ULog#ulog.file),
      Storage:sync(StorageInstance)
    end
  ),
  {noreply, State#state{timeout=infinity}};

handle_info(_Info, State) ->
  {noreply, State, State#state.timeout}.

terminate(_Reason, #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance}) ->
  file:close(ULog#ulog.file),
  Storage:close(StorageInstance),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State, State#state.timeout}.


%% ================================================================================================
%% Event functions
%% ================================================================================================
event_call(Event, State = #state{ulog=ULog, nodes=[]}) ->
  handle_call({event, ULog#ulog.position, term_to_binary(Event)}, self(), State);

event_call(Event, State = #state{ulog=ULog, nodes=Nodes, name=ServerName}) ->
  Position = ULog#ulog.position,
  BinaryEvent = term_to_binary(Event),
  %{Goods, _Bads} = gen_server2:multi_call(
  %  lib_misc:multiply(Nodes, nodes([visible])), ServerName, {event, Position, BinaryEvent}
  %),
  %case lists:any(fun(Result) -> case Result of {_, ok} -> true; _ -> false end end, Goods) of
  %  true -> handle_call({event, Position, BinaryEvent}, self(), State);
  %  false -> {reply, failed, State, State#state.timeout}
  %end.
  gen_server2:abcast(
    lib_misc:multiply(Nodes, nodes([visible])), ServerName, {event, Position, BinaryEvent}
  ),
  handle_call({event, Position, BinaryEvent}, self(), State).

event_int(BinaryEvent, Storage, StorageInstance, Position) ->
  [Fun | Args] = tuple_to_list(binary_to_term(BinaryEvent)),
  apply(Storage, Fun, [StorageInstance | Args]),
  Storage:put(StorageInstance, {'__motiondb__', last_position}, Position).


%% ================================================================================================
%% ULog functions
%% ================================================================================================
ulog_init(Path) ->
  {ok, Files} = file:list_dir(Path),
  Num = lists:max(lists:map(
    fun(F) ->
      case lists:suffix(".ulog", F) of
        true -> list_to_integer(lists:sublist(F, length(F) - 5));
        _ -> 0
      end
    end,
    ["" | Files]
  )),
  FileName = filename:join([Path, integer_to_list(Num) ++ ".ulog"]),
  {ok, File} = file:open(FileName, [read, write, binary]),
  Pos = filelib:file_size(FileName),
  file:position(File, Pos),
  #ulog{file = File, position = Pos + (Num bsl 24)}.

ulog_append(ULog = #ulog{file=File, position=Position}, BinaryEvent, #state{path=Path}) ->
  file:write(File, <<(size(BinaryEvent)):32, BinaryEvent/binary>>),

  if (Position + size(BinaryEvent) + 4) bsr 24 =/= Position bsr 24 ->
    file:close(File),
    % TODO: remove old ulog files (call ulog_remove_old(Path, Position) when all partition nodes is up)
    {ok, NewFile} = file:open(
      filename:join([Path, integer_to_list((Position bsr 24) + 1) ++ ".ulog"]), [read, write, binary]
    ),
    #ulog{file = NewFile, position = ((Position bsr 24) + 1) bsl 24};
  true ->
    ULog#ulog{position = Position + size(BinaryEvent) + 4}
  end.

ulog_truncate(Position, #state{ulog=ULog, path=Path, storage=Storage, storage_instance=StorageInstance}) ->
  Pos = (Position band (1 bsl 24 - 1)),
  file:close(ULog#ulog.file),
  ulog_remove_old(Path, Position + (1 bsl 24)),
  {ok, NewFile} = file:open(
    filename:join([Path, integer_to_list(Position bsr 24) ++ ".ulog"]), [read, write, binary]
  ),
  file:truncate(NewFile),
  lists:foreach(fun(_) -> file:write(NewFile, <<0:8>>) end, lists:seq(1, Pos)),
  file:position(NewFile, Pos),
  % truncate storage
  Storage:truncate(StorageInstance),
  Storage:put(StorageInstance, {'__motiondb__', last_position}, Position),
  Storage:sync(StorageInstance),
  #ulog{file=NewFile, position=Position}.

ulog_remove_old(Path, Position) ->
  lists:foreach(
    fun(I) -> file:delete(filename:join([Path, integer_to_list(I) ++ ".ulog"])) end,
    lists:seq(0, (Position bsr 24) - 1)
  ).

ulog_get_events(Position, #state{path=Path}) ->
  Num = Position bsr 24,
  Pos = (Position band (1 bsl 24 - 1)),
  FileName = filename:join([Path, integer_to_list(Num) ++ ".ulog"]),
  Size = filelib:file_size(FileName),
  if Size > Pos ->
    case file:open(FileName, [read, binary]) of
      {ok, File} ->
        file:position(File, Pos),
        {ok, Data} = file:read(File, Size - Pos),
        file:close(File),
        {ok, Data};
      _ -> no_events
    end;
  true -> no_events
  end.


%% ================================================================================================
%% Local sync
%% ================================================================================================
sync_local(ServerName, Path, Storage, StorageInstance, Position) ->
  case Storage:get(StorageInstance, {'__motiondb__', last_position}) of
    {ok, LastPosition} when LastPosition < Position ->
      %io:format("Syncing ~p local from ~p to ~p ~n", [ServerName, LastPosition, Position]),
      case sync_local_read(Path, LastPosition) of
        {ok, Data} ->
          sync_local_events(Storage, StorageInstance, LastPosition, Data),
          sync_local(ServerName, Path, Storage, StorageInstance, Position);
        _ -> ok
      end;
    {error, not_found} when Position > 0 ->
      %io:format("Syncing ~p local from 0 to ~p ~n", [ServerName, Position]),
      case sync_local_read(Path, 0) of
        {ok, Data} ->
          sync_local_events(Storage, StorageInstance, 0, Data),
          sync_local(ServerName, Path, Storage, StorageInstance, Position);
        _ -> ok
      end;      
    _ ->
      %io:format("Local sync finished~n"),
      Storage:sync(StorageInstance)
  end.

sync_local_events(Storage, StorageInstance, Position, <<Size:32, Data/binary>>) ->
  <<BinaryEvent:Size/binary, Other/binary>> = Data,
  event_int(BinaryEvent, Storage, StorageInstance, Position + 4 + size(BinaryEvent)),
  sync_local_events(Storage, StorageInstance, Position + 4 + size(BinaryEvent), Other);

sync_local_events(_Storage, _StorageInstance, Position, <<>>) ->
  Position.

sync_local_read(Path, Position) ->
  Num = Position bsr 24,
  Pos = (Position band (1 bsl 24 - 1)),
  FileName = filename:join([Path, integer_to_list(Num) ++ ".ulog"]),
  Size = filelib:file_size(FileName),
  if Size > Pos ->
      case file:open(FileName, [read, binary]) of
        {ok, File} ->
          file:position(File, Pos),
          {ok, Data} = file:read(File, Size - Pos),
          file:close(File),
          {ok, Data};
        _ ->
          failed
      end;
    true ->
      no_events
  end.


%% ================================================================================================
%% Global sync
%% ================================================================================================
sync_global(ServerName, RemoteNodes) ->
  sync_global_int(ServerName, RemoteNodes),
  case is_up(ServerName) of
    true -> ok;
    false ->
      case lists:all(fun(Node) -> is_up({ServerName, Node}) /= node_down end, RemoteNodes) of
        true ->
          gen_server2:call(ServerName, turn_up),
          lists:foreach(
            fun(Node) ->
              spawn(Node, storage, sync, [ServerName, [node() | lists:delete(Node, RemoteNodes)]])
            end,
            RemoteNodes
          ),
          ok;
        _ -> failed
      end;
    _ -> failed
  end.

sync_global_int(ServerName, [RemoteNode | RemoteNodes]) ->
  case is_up({ServerName, RemoteNode}) of
    node_down -> sync_global_int(ServerName, RemoteNodes);
    IsUp ->
      %io:format("Sync started for node ~p~n", [RemoteNode]),
      case sync_global_with_server(ServerName, {ServerName, RemoteNode}) of
        ok when IsUp ->
          %io:format("Sync ok, db is up now~n"),
          ok = gen_server2:call(ServerName, turn_up);
        _ -> sync_global_int(ServerName, RemoteNodes)
      end
  end;

sync_global_int(_, []) ->
  failed.

sync_global_with_server(LocalServer, RemoteServer) ->
  {ok, Position} = gen_server2:call(LocalServer, log_position),
  case (catch gen_server2:call(RemoteServer, {get_events, Position})) of
    {ok, Data} ->
      %io:format("Syncyng global from ~p to ~p~n", [Position, Position + size(Data)]),
      ok = sync_global_events(LocalServer, Position, Data),
      sync_global_with_server(LocalServer, RemoteServer);
    no_events -> ok;
    {'EXIT', _P} -> failed
  end.

sync_global_events(LocalServer, Position, <<Size:32, Data/binary>>) ->
  <<Event:Size/binary, Other/binary>> = Data,
  ok = gen_server2:call(LocalServer, {event, Position, Event}),
  sync_global_events(LocalServer, Position + 4 + size(Event), Other);

sync_global_events(_LocalServer, _Position, <<>>) ->
  ok.

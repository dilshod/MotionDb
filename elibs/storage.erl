-module(storage).
-behaviour(gen_server2).

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, sync/2]).

-define(SYNC_TIMEOUT, 2000).

-record(ulog, {
  file,
  position
}).

-record(state, {
  name,
  storage,
  storage_instance,
  path,
  nodes,
  ulog,
  sync_timeout,
  state
}).


start_link(Storage, ServerName, Path, Nodes) ->
  RemoteNodes = lists:delete(node(), Nodes),
  Pid = gen_server2:start_link({local, ServerName}, ?MODULE, [ServerName, Storage, Path, RemoteNodes], []),
  sync(ServerName, RemoteNodes),
  turn_on(ServerName, RemoteNodes),
  Pid.

init([ServerName, Storage, Path, Nodes]) ->
  file:make_dir(Path),
  case Storage:start(Path) of
    {ok, StorageInstance} ->
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
      ULog = #ulog{
        file = File,
        position = Pos + (Num bsl 24)
      },

      local_sync(Path, Storage, StorageInstance, Pos + (Num bsl 24)),

      {ok, #state{
        name=ServerName, storage=Storage, storage_instance=StorageInstance, ulog=ULog, path=Path, nodes=Nodes,
        sync_timeout=infinity, state=down
      }};
    {error, Msg} ->
      {error, {error, Msg}}
  end.

handle_call(Event = {put, _Key, _Value, _AllowDuplication}, _From, State) ->
  call_event(Event, State);

handle_call(Event = {remove, _Key}, _From, State) ->
  call_event(Event, State);

handle_call(Event = {remove, _Key, _Value}, _From, State) ->
  call_event(Event, State);

handle_call({get, Key, WithDuplication}, _From, State = #state{storage=Storage, storage_instance=StorageInstance}) ->
  case State#state.state of
    up ->
      {reply, Storage:get(StorageInstance, Key, WithDuplication), State, State#state.sync_timeout};
    _ ->
      {reply, {error, nodedown}, State, State#state.sync_timeout}
  end;

handle_call({event, Position, BinaryEvent}, _From, State = #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance, path=Path}) ->
  if ULog#ulog.position == Position ->
      file:write(ULog#ulog.file, <<(size(BinaryEvent)):32, BinaryEvent/binary>>),

      NewULog = if (Position + size(BinaryEvent) + 4) bsr 24 =/= Position bsr 24 ->
          file:close(ULog#ulog.file),
          % remove old ulog files
          spawn(
            fun() ->
              case gen_server2:call(motiondb_schema, is_replica_nodes_up) of
                true when (Position bsr 24) > 0 ->
                  lists:foreach(
                    fun(I) -> file:delete(filename:join([Path, integer_to_list(I) ++ ".ulog"])) end,
                    lists:seq(0, (Position bsr 24) - 1)
                  );
                _ -> ok
              end
            end
          ),
          {ok, NewFile} = file:open(filename:join([Path, integer_to_list((Position bsr 24) + 1) ++ ".ulog"]), [read, write, binary]),
          #ulog{file = NewFile, position = ((Position bsr 24) + 1) bsl 24};
        true ->
          ULog#ulog{position = Position + size(BinaryEvent) + 4}
      end,

      int_event(BinaryEvent, Storage, StorageInstance, NewULog#ulog.position),
      {reply, ok, State#state{ulog=NewULog, sync_timeout=?SYNC_TIMEOUT}, ?SYNC_TIMEOUT};
    true ->
      if ULog#ulog.position < Position ->
          {reply, {error, inconsistent}, State, State#state.sync_timeout};
        true ->
          {reply, ok, State, State#state.sync_timeout}
      end
  end;

handle_call(turn_up, _From, State) ->
  {reply, ok, State#state{state=up}, State#state.sync_timeout};

handle_call(turn_down, _From, State) ->
  {reply, ok, State#state{state=down}, State#state.sync_timeout};

handle_call(is_up, _From, State) ->
  {reply, {ok, State#state.state == up}, State, State#state.sync_timeout};

handle_call(log_position, _From, State = #state{ulog=ULog}) ->
  {reply, {ok, ULog#ulog.position}, State, State#state.sync_timeout};

handle_call({get_events, Position}, _From, State = #state{path=Path}) ->
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
          {reply, {ok, Data}, State, State#state.sync_timeout};
        _ ->
          {reply, failed, State, State#state.sync_timeout}
      end;
    true ->
      {reply, no_events, State, State#state.sync_timeout}
  end;

handle_call(_Msg, _From, State) ->
  {reply, {error, invalid_message}, State, State#state.sync_timeout}.

handle_cast(_Msg, State) ->
  {noreply, State, State#state.sync_timeout}.

handle_info(timeout, State = #state{ulog=ULog, storage=Storage, storage_instance=StorageInstance}) ->
  spawn(
    fun() ->
      file:sync(ULog#ulog.file),
      Storage:sync(StorageInstance)
    end
  ),
  {noreply, State#state{sync_timeout=infinity}};

handle_info(_Info, State) ->
  {noreply, State, State#state.sync_timeout}.

terminate(_Reason, #state{storage=Storage, storage_instance=StorageInstance}) ->
  Storage:close(StorageInstance),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State, State#state.sync_timeout}.


%% ==========================================================================
%% Global Sync
%% ==========================================================================
sync(ServerName, [RemoteNode | RemoteNodes]) ->
  case catch(gen_server2:call({ServerName, RemoteNode}, is_up)) of
    {ok, IsUp} ->
      io:format("Sync started for node ~p~n", [RemoteNode]),
      case sync_with_server(ServerName, {ServerName, RemoteNode}) of
        ok when IsUp ->
          io:format("Sync ok, db is up now~n"),
          ok = gen_server2:call(ServerName, turn_up);
        ok ->
          sync(ServerName, RemoteNodes);
        failed ->
          sync(ServerName, RemoteNodes)
      end;
    _ ->
      sync(ServerName, RemoteNodes)
  end;

sync(_, []) ->
  failed.

sync_with_server(LocalServer, RemoteServer) ->
  {ok, Position} = gen_server2:call(LocalServer, log_position),
  case catch(gen_server2:call(RemoteServer, {get_events, Position})) of
    {ok, Data} ->
      io:format("Syncyng global from ~p to ~p~n", [Position, Position + size(Data)]),
      ok = sync_events(LocalServer, Position, Data),
      sync_with_server(LocalServer, RemoteServer);
    no_events ->
      ok;
    {'EXIT', _P} ->
      failed
  end.

sync_events(LocalServer, Position, <<Size:32, Data/binary>>) ->
  <<Event:Size/binary, Other/binary>> = Data,
  ok = gen_server2:call(LocalServer, {event, Position, Event}),
  sync_events(LocalServer, Position + 4 + size(Event), Other);

sync_events(_LocalServer, _Position, <<>>) ->
  ok.

turn_on(ServerName, RemoteNodes) ->
  case gen_server2:call(ServerName, is_up) of
    {ok, true} -> ok;
    {ok, false} ->
      case lists:all(
          fun(Node) ->
            case catch(gen_server2:call({ServerName, Node}, is_up)) of {ok, false} -> true; _ -> false end
          end,
          RemoteNodes
        ) of
        true ->
          gen_server2:call(ServerName, turn_up),
          lists:foreach(
            fun(Node) ->
              spawn(Node, storage, sync, [ServerName, [node() | lists:delete(Node, RemoteNodes)]])
            end,
            RemoteNodes
          ),
          ok;
        _ ->
          failed
      end
  end.

%% ===================================================================
%% Local sync
%% ===================================================================
local_sync(Path, Storage, StorageInstance, Position) ->
  case Storage:get(StorageInstance, {'__motiondb__', last_position}, false) of
    {ok, LastPosition} when LastPosition < Position ->
      io:format("Syncing local from ~p to ~p ~n", [LastPosition, Position]),
      case local_read(Path, LastPosition) of
        {ok, Data} ->
          local_sync_events(Storage, StorageInstance, LastPosition, Data),
          local_sync(Path, Storage, StorageInstance, Position);
        _ -> ok
      end;
    {error, not_found} when Position > 0 ->
      io:format("Syncing local from 0 to ~p ~n", [Position]),
      case local_read(Path, 0) of
        {ok, Data} ->
          local_sync_events(Storage, StorageInstance, 0, Data),
          local_sync(Path, Storage, StorageInstance, Position);
        _ -> ok
      end;      
    _ ->
      io:format("Local sync finished~n"),
      Storage:sync(StorageInstance)
  end.

local_sync_events(Storage, StorageInstance, Position, <<Size:32, Data/binary>>) ->
  <<BinaryEvent:Size/binary, Other/binary>> = Data,
  int_event(BinaryEvent, Storage, StorageInstance, Position + 4 + size(BinaryEvent)),
  local_sync_events(Storage, StorageInstance, Position + 4 + size(BinaryEvent), Other);

local_sync_events(_Storage, _StorageInstance, Position, <<>>) ->
  Position.

local_read(Path, Position) ->
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


%% ===================================================================
%% Internal functions
%% ===================================================================
call_event(Event, State = #state{ulog=ULog, nodes=Nodes, name=ServerName}) ->
  BinaryEvent = term_to_binary(Event),
  Position = ULog#ulog.position,
  {Goods, _Bads} = gen_server2:multi_call(motiondb_schema:multiply(Nodes, nodes([visible])), ServerName, {event, Position, BinaryEvent}),
  Any = lists:any(fun(Result) -> case Result of {_, ok} -> true; _ -> false end end, Goods),
  if Any orelse length(Nodes) == 0 ->
      handle_call({event, Position, BinaryEvent}, self(), State);
    true ->
      {reply, failed, State, State#state.sync_timeout}
  end.

int_event(BinaryEvent, Storage, StorageInstance, Position) ->
  case binary_to_term(BinaryEvent) of
    {put, Key, Value, AllowDuplication} ->
      Storage:put(StorageInstance, Key, Value, AllowDuplication);
    {remove, Key} ->
      Storage:remove(StorageInstance, Key);
    {remove, Key, Value} ->
      Storage:remove(StorageInstance, Key, Value)
  end,
  Storage:put(StorageInstance, {'__motiondb__', last_position}, Position, false).

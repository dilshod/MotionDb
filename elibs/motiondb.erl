-module(motiondb).
-behaviour(gen_server2).
-include("motiondb.hrl").

-export([start_link/0, initialize/0, multiply/2, current_partition/1, node_partition/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
  ets:new(motiondb, [named_table, public, set]),
  Path = filename:join([filename:dirname(code:which(motiondb)), "..", "config"]),
  file:make_dir(Path),
  case file:read_file(filename:join([Path, "config.bin"])) of
    {ok, Content} ->
      State = binary_to_term(Content),
      io:format("~p~n", [State]),
      ets:insert(motiondb, {state, State});
    {error, enoent} ->
      initialize(),
      save_state()
  end,
  gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

save_state() ->
  [{state, State}] = ets:lookup(motiondb, state),
  file:write_file(filename:join([filename:dirname(code:which(membership)), "..", "config", "config.bin"]), term_to_binary(State)),
  ok.

%%
%% @spec initialize() -> ok.
%% @doc Initialize first time
%% @end
%%
initialize() ->
  io:format("INITALIZING~n"),
  Partition = #partition{
    key = 0,
    key_bits = 0,
    nodes = [a@localhost, b@localhost]
  },
  ets:insert(motiondb, {state, #config{
    partitions = [Partition],
    storage = storage_tc,
    datapath = "database"
  }}),
  ok.

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
%% @spec current_partition(State) -> Partition.
%% @doc Get current partition
%%
%%
current_partition(State) ->
  node_partition(State#config.partitions, node()).

node_partition([Partition | Partitions], Node) ->
  case lists:any(fun(N) -> Node == N end, Partition#partition.nodes) of
    true -> Partition;
    false -> node_partition(Partitions, Node)
  end;

node_partition([], _) ->
  none.


%%
%% Server functions
%%
init([]) ->
  {ok, {}}.

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

-module(motiondb).
-include("motiondb.hrl").
-export([start/0, initialize/0, multiply/2]).

start() ->
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
  ok.

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

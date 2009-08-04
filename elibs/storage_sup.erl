-module(storage_sup).
-behaviour(supervisor).
-include("motiondb.hrl").
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    [{state, State}] = ets:lookup(motiondb, state),
    io:format("~p~n", [State]),
    Path = filename:join([State#config.datapath, atom_to_list(node())]),
    file:make_dir(Path),
    Partition = motiondb:current_partition(State),

    Children = lists:map(
      fun(I) ->
        IStr = integer_to_list(I),
        Name = list_to_atom("storage" ++ IStr),
        {
          Name,
          {storage, start_link, [storage_tc, Name, filename:join([Path, IStr]), Partition#partition.nodes]},
          permanent, ?SHUTDOWN_WAITING_TIME, worker, [storage, storage_tc]
        }
      end,
      lists:seq(0, 15)
    ),
    io:format("~p~n", [Children]),
    {ok, {{one_for_one,10,1}, Children}}.

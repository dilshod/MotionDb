-module(storage_sup).
-behaviour(supervisor).
-include("motiondb.hrl").
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    [Schema] = ets:lookup(motiondb, schema),
    Path = filename:join([filename:dirname(code:which(storage_sup)), "..", "database", atom_to_list(node())]),
    file:make_dir(Path),
    Partition = motiondb_schema:current_partition(Schema),

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
    {ok, {{one_for_one,10,1}, Children}}.

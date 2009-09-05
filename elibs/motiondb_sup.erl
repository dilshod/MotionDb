-module(motiondb_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  schema:init(),
  file:make_dir(lib_misc:root(database)),
  file:make_dir(lib_misc:root(["database", atom_to_list(node())])),

  Children = lists:map(
    fun(I) ->
      Name = storage:storage(I),
      {Name, {storage, start_link, [storage_tc, Name]}, permanent, 5000, worker, [storage, storage_tc]}
    end,
    lists:seq(0, 15)
  ) ++ [
    {schema, {schema, start_link, []}, permanent, 5000, worker, [schema]}
  ],
  {ok, {{one_for_one, 10, 1}, Children}}.

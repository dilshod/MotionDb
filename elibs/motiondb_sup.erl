-module(motiondb_sup).
-behaviour(supervisor).
-include("motiondb.hrl").
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Flags = {one_for_one, 2, 10},
    Children = [
      worker_spec(motiondb_schema, motiondb_schema, []),
      supervisor_spec(storage_sup, storage_sup, [])
    ],
    {ok, {Flags, Children}}.

worker_spec(Id, Module, Args) ->
    StartFunc = {Module, start_link, Args},
    {Id, StartFunc, permanent, ?SHUTDOWN_WAITING_TIME, worker, [Module]}.

supervisor_spec(Id, Module, Args) ->
    StartFunc = {Module, start_link, Args},
    {Id, StartFunc, permanent, infinity, supervisor, [Module]}.

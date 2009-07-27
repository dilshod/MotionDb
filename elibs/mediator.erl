-module(mediator).
%-include("edyno.hrl").
-export([put/2, get/1]).

%%
%% @spec put(Storage::atom(), Key, Value) -> {ok, Count} | {error, Reason}.
%% @doc Put item to storage
%% @end
%%
put(Key, Value) ->
  ok.

%%
%% @spec get(Storage::atom(), Key) -> {ok, {Context, Value}} | {ok, not_found} | {error, Reason}.
%% @doc Get item to storage
%% @end
%%
get(Key) ->
  ok.

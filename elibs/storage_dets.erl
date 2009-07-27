-module(storage_dets).
-export([start/1, close/1, sync/1, put/4, get/3, remove/2, remove/3]).

start(Path) ->
  FileSet = filename:join([Path, "dbs.dets"]),
  FileBag = filename:join([Path, "dbb.dets"]),
  {ok, Set} = dets:open_file(FileSet, [{file, FileSet}, {type, set}]),
  {ok, Bag} = dets:open_file(FileBag, [{file, FileBag}, {type, bag}]),
  {ok, {Set, Bag}}.

close({Set, Bag}) ->
  dets:close(Set),
  dest:close(Bag).

sync({Set, Bag}) ->
  dets:sync(Set),
  dets:sync(Bag).

put({Set, _}, Key, Value, false) ->
  dets:insert(Set, {Key, Value});

put({_, Bag}, Key, Value, true) ->
  dets:insert(Bag, {Key, Value}).

get({Set, _}, Key, false) ->
  case dets:lookup(Set, Key) of
    [{Key, Value}] -> {ok, Value};
    _ -> {error, not_found}
  end;

get({_, Bag}, Key, true) ->
  lists:map(
    fun({_, Value}) ->
      Value
    end,
    dets:lookup(Bag, Key)
  ).

remove({Set, Bag}, Key) ->
  dets:delete(Set, Key),
  dets:delete(Bag, Key).

remove({Set, Bag}, Key, Value) ->
  dets:delete(Set, Key),
  dets:delete_object(Bag, {Key, Value}).

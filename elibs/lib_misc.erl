-module(lib_misc).
-include("motiondb.hrl").

-export([root/0, root/1, multiply/2, send_file/2, receive_file/2, sort_nodes/1]).

-define(CHUNK, 8192).

%%
%% @spec root() -> RootDir::list()
%% @doc Get root path
%% @end
%%
root() ->
  filename:join([
    filename:dirname(
      case code:which(lib_misc) of
        cover_compiled -> code:which(test_lib_misc);
        Dir -> Dir
      end
    ), ".."
  ]).

%%
%% @spec root(Path) -> RootDir::list()
%% @doc Get root path + Path
%% @end
%%
root(Path) when is_atom(Path) ->
  filename:join([root(), atom_to_list(Path)]);

root(Path) ->
  filename:join([root()] ++ Path).

%%
%% @spec multiply(Left::list(), Right::list()) -> Result::list()
%% @doc Multiply two lists
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
%% @spec send_file(Node, Filename) -> ok | {error, node_down} | {}
%% @doc Send file from local node to remote node
%% @end
%%
send_file(Node, Filename) ->
  case net_adm:ping(Node) of
    pong ->
      case file:open(Filename, [raw, binary, read]) of
        {ok, File} ->
          Ref = make_ref(),
          SaveFilename = integer_to_list(erlang:phash2({self(), Ref})),
          Receiver = spawn_link(Node, lib_misc, receive_file, [Ref, SaveFilename]),
          Result = send_contents(Ref, File, Receiver, crypto:md5_init()),
          file:close(File),
          case Result of
            ok -> {ok, SaveFilename};
            _ -> Result
          end;
        Err -> {error, Err}
      end;
    pang -> {error, node_down}
  end.

send_contents(Ref, File, Receiver, MD5) ->
  case file:read(File, ?CHUNK) of
    {ok, Data} -> 
      Receiver ! {Ref, data, Data},
      send_contents(Ref, File, Receiver, crypto:md5_update(MD5, Data));
    eof ->
      Receiver ! {Ref, md5, self(), crypto:md5_final(MD5)},
      receive
        {Ref, ok} -> ok;
        {Ref, error} -> {error, invalid_hash}
      after timer:minutes(10) -> {error, timeout}
      end;
    {error, Reason} -> 
      error_logger:info_msg("Send failed with reason ~p~n", [Reason]),
      Receiver ! {Ref, error, Reason},
      {error, Reason}
  end.

receive_file(Ref, Filename) ->
  {ok, File} = file:open(lib_misc:root(["database", Filename]), [raw, binary, write]),
  receive_contents(Ref, File, crypto:md5_init()).

receive_contents(Ref, File, MD5) ->
  receive
    {Ref, data, Data} ->
      file:write(File, Data),
      receive_contents(Ref, File, crypto:md5_update(MD5, Data));
    {Ref, md5, From, Hash} ->
      file:close(File),
      case crypto:md5_final(MD5) of
        Hash -> From ! {Ref, ok};
        _ -> From ! {Ref, error}
      end;
    {Ref, error, Reason} ->
      error_logger:info_msg("Receive failed with reason ~p~n", [Reason]),
      exit(Reason);
    _ -> ok
  after timer:minutes(10) -> ok
  end.

%%
%% @spec sort_nodes(Nodes) -> SortedNodes
%% @doc Sort nodes list randomly, and if there is a local node in the list it will appear first
%% @end
%%
sort_nodes(Nodes) ->
  lists:sort(
    fun(L, R) ->
      (L == node() orelse (L /= node() andalso random:uniform(2) == 1)) andalso R /= node()
    end,
    Nodes
  ).

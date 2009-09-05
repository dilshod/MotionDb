-module(test_lib_misc).
-include("motiondb.hrl").
-include_lib("eunit/include/eunit.hrl").

multiply_test_() ->
  [
   ?_assertEqual([d, "c"], lib_misc:multiply([a, b, "c", d], ["c", d, e])),
   ?_assertEqual([], lib_misc:multiply([1, 2], [3, 4])),
   ?_assertEqual([all], lib_misc:multiply([all], [all])),
   ?_assertEqual([], lib_misc:multiply([all], [])),
   ?_assertEqual([], lib_misc:multiply([], [all])),
   ?_assertEqual([], lib_misc:multiply([], []))
  ].

send_file_test() ->
  TestFile = lib_misc:root(["database", "send_file.test"]),
  {ok, File} = file:open(TestFile, [write]),
  file:write(File, "test data"),
  file:close(File),
  
  ?assertMatch({error, node_down}, lib_misc:send_file(invalidnode@invalidhost, "TestFile")),
  Res = lib_misc:send_file(node(), TestFile),
  ?assertMatch({ok, _Filename}, Res),
  {ok, Filename} = Res,
  
  ?assertEqual(filelib:file_size(lib_misc:root(["database", Filename])), filelib:file_size(TestFile)),
  file:delete(lib_misc:root(["database", Filename])),
  file:delete(TestFile).

sort_nodes_test_() ->
  Node = node(),
  % random sort test
  ?assert(
    lib_misc:sort_nodes([a, b, c]) =/= lib_misc:sort_nodes([a, b, c]) orelse
    lib_misc:sort_nodes([a, b, c]) =/= lib_misc:sort_nodes([a, b, c]) orelse
    lib_misc:sort_nodes([a, b, c]) =/= lib_misc:sort_nodes([a, b, c])
  ),
  [
   ?_assertMatch([], lib_misc:sort_nodes([])),
   ?_assertMatch([Node | _], lib_misc:sort_nodes([node1@localhost, Node, node2@localhost, node3@localhost]))
  ].

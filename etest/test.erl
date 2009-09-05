-module(test).
-export([test/0, coverage/0]).

start() ->
  file:make_dir(lib_misc:root(database)),
  file:make_dir(lib_misc:root(["database", atom_to_list(node())])),
  application:start(crypto).

test() ->
  start(),
  all_test:test().

coverage() ->
  start(),
  Dir = filename:join([filename:dirname(code:which(test)), ".."]),
  Modules = cover:compile_directory("elibs", [{i, filename:join([Dir, "deps/yaws-1.8.4/include"])}, {d, 'TEST'}, {warn_format, 0}]),

  % run tests
  all_test:test(),

  % save coverage
  lists:foreach(
    fun({ok, Module}) ->
      cover:analyze_to_file(Module, filename:join([Dir, "doc", atom_to_list(Module) ++ "_coverage.html"]), [html])
    end,
    Modules
  ).

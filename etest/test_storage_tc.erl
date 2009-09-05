-module(test_storage_tc).
-include_lib("eunit/include/eunit.hrl").

test_backup(Port) ->
  Backup = filename:join([filename:dirname(code:which(test_storage_tc)), "..", "database", atom_to_list(node()), "backup"]),
  file:make_dir(Backup),
  file:delete(filename:join([Backup, "db.tc"])),

  storage_tc:put(Port, backup_test, test_ok),
  storage_tc:backup(Port, filename:join([Backup, "db.tc"])),
  Res = storage_tc:start(Backup),
  ?assertMatch({ok, _}, Res),
  {ok, BackupPort} = Res,
  ?assertEqual({ok, test_ok}, storage_tc:get(BackupPort, backup_test)),
  storage_tc:close(BackupPort).

tc_tests(Port) ->
  [
   % truncate test
   ?_assertMatch(ok, storage_tc:put(Port, 1, test)),
   ?_assertMatch(ok, storage_tc:truncate(Port)),
   ?_assertMatch({ok, 0, _}, storage_tc:info(Port)),
   % put get test
   ?_assertMatch(ok, storage_tc:put(Port, test, word)),
   ?_assertMatch({ok, word}, storage_tc:get(Port, test)),
   ?_assertMatch({ok, [word]}, storage_tc:getvalues(Port, test)),
   % append test
   ?_assertMatch(ok, storage_tc:append(Port, test, 123)),
   ?_assertMatch({ok, [word, 123]}, storage_tc:getvalues(Port, test)),
   % putvalues test
   ?_assertMatch(ok, storage_tc:putvalues(Port, test, [2, 4, 6, 8, "all"])),
   ?_assertMatch({ok, [2, 4, 6, 8, "all"]}, storage_tc:getvalues(Port, test)),
   ?_assertMatch({ok, 5, _}, storage_tc:info(Port)),
   % remove value test
   ?_assertMatch(ok, storage_tc:remove(Port, test, "all")),
   ?_assertMatch(ok, storage_tc:remove(Port, test, 4)),
   ?_assertMatch({ok, [2, 6, 8]}, storage_tc:getvalues(Port, test)),
   % remove test
   ?_assertMatch(ok, storage_tc:remove(Port, test)),
   ?_assertMatch({error, not_found}, storage_tc:get(Port, test)),
   ?_assertMatch({ok, []}, storage_tc:getvalues(Port, test)),
   % sync test
   ?_assertMatch(ok, storage_tc:sync(Port)),
   % first/next test
   ?_assertMatch(ok, storage_tc:truncate(Port)),
   ?_assertMatch({error, not_found}, storage_tc:first(Port)),
   ?_assertMatch(ok, storage_tc:put(Port, 33, first)),
   ?_assertMatch(ok, storage_tc:put(Port, test, third)),
   ?_assertMatch(ok, storage_tc:put(Port, record, second)),
   ?_assertMatch({ok, 33}, storage_tc:first(Port)),
   ?_assertMatch({ok, test}, storage_tc:next(Port, 33)),
   ?_assertMatch({ok, record}, storage_tc:next(Port, test)),
   ?_assertMatch({error, not_found}, storage_tc:next(Port, record)),
   % backup
   ?_assertMatch(ok, test_backup(Port))
  ].

basic_test_() ->
  {setup,
   fun() ->
     Path = filename:join([filename:dirname(code:which(test_storage_tc)), "..", "database", atom_to_list(node())]),
     file:make_dir(Path),
     {ok, Port} = storage_tc:start(Path),
     Port
   end,
   fun(Port) -> storage_tc:close(Port) end,
   fun tc_tests/1}.

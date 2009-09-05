-module(test_storage).
-include("motiondb.hrl").
-include_lib("eunit/include/eunit.hrl").

backup_load_tests(Pid) ->
  % backup test
  storage:truncate(Pid),
  storage:put(Pid, backup_test1, test_ok),
  Result = storage:backup(Pid),
  ?assertMatch({ok, _Filename, _Position}, Result),
  {ok, Filename, Position} = Result,
  % get position test
  ?assertMatch({ok, Position}, gen_server2:call(Pid, log_position)),
  storage:put(Pid, backup_test2, fail),
  ?assertNot({ok, Position} == gen_server2:call(Pid, log_position)),
  % load test
  ?assertMatch(ok, storage:load(Pid, Filename, Position)),
  ?assertMatch({error, node_down}, storage:get(Pid, backup_test2)),
  ?assertMatch(ok, gen_server2:call(Pid, turn_up)),
  ?assertMatch({error, not_found}, storage:get(Pid, backup_test2)),
  ?assertMatch({ok, test_ok}, storage:get(Pid, backup_test1)),
  ok.

storage_one_node_tests(Pid) ->
  [
   ?_assertMatch({error, invalid_message}, gen_server2:call(Pid, invalid_message)),
   % test storage serverName gen function
   ?_assertMatch(storage0, storage:storage(0)),
   ?_assertMatch(storage1, storage:storage(1)),
   % turn down test
   ?_assertMatch(ok, gen_server2:call(Pid, turn_down)),
   ?_assertMatch(false, storage:is_up(Pid)),
   ?_assertMatch(ok, storage:truncate(Pid)),
   % node down test
   ?_assertMatch({error, node_down}, storage:info(Pid)),
   ?_assertMatch({error, node_down}, storage:get(Pid, test)),
   ?_assertMatch({error, node_down}, storage:getvalues(Pid, test)),
   ?_assertMatch({error, node_down}, storage:first(Pid)),
   ?_assertMatch({error, node_down}, storage:next(Pid, test)),
   ?_assertMatch({error, node_down}, storage:backup(Pid)),
   % turn up test
   ?_assertMatch(ok, gen_server2:call(Pid, turn_up)),
   ?_assertMatch(true, storage:is_up(Pid)),
   ?_assertMatch({ok, 0, _}, storage:info(Pid)),
   % put/get tests
   ?_assertMatch({error, not_found}, storage:get(Pid, {first, record})),
   ?_assertMatch(ok, storage:put(Pid, {first, record}, {record, data})),
   ?_assertMatch({ok, {record, data}}, storage:get(Pid, {first, record})),
   % info test
   ?_assertMatch({ok, 1, _}, storage:info(Pid)),
   ?_assertMatch(ok, storage:put(Pid, {second, record}, test)),
   ?_assertMatch({ok, 2, _}, storage:info(Pid)),
   % sync test
   ?_assertMatch(ok, gen_server2:call(Pid, sync)),
   ?_assertMatch(ok, timer:sleep(10)),
   % append/putvalues/getvalues test
   ?_assertMatch({ok, [{record, data}]}, storage:getvalues(Pid, {first, record})),
   ?_assertMatch(ok, storage:append(Pid, {first, record}, record2)),
   ?_assertMatch({ok, [{record, data}, record2]}, storage:getvalues(Pid, {first, record})),
   ?_assertMatch({ok, []}, storage:getvalues(Pid, second)),
   ?_assertMatch(ok, storage:putvalues(Pid, second, [1, 2, 4, 6, 8, 9, done])),
   ?_assertMatch({ok, [1, 2, 4, 6, 8, 9, done]}, storage:getvalues(Pid, second)),
   % remove test
   ?_assertMatch(ok, storage:remove(Pid, second, 4)),
   ?_assertMatch(ok, storage:remove(Pid, second, 1)),
   ?_assertMatch(ok, storage:remove(Pid, second, done)),
   ?_assertMatch({ok, [2, 6, 8, 9]}, storage:getvalues(Pid, second)),
   ?_assertMatch(ok, storage:remove(Pid, second)),
   ?_assertMatch({ok, []}, storage:getvalues(Pid, second)),
   % first/next test
   ?_assertMatch(ok, storage:truncate(Pid)),
   ?_assertMatch(ok, storage:put(Pid, 1, 2)),
   ?_assertMatch(ok, storage:remove(Pid, 1)),
   ?_assertMatch({error, not_found}, storage:first(Pid)),
   ?_assertMatch(ok, storage:put(Pid, first, {record, 1})),
   ?_assertMatch({ok, first}, storage:first(Pid)),
   ?_assertMatch(ok, storage:put(Pid, second, {record, 2})),
   ?_assertMatch(ok, storage:put(Pid, third, {record, 3})),
   ?_assertMatch({ok, first}, storage:first(Pid)),
   ?_assertMatch({ok, third}, storage:next(Pid, first)),
   ?_assertMatch({ok, second}, storage:next(Pid, third)),
   ?_assertMatch({error, not_found}, storage:next(Pid, second)),
   % backup/load test
   ?_assertMatch(ok, backup_load_tests(Pid))
  ].

one_node_test_() ->
  {setup,
   fun() ->
     schema:destroy(),
     schema:init(),
     {ok, Pid} = storage:start_link(storage_tc, test),
     Pid
   end,
   fun(Pid) -> storage:stop(Pid) end,
   fun storage_one_node_tests/1}.

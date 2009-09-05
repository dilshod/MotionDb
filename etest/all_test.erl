-module(all_test).
-include_lib("eunit/include/eunit.hrl").

all_test_() ->
  [{module, test_lib_misc},
   {module, test_storage_tc},
   {module, test_storage},
   {module, test_schema}].

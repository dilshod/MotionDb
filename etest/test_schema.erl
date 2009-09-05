-module(test_schema).
-include("motiondb.hrl").
-include_lib("eunit/include/eunit.hrl").

schema_set_get_test_() ->
  Schema1 = #schema{version=1, partitions=[]},
  Schema2 = #schema{version=2, partitions=[#partition{}]},
  [
   ?_assertMatch(ok, schema:destroy()),
   ?_assertMatch({ok, #schema{}}, schema:init()),
   ?_assertMatch(#schema{}, schema:get()),
   ?_assertMatch({ok, Schema1}, schema:set(Schema1)),
   ?_assertMatch({unchanged, Schema1}, schema:set(Schema1)),
   ?_assertMatch(Schema1, schema:get()),
   ?_assertMatch({ok, Schema2}, schema:set(Schema2)),
   ?_assertMatch({invalid_version, Schema2}, schema:set(Schema1)),
   ?_assertMatch(Schema2, schema:get())
  ].

current_partition_test_() ->
  Node = node(),
  Partition1 = #partition{key=0, key_bits=1, nodes=[node1@localhost, node2@localhost]},
  Partition2 = #partition{key=1, key_bits=2, nodes=[node3@localhost, node4@localhost]},
  Partition3 = #partition{key=2, key_bits=2, nodes=[node5@localhost, node()]},
  Schema1 = #schema{version=1, partitions = [Partition1, Partition2]},
  Schema2 = #schema{version=2, partitions = [Partition1, Partition2#partition{right_partition=Partition3}]},
  Schema3 = #schema{version=3, partitions = [Partition1, Partition2, Partition3]},
  [
   ?_assertMatch(ok, schema:destroy()),
   ?_assertMatch({ok, #schema{}}, schema:init()),
   % schema1
   ?_assertMatch({ok, Schema1}, schema:set(Schema1)),
   ?_assertMatch({error, not_found}, schema:node_partition(invalid_node@localhost)),
   ?_assertMatch({ok, Partition2}, schema:node_partition(node3@localhost)),
   ?_assertMatch([node1@localhost, node2@localhost, node3@localhost, node4@localhost], schema:all_remote_nodes()),
   ?_assertMatch({error, not_found}, schema:current_partition())
   % schema2
%   ?_assertMatch({ok, Schema2}, schema:set(Schema2))
%   ?_assertMatch({ok, Partition3}, schema:node_partition(node5@localhost)),
%   ?_assertMatch([node1@localhost, node2@localhost, node3@localhost, node4@localhost, node5@localhost], schema:all_remote_nodes()),
%   ?_assertMatch([node1@localhost, node2@localhost, node3@localhost, node4@localhost, node5@localhost, Node], schema:all_nodes()),
%   ?_assertMatch({ok, Partition3}, schema:current_partition()),
   % schema3
%   ?_assertMatch({ok, Schema3}, schema:set(Schema3)),
%   ?_assertMatch({ok, Partition3}, schema:current_partition())
  ].

partition_for_hash_test_() ->
  Partitions = [
    #partition{key=2#00, key_bits=2},
    #partition{key= 2#1, key_bits=1},
    #partition{key=2#10, key_bits=2}
  ],
  [
   ?_assertEqual({error, not_found}, schema:partition_for_hash(0, [])),
   ?_assertEqual({ok, #partition{key=0, key_bits=0}}, schema:partition_for_hash(0, [#partition{key=0, key_bits=0}])),
   ?_assertEqual({ok, #partition{key=0, key_bits=0}}, schema:partition_for_hash(1, [#partition{key=0, key_bits=0}])),
   ?_assertEqual({ok, #partition{key=0, key_bits=2}}, schema:partition_for_hash(0, Partitions)),
   ?_assertEqual({ok, #partition{key=1, key_bits=1}}, schema:partition_for_hash(1, Partitions)),
   ?_assertEqual({ok, #partition{key=2, key_bits=2}}, schema:partition_for_hash(2, Partitions)),
   ?_assertEqual({ok, #partition{key=1, key_bits=1}}, schema:partition_for_hash(3, Partitions)),
   ?_assertEqual({ok, #partition{key=0, key_bits=2}}, schema:partition_for_hash(4, Partitions)),
   ?_assertEqual({ok, #partition{key=1, key_bits=1}}, schema:partition_for_hash(5, Partitions)),
   ?_assertEqual({ok, #partition{key=2, key_bits=2}}, schema:partition_for_hash(6, Partitions)),
   ?_assertEqual({ok, #partition{key=1, key_bits=1}}, schema:partition_for_hash(7, Partitions)),
   ?_assertEqual({ok, #partition{key=0, key_bits=2}}, schema:partition_for_hash(erlang:phash2(test, (1 bsl 32) - 1), Partitions))
  ].

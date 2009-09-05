
-record(schema, {
  version = 0,
  partitions,
  state
}).

-record(partition, {
  key,
  key_bits,
  nodes = [],
  joining_node = none,
  right_partition = none,
  right_operation = none
}).

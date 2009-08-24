
-define(SHUTDOWN_WAITING_TIME, 2000).

-record(schema, {
  version,
  partitions,
  storage,
  datapath,
  state
}).

-record(partition, {
  key,
  key_bits,
  nodes,
  right_partition = none
}).

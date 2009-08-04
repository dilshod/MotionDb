
-define(SHUTDOWN_WAITING_TIME, 2000).

-record(config, {
  partitions,
  storage,
  datapath
}).

-record(partition, {
  key,
  key_bits,
  nodes,
  right_partition = none
}).

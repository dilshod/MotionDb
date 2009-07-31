
-record(config, {
  partitions,
  storage,
  datapath
}).

-record(partition, {
  key,
  key_bits,
  nodes
}).

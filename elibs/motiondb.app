{application, motiondb, [
  {description, "MotionDB Node"},
  {mod, {motiondb, []}},
  {vsn, "?VERSION"},
  {modules, [
    motiondb, schema, storage, storage_tc
  ]},
  {env, [
    {withhttp, false},
    {servername, "local.com"},
    {port, 11500}
  ]},
  {registered, []},
  {applications, [kernel, stdlib, sasl, crypto]}
]}.

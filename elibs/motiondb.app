{application, motiondb, [
  {description, "MotionDB Node"},
  {mod, {motiondb_app, []}},
  {vsn, "?VERSION"},
  {modules, [
      motiondb_app, motiondb
  ]},
  {registered, []},
  {applications, [kernel, stdlib, sasl, crypto]}
]}.

{application, motiondb, [
  {description, "MotionDB Node"},
  {mod, {mdb_app, []}},
  {vsn, "?VERSION"},
  {modules, [
      mdb_app
  ]},
  {registered, []},
  {applications, [kernel, stdlib, sasl, crypto]}
]}.


#include <ei.h>
#include <erl_driver.h>
#include <tcbdb.h>
#include <string.h>

#define OPEN 0
#define CLOSE 1
#define PUT 2
#define PUTDUP 3
#define PUTCAT 4
#define PUTKEEP 5
#define GET 6
#define GETDUP 7
#define REMOVE 8
#define RANGE 9
#define SYNC 10
#define INFO 11
#define ITERATE 12
#define VANISH 13
#define BACKUP 14

#define MAX_KEY_SIZE 1024
#define MAX_VALUE_SIZE 16384

typedef struct _tcbdb_drv_t {
  ErlDrvPort port;
  TCBDB *bdb;
} tcbdb_drv_t;

static int ei_error(ei_x_buff *x, const char *error, char** res, int res_size) {
  if (x) ei_x_free(x);
  ei_x_new_with_version(x);
  ei_x_encode_tuple_header(x, 2);
  ei_x_encode_atom(x, "error");
  ei_x_encode_atom(x, error);
  if (res_size < x->index)
    (*res) = (char *) driver_alloc(x->index);
  int n = x->index;
  memcpy(*res, x->buff, x->index);
  ei_x_free(x);
  return n;
};

//=======================================================================
// ERL_DRIVER CALLBACKS
static ErlDrvData init(ErlDrvPort port, char *cmd) {
  tcbdb_drv_t *driver;
  driver = driver_alloc(sizeof(tcbdb_drv_t));
  driver->port = port;
  driver->bdb = NULL;
  return (ErlDrvData)driver;
}

static void stop(ErlDrvData handle) {
  tcbdb_drv_t *driver = (tcbdb_drv_t*)handle;
  if (driver->bdb) {
    tcbdbclose(driver->bdb);
    tcbdbdel(driver->bdb);
    driver->bdb = NULL;
  }
  driver_free(driver);
}

static void output(ErlDrvData handle, char *buf, int len) {
}

static int control(ErlDrvData handle, unsigned int command, char* buf, int count, char** res, int res_size) {
  tcbdb_drv_t *driver = (tcbdb_drv_t*)handle;
  TCBDB *bdb = driver->bdb;

  int index = 1, tp, sz, version;
  char key[MAX_KEY_SIZE], value[MAX_VALUE_SIZE];
  long key_size, value_size;
  long long long_tmp;
  bool rs;
  const char *value_tmp = NULL;

  ei_x_buff x;
  ei_x_new_with_version(&x);
  if (bdb == NULL && command != OPEN)
    return ei_error(&x, "database_not_opened", res, res_size);

  ei_decode_version(buf, &index, &version);

  switch (command) {
    case OPEN:
      // open(Filepath::string())
      if (bdb == NULL) {
        ei_get_type(buf, &index, &tp, &sz);
        if (tp != ERL_STRING_EXT)
          return ei_error(&x, "invalid_argument", res, res_size);

        TCBDB *bdb = tcbdbnew();
        tcbdbsetmutex(bdb);
        tcbdbsetcache(bdb, 104800, 51200);
        tcbdbsetxmsiz(bdb, 1048576);
        tcbdbtune(bdb, 0, 0, 0, 7, -1, BDBTLARGE);

        char *file = driver_alloc(sz + 1);
        ei_decode_string(buf, &index, file);
        rs = tcbdbopen(bdb, file, BDBOWRITER | BDBOCREAT);
        driver_free(file);
        if (!rs)
          return ei_error(&x, tcbdberrmsg(tcbdbecode(bdb)), res, res_size);
        driver->bdb = bdb;
        ei_x_encode_atom(&x, "ok");
      } else
        return ei_error(&x, "database already opened", res, res_size);
      break;

    case CLOSE:
      tcbdbclose(bdb);
      tcbdbdel(bdb);
      driver->bdb = NULL;
      ei_x_encode_atom(&x, "ok");
      break;

    case PUT:
    case PUTDUP:
    case PUTCAT:
    case PUTKEEP:
      // put({Key::binary(), Value::binary()})
      ei_get_type(buf, &index, &tp, &sz);
      if (tp != ERL_SMALL_TUPLE_EXT || sz != 2)
        return ei_error(&x, "invalid_argument", res, res_size);
      ei_decode_tuple_header(buf, &index, &sz);

      ei_get_type(buf, &index, &tp, &sz);
      if (tp != ERL_BINARY_EXT || sz > MAX_KEY_SIZE)
        return ei_error(&x, "invalid_argument", res, res_size);
      ei_decode_binary(buf, &index, &key[0], &key_size);
      ei_get_type(buf, &index, &tp, &sz);
      if (tp != ERL_BINARY_EXT)
        return ei_error(&x, "invalid_argument", res, res_size);
      if (sz <= MAX_VALUE_SIZE) {
        ei_decode_binary(buf, &index, &value[0], &value_size);
        switch (command) {
          case PUT: rs = tcbdbput(bdb, &key[0], key_size, &value[0], value_size); break;
          case PUTDUP: rs = tcbdbputdup(bdb, &key[0], key_size, &value[0], value_size); break;
          case PUTCAT: rs = tcbdbputcat(bdb, &key[0], key_size, &value[0], value_size); break;
          default: rs = tcbdbputkeep(bdb, &key[0], key_size, &value[0], value_size);
        }
      } else {
        void *p = driver_alloc(sz);
        if (p) {
          ei_decode_binary(buf, &index, p, &value_size);
          switch (command) {
            case PUT: rs = tcbdbput(bdb, &key[0], key_size, p, value_size); break;
            case PUTDUP: rs = tcbdbputdup(bdb, &key[0], key_size, p, value_size); break;
            case PUTCAT: rs = tcbdbputcat(bdb, &key[0], key_size, p, value_size); break;
            default: rs = tcbdbputkeep(bdb, &key[0], key_size, p, value_size);
          }
          driver_free(p);
        } else
          return ei_error(&x, "too long value", res, res_size);
      };
      if (!rs)
        return ei_error(&x, tcbdberrmsg(tcbdbecode(bdb)), res, res_size);
      ei_x_encode_atom(&x, "ok");
      break;

    case GET:
      // get(Key::binary()) -> {ok, Value} | {error, not_found}
      ei_get_type(buf, &index, &tp, &sz);
      if (tp != ERL_BINARY_EXT || sz > MAX_KEY_SIZE)
        return ei_error(&x, "invalid_argument", res, res_size);
      ei_decode_binary(buf, &index, &key[0], &key_size);

      value_tmp = tcbdbget3(bdb, &key[0], key_size, &sz);
      ei_x_encode_tuple_header(&x, 2);
      if (value_tmp != NULL) {
        ei_x_encode_atom(&x, "ok");
        ei_x_encode_binary(&x, value_tmp, sz);
      } else {
        ei_x_encode_atom(&x, "error");
        ei_x_encode_atom(&x, "not_found");
      }
      break;

    case GETDUP:
      // get(Key::binary()) -> {ok, Values}
      ei_get_type(buf, &index, &tp, &sz);
      if (tp != ERL_BINARY_EXT || sz > MAX_KEY_SIZE)
        return ei_error(&x, "invalid_argument", res, res_size);
      ei_decode_binary(buf, &index, &key[0], &key_size);

      TCLIST *vals = tcbdbget4(bdb, key, key_size);
      if (vals) {
        ei_x_encode_tuple_header(&x, 2);
        ei_x_encode_atom(&x, "ok");
        int j;
        for (j=0; j<tclistnum(vals); j++) {
          value_tmp = tclistval(vals, j, &sz);
          ei_x_encode_list_header(&x, 1);
          ei_x_encode_binary(&x, value_tmp, sz);
        }
        tclistdel(vals);
        ei_x_encode_empty_list(&x);
      } else {
        ei_x_encode_tuple_header(&x, 2);
        ei_x_encode_atom(&x, "ok");
        ei_x_encode_empty_list(&x);
      }
      break;

    case REMOVE:
      // remove(Keys::list())
      // remove(Key::binary())
      // remove({Key::binary(), Value::binary()})
      ei_get_type(buf, &index, &tp, &sz);
      if (tp == ERL_LIST_EXT) {
        int count, j;
        ei_decode_list_header(buf, &index, &count);
        for (j=0; j<count; j++) {
          ei_get_type(buf, &index, &tp, &sz);
          if (tp != ERL_BINARY_EXT || sz > MAX_KEY_SIZE)
            return ei_error(&x, "invalid_argument", res, res_size);
          ei_decode_binary(buf, &index, &key[0], &key_size);
          if (!tcbdbout3(bdb, &key[0], key_size))
            return ei_error(&x, tcbdberrmsg(tcbdbecode(bdb)), res, res_size);
        }
        ei_x_encode_atom(&x, "ok");
      } else if (tp == ERL_BINARY_EXT && sz <= MAX_KEY_SIZE) {
        ei_decode_binary(buf, &index, &key[0], &key_size);
        tcbdbout3(bdb, &key[0], key_size);
        ei_x_encode_atom(&x, "ok");
      } else if (tp == ERL_SMALL_TUPLE_EXT && sz == 2) {
        ei_decode_tuple_header(buf, &index, &sz);
        // get key
        ei_get_type(buf, &index, &tp, &sz);
        if (tp != ERL_BINARY_EXT || sz > MAX_KEY_SIZE)
          return ei_error(&x, "invalid_argument", res, res_size);
        ei_decode_binary(buf, &index, &key[0], &key_size);
        // get value
        ei_get_type(buf, &index, &tp, &sz);
        if (tp != ERL_BINARY_EXT || sz > MAX_VALUE_SIZE)
          return ei_error(&x, "invalid_argument", res, res_size);
        ei_decode_binary(buf, &index, &value[0], &value_size);
        // remove by key&value
        BDBCUR *cur = tcbdbcurnew(bdb);
        if (!tcbdbcurjump(cur, &key[0], key_size))
          return ei_error(&x, "record_not_found", res, res_size);
        
        bool removed = false, not_found = false;
        while (!removed && !not_found) {
          int cur_key_size, cur_val_size;
          const void *curkey = tcbdbcurkey3(cur, &cur_key_size);
          if (cur_key_size == key_size && memcmp(curkey, key, key_size) == 0) {
            const void *curval = tcbdbcurval3(cur, &cur_val_size);
            if (cur_val_size == value_size && memcmp(curval, value, value_size) == 0) {
              tcbdbcurout(cur);
              removed = true;
            } else
              if (!tcbdbcurnext(cur))
                not_found = true;
          } else not_found = true;
        }
        if (not_found) ei_x_encode_atom(&x, "not_found");
        else ei_x_encode_atom(&x, "ok");
        
      } else
        return ei_error(&x, "invalid_argument", res, res_size);
      break;

    case RANGE:
      /*
       * range({Prefix::binary(), limit:integer()})
       * range({StartKey::binary(), BeginInclusion::boolean(), EndKey::binary(), EndInclusion::binary(), limit:integer()})
       */
      ei_get_type(buf, &index, &tp, &sz);
      if (tp != ERL_SMALL_TUPLE_EXT || sz < 2)
        return ei_error(&x, "invalid_argument", res, res_size);
      ei_decode_tuple_header(buf, &index, &sz);

      ei_get_type(buf, &index, &tp, &sz);
      if (tp == ERL_BINARY_EXT && sz <= MAX_KEY_SIZE) {
        char keys[MAX_KEY_SIZE], keyf[MAX_KEY_SIZE];
        long keys_size, keyf_size;
        int keys_inc, keyf_inc;
        long max = -1;
        TCLIST *range;

        ei_decode_binary(buf, &index, &keys[0], &keys_size);
        ei_get_type(buf, &index, &tp, &sz);
        if (tp == ERL_ATOM_EXT) {
          // range
          ei_decode_boolean(buf, &index, &keys_inc);
          ei_get_type(buf, &index, &tp, &sz);
          if (tp != ERL_BINARY_EXT || sz > MAX_KEY_SIZE)
            return ei_error(&x, "invalid_argument", res, res_size);
          ei_decode_binary(buf, &index, &keyf[0], &keyf_size);
          ei_get_type(buf, &index, &tp, &sz);
          if (tp != ERL_ATOM_EXT)
            return ei_error(&x, "invalid_argument", res, res_size);
          ei_decode_boolean(buf, &index, &keyf_inc);

          ei_get_type(buf, &index, &tp, &sz);
          if (tp != ERL_INTEGER_EXT)
            return ei_error(&x, "invalid_argument", res, res_size);
          ei_decode_long(buf, &index, &max);

          range = tcbdbrange(bdb, &keys[0], keys_size, keys_inc == 1, &keyf[0], keyf_size, keyf_inc == 1, max);
        } else if (tp == ERL_INTEGER_EXT) {
          // prefix
          ei_get_type(buf, &index, &tp, &sz);
          if (tp != ERL_INTEGER_EXT)
            return ei_error(&x, "invalid_argument", res, res_size);
          ei_decode_long(buf, &index, &max);

          range = tcbdbfwmkeys(bdb, &keys[0], keys_size, max);
        } else
          return ei_error(&x, "invalid_argument", res, res_size);

        const char *key;
        int key_size, value_size;
        int idx, cnt = 0, rcount = tclistnum(range);

        ei_x_encode_tuple_header(&x, 2);
        ei_x_encode_atom(&x, "ok");

        BDBCUR *cur = tcbdbcurnew(bdb);
        for (idx=0; idx<rcount; idx++) {
          key = tclistval(range, idx, &key_size);
          TCLIST *vals = tcbdbget4(bdb, key, key_size);
          if (vals) {
            int j;
            for (j=0; j<tclistnum(vals); j++) {
              ei_x_encode_list_header(&x, 1);
              value_tmp = tclistval(vals, j, &value_size);
              ei_x_encode_binary(&x, value_tmp, value_size);
              if (max >= 0 && ++cnt >= max) break;
            }
            tclistdel(vals);
          }
          idx++;
        }
        tcbdbcurdel(cur);
        tclistdel(range);
        ei_x_encode_empty_list(&x);

      } else
        return ei_error(&x, "invalid_argument", res, res_size);
      break;

    case SYNC:
      // sync()
      if (!tcbdbsync(bdb))
        return ei_error(&x, tcbdberrmsg(tcbdbecode(bdb)), res, res_size);
      ei_x_encode_atom(&x, "ok");
      break;

    case INFO:
      // info()
      ei_x_encode_tuple_header(&x, 3);
      ei_x_encode_atom(&x, "ok");
      long_tmp = tcbdbrnum(bdb);
      ei_x_encode_longlong(&x, long_tmp);
      long_tmp = tcbdbfsiz(bdb);
      ei_x_encode_longlong(&x, long_tmp);
      break;

    case ITERATE:
      // Read(none) -> {ok, Key} | {error, not_found}
      // Read(Key::binary()) -> {ok, Key} | {error, not_found}
      ei_get_type(buf, &index, &tp, &sz);
      BDBCUR *cur = tcbdbcurnew(bdb);
      if (tp == ERL_BINARY_EXT && sz <= MAX_KEY_SIZE) {
        ei_decode_binary(buf, &index, &key[0], &key_size);
        rs = tcbdbcurjump(cur, &key[0], key_size) && tcbdbcurnext(cur);
      } else
        rs = tcbdbcurfirst(cur);
      if (rs) {
        int key_size;
        const char *key = tcbdbcurkey3(cur, &key_size);

        ei_x_encode_tuple_header(&x, 2);
        ei_x_encode_atom(&x, "ok");
        ei_x_encode_binary(&x, key, key_size);
        tcbdbcurdel(cur);
      } else {
        tcbdbcurdel(cur);
        return ei_error(&x, "not_found", res, res_size);
      }
      break;

    case VANISH:
      // vanish() -> ok
      if (!tcbdbvanish(bdb))
        return ei_error(&x, tcbdberrmsg(tcbdbecode(bdb)), res, res_size);
      ei_x_encode_atom(&x, "ok");
      break;

    case BACKUP:
      // backup(path::string()) -> ok | {error, Reason}
      ei_get_type(buf, &index, &tp, &sz);
      if (tp == ERL_STRING_EXT) {
        char *file = driver_alloc(sz + 1);
        ei_decode_string(buf, &index, file);
        if (tcbdbcopy(driver->bdb, file))
          ei_x_encode_atom(&x, "ok");
        else
          return ei_error(&x, tcbdberrmsg(tcbdbecode(bdb)), res, res_size);
      } else
        return ei_error(&x, "invalid_argument", res, res_size);
      break;

    default:
      return ei_error(&x, "invalid_command", res, res_size);
  }

  if (res_size < x.index)
    (*res) = (char *)driver_alloc(x.index);
  int n = x.index;
  memcpy(*res, x.buff, x.index);
  ei_x_free(&x);
  return n;
};

static ErlDrvEntry tcbdb_driver_entry = {
  NULL,                             /* init */
  init, 
  stop, 
  output,                           /* output */
  NULL,                             /* ready_input */
  NULL,                             /* ready_output */ 
  "tc",                             /* the name of the driver */
  NULL,                             /* finish */
  NULL,                             /* handle */
  control,                          /* control */
  NULL,                             /* timeout */
  NULL,                             /* outputv */
  NULL,                             /* ready_async */
  NULL,                             /* flush */
  NULL,                             /* call */
  NULL,                             /* event */
  ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
  ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT(tcbdb_driver) {
  return &tcbdb_driver_entry;
}

/* SQLite file-length enforcement only. No PipeStream wire or state-machine code. */
#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define GUARD_NAME "pipestream-java-bounded-unix-v1"
#define GUARDS 64
#define POLICY_BYTES 72
#define GRANULE 65536

typedef struct Guard {
  char path[PATH_MAX];
  unsigned char policy[POLICY_BYTES];
  sqlite3_int64 limits[4]; /* main, WAL, journal, shared memory */
  unsigned refs;
  unsigned tickets;
} Guard;

typedef struct GuardedFile {
  sqlite3_file base;
  sqlite3_file *real;
  Guard *guard;
  sqlite3_int64 limit;
} GuardedFile;

static pthread_mutex_t registry_lock = PTHREAD_MUTEX_INITIALIZER;
static Guard *guards[GUARDS];
static sqlite3_vfs *parent_vfs;
static sqlite3_vfs guarded_vfs;

static int path_state(const char *path, sqlite3_int64 limit) {
  struct stat info;
  if (lstat(path, &info) != 0) return errno == ENOENT ? SQLITE_OK : SQLITE_IOERR_FSTAT;
  if (!S_ISREG(info.st_mode) || info.st_nlink > 1) return SQLITE_CANTOPEN;
  return info.st_size < 0 || info.st_size > limit ? SQLITE_FULL : SQLITE_OK;
}

static int policy_matches(const Guard *guard) {
  char path[PATH_MAX + 16];
  unsigned char bytes[POLICY_BYTES + 1];
  (void)snprintf(path, sizeof(path), "%s.psjlimits", guard->path);
  if (path_state(path, POLICY_BYTES) != SQLITE_OK) return SQLITE_CANTOPEN;
  FILE *file = fopen(path, "rb");
  if (!file) return SQLITE_CANTOPEN;
  size_t count = fread(bytes, 1, sizeof(bytes), file);
  int failed = ferror(file);
  int closed = fclose(file);
  return !failed && closed == 0 && count == POLICY_BYTES &&
      memcmp(bytes, guard->policy, POLICY_BYTES) == 0 ? SQLITE_OK : SQLITE_CANTOPEN;
}

static int guard_paths(const Guard *guard) {
  static const char *const suffixes[] = {"", "-wal", "-journal", "-shm"};
  int result = policy_matches(guard);
  for (int index = 0; result == SQLITE_OK && index < 4; ++index) {
    char path[PATH_MAX + 16];
    (void)snprintf(path, sizeof(path), "%s%s", guard->path, suffixes[index]);
    result = path_state(path, guard->limits[index]);
  }
  return result;
}

/* The caller owns registry_lock. Each open file and registration has one ref. */
static void release_guard(Guard *guard) {
  if (--guard->refs != 0) return;
  for (int i = 0; i < GUARDS; ++i) if (guards[i] == guard) guards[i] = NULL;
  sqlite3_free(guard);
}

static Guard *lookup(const char *name, int *kind) {
  if (!name) return NULL;
  for (int i = 0; i < GUARDS; ++i) {
    Guard *guard = guards[i];
    if (!guard) continue;
    size_t length = strlen(guard->path);
    if (strncmp(name, guard->path, length) != 0) continue;
    const char *suffix = name + length;
    if (!*suffix) *kind = 0;
    else if (strcmp(suffix, "-wal") == 0) *kind = 1;
    else if (strcmp(suffix, "-journal") == 0) *kind = 2;
    else if (strcmp(suffix, "-shm") == 0) *kind = 3;
    else continue;
    return guard;
  }
  return NULL;
}

static int close_file(sqlite3_file *file) {
  GuardedFile *f = (GuardedFile *)file;
  int result = f->real->pMethods->xClose(f->real);
  sqlite3_free(f->real);
  pthread_mutex_lock(&registry_lock);
  release_guard(f->guard);
  pthread_mutex_unlock(&registry_lock);
  f->base.pMethods = NULL;
  return result;
}
static int read_file(sqlite3_file *file, void *buffer, int count, sqlite3_int64 offset) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xRead(f->real, buffer, count, offset);
}
static int write_file(sqlite3_file *file, const void *buffer, int count, sqlite3_int64 offset) {
  GuardedFile *f = (GuardedFile *)file;
  if (offset < 0 || count < 0 || offset > f->limit || count > f->limit - offset) return SQLITE_FULL;
  return f->real->pMethods->xWrite(f->real, buffer, count, offset);
}
static int truncate_file(sqlite3_file *file, sqlite3_int64 length) {
  GuardedFile *f = (GuardedFile *)file;
  if (length < 0 || length > f->limit) return SQLITE_FULL;
  return f->real->pMethods->xTruncate(f->real, length);
}
static int sync_file(sqlite3_file *file, int flags) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xSync(f->real, flags);
}
static int file_size(sqlite3_file *file, sqlite3_int64 *length) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xFileSize(f->real, length);
}
static int lock_file(sqlite3_file *file, int flags) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xLock(f->real, flags);
}
static int unlock_file(sqlite3_file *file, int flags) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xUnlock(f->real, flags);
}
static int reserved_lock(sqlite3_file *file, int *result) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xCheckReservedLock(f->real, result);
}
static int control_file(sqlite3_file *file, int op, void *arg) {
  GuardedFile *f = (GuardedFile *)file;
  if (op == SQLITE_FCNTL_SIZE_HINT) {
    sqlite3_int64 size = *(sqlite3_int64 *)arg;
    return size < 0 || size > f->limit ? SQLITE_FULL : SQLITE_OK;
  }
  if (op == SQLITE_FCNTL_CHUNK_SIZE) {
    int zero = 0;
    return f->real->pMethods->xFileControl(f->real, op, &zero);
  }
  if (op == SQLITE_FCNTL_MMAP_SIZE) {
    sqlite3_int64 zero = 0;
    int result = f->real->pMethods->xFileControl(f->real, op, &zero);
    *(sqlite3_int64 *)arg = 0;
    return result;
  }
  switch (op) {
    case SQLITE_FCNTL_LOCKSTATE: case SQLITE_FCNTL_LAST_ERRNO:
    case SQLITE_FCNTL_PERSIST_WAL: case SQLITE_FCNTL_POWERSAFE_OVERWRITE:
    case SQLITE_FCNTL_VFSNAME: case SQLITE_FCNTL_HAS_MOVED:
    case SQLITE_FCNTL_LOCK_TIMEOUT: case SQLITE_FCNTL_EXTERNAL_READER:
      return f->real->pMethods->xFileControl(f->real, op, arg);
    default: return SQLITE_NOTFOUND;
  }
}
static int sector_size(sqlite3_file *file) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xSectorSize(f->real);
}
static int characteristics(sqlite3_file *file) {
  (void)file;
  return 0; /* No atomic-write capability may bypass guarded writes. */
}
static int shm_map(sqlite3_file *file, int page, int size, int extend, void volatile **out) {
  GuardedFile *f = (GuardedFile *)file;
  *out = NULL;
  if (page < 0 || size != 32768) return SQLITE_IOERR_SHMSIZE;
  sqlite3_int64 required = ((sqlite3_int64)page + 1) * size;
  required = ((required + GRANULE - 1) / GRANULE) * GRANULE;
  if (required > f->guard->limits[3]) return SQLITE_FULL;
  return f->real->pMethods->xShmMap(f->real, page, size, extend, out);
}
static int shm_lock(sqlite3_file *file, int offset, int count, int flags) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xShmLock(f->real, offset, count, flags);
}
static void shm_barrier(sqlite3_file *file) {
  GuardedFile *f = (GuardedFile *)file;
  f->real->pMethods->xShmBarrier(f->real);
}
static int shm_unmap(sqlite3_file *file, int erase) {
  GuardedFile *f = (GuardedFile *)file;
  return f->real->pMethods->xShmUnmap(f->real, erase);
}
static const sqlite3_io_methods methods = {
  .iVersion = 2, .xClose = close_file, .xRead = read_file, .xWrite = write_file,
  .xTruncate = truncate_file, .xSync = sync_file, .xFileSize = file_size,
  .xLock = lock_file, .xUnlock = unlock_file, .xCheckReservedLock = reserved_lock,
  .xFileControl = control_file, .xSectorSize = sector_size,
  .xDeviceCharacteristics = characteristics, .xShmMap = shm_map,
  .xShmLock = shm_lock, .xShmBarrier = shm_barrier, .xShmUnmap = shm_unmap
};
static const sqlite3_io_methods journal_methods = {
  .iVersion = 1, .xClose = close_file, .xRead = read_file, .xWrite = write_file,
  .xTruncate = truncate_file, .xSync = sync_file, .xFileSize = file_size,
  .xLock = lock_file, .xUnlock = unlock_file, .xCheckReservedLock = reserved_lock,
  .xFileControl = control_file, .xSectorSize = sector_size,
  .xDeviceCharacteristics = characteristics
};

static int open_file(sqlite3_vfs *vfs, const char *name, sqlite3_file *file, int flags, int *out) {
  (void)vfs;
  GuardedFile *f = (GuardedFile *)file;
  memset(f, 0, sizeof(*f));
  int kind = -1;
  pthread_mutex_lock(&registry_lock);
  Guard *guard = lookup(name, &kind);
  if (guard && guard->refs < 1048576) ++guard->refs;
  else guard = NULL;
  pthread_mutex_unlock(&registry_lock);
  if (!guard) return SQLITE_CANTOPEN;
  int allowed = kind == 0 ? SQLITE_OPEN_MAIN_DB : kind == 1 ? SQLITE_OPEN_WAL : SQLITE_OPEN_MAIN_JOURNAL;
  int result = (kind > 2 || (flags & allowed) == 0 || (flags & SQLITE_OPEN_DELETEONCLOSE))
      ? SQLITE_CANTOPEN : guard_paths(guard);
  if (result == SQLITE_OK) {
    f->real = sqlite3_malloc64((sqlite3_uint64)parent_vfs->szOsFile);
    if (!f->real) result = SQLITE_NOMEM;
    else {
      memset(f->real, 0, (size_t)parent_vfs->szOsFile);
      result = parent_vfs->xOpen(parent_vfs, name, f->real, flags, out);
      if (result == SQLITE_OK && kind == 0 && (f->real->pMethods->iVersion < 2 ||
          !f->real->pMethods->xShmMap || !f->real->pMethods->xShmLock ||
          !f->real->pMethods->xShmBarrier || !f->real->pMethods->xShmUnmap)) result = SQLITE_CANTOPEN;
    }
  }
  if (result != SQLITE_OK) {
    if (f->real && f->real->pMethods) (void)f->real->pMethods->xClose(f->real);
    sqlite3_free(f->real);
    pthread_mutex_lock(&registry_lock);
    release_guard(guard);
    pthread_mutex_unlock(&registry_lock);
    return result;
  }
  f->guard = guard;
  f->limit = guard->limits[kind];
  f->base.pMethods = kind == 0 ? &methods : &journal_methods;
  return SQLITE_OK;
}

static int delete_file(sqlite3_vfs *vfs, const char *name, int sync_dir) {
  (void)vfs;
  int kind = -1;
  pthread_mutex_lock(&registry_lock);
  Guard *guard = lookup(name, &kind);
  int result = guard && kind > 0 ? SQLITE_OK : SQLITE_IOERR_DELETE;
  pthread_mutex_unlock(&registry_lock);
  return result == SQLITE_OK ? parent_vfs->xDelete(parent_vfs, name, sync_dir) : result;
}
static int access_file(sqlite3_vfs *vfs, const char *name, int flags, int *out) {
  (void)vfs;
  return parent_vfs->xAccess(parent_vfs, name, flags, out);
}
static int full_path(sqlite3_vfs *vfs, const char *name, int length, char *out) {
  (void)vfs;
  return parent_vfs->xFullPathname(parent_vfs, name, length, out);
}
static void *dl_open(sqlite3_vfs *vfs, const char *name) { (void)vfs; return parent_vfs->xDlOpen(parent_vfs, name); }
static void dl_error(sqlite3_vfs *vfs, int length, char *out) { (void)vfs; parent_vfs->xDlError(parent_vfs, length, out); }
static void (*dl_sym(sqlite3_vfs *vfs, void *handle, const char *name))(void) {
  (void)vfs; return parent_vfs->xDlSym(parent_vfs, handle, name);
}
static void dl_close(sqlite3_vfs *vfs, void *handle) { (void)vfs; parent_vfs->xDlClose(parent_vfs, handle); }
static int randomness(sqlite3_vfs *vfs, int length, char *out) { (void)vfs; return parent_vfs->xRandomness(parent_vfs, length, out); }
static int sleep_vfs(sqlite3_vfs *vfs, int micros) { (void)vfs; return parent_vfs->xSleep(parent_vfs, micros); }
static int time_vfs(sqlite3_vfs *vfs, double *out) { (void)vfs; return parent_vfs->xCurrentTime(parent_vfs, out); }
static int last_error(sqlite3_vfs *vfs, int length, char *out) { (void)vfs; return parent_vfs->xGetLastError(parent_vfs, length, out); }
static int time_int(sqlite3_vfs *vfs, sqlite3_int64 *out) { (void)vfs; return parent_vfs->xCurrentTimeInt64(parent_vfs, out); }

static const char *path_argument(sqlite3_value *value) {
  if (sqlite3_value_type(value) != SQLITE_TEXT) return NULL;
  const char *path = (const char *)sqlite3_value_text(value);
  int length = sqlite3_value_bytes(value);
  if (!path || length < 2 || length >= PATH_MAX - 16 || path[0] != '/' ||
      (int)strlen(path) != length) return NULL;
  return path;
}
static void register_path(sqlite3_context *context, int count, sqlite3_value **values) {
  (void)count;
  const char *path = path_argument(values[0]);
  Guard proposed = {0};
  int result = SQLITE_OK;
  if (!path || sqlite3_value_type(values[5]) != SQLITE_BLOB ||
      sqlite3_value_bytes(values[5]) != POLICY_BYTES) result = SQLITE_MISUSE;
  for (int i = 0; i < 4; ++i) {
    proposed.limits[i] = sqlite3_value_int64(values[i + 1]);
    if (sqlite3_value_type(values[i + 1]) != SQLITE_INTEGER || proposed.limits[i] <= 0 ||
        proposed.limits[i] % GRANULE || proposed.limits[i] > (i == 3 ? 16777216LL : 17179869184LL)) result = SQLITE_MISUSE;
  }
  if (result == SQLITE_OK) {
    (void)snprintf(proposed.path, sizeof(proposed.path), "%s", path);
    const void *bytes = sqlite3_value_blob(values[5]);
    if (!bytes) { sqlite3_result_error_nomem(context); return; }
    memcpy(proposed.policy, bytes, POLICY_BYTES);
    if (memcmp(proposed.policy, "PSJDB001", 8) != 0) result = SQLITE_MISUSE;
    for (int i = 0; i < 4; ++i) {
      uint64_t limit = 0;
      for (int j = 0; j < 8; ++j) limit = (limit << 8) | proposed.policy[8 + i * 8 + j];
      if (limit != (uint64_t)proposed.limits[i]) result = SQLITE_MISUSE;
    }
  }
  if (result != SQLITE_OK) { sqlite3_result_error_code(context, result); return; }
  pthread_mutex_lock(&registry_lock);
  int empty = -1;
  Guard *existing = NULL;
  for (int i = 0; i < GUARDS; ++i) {
    if (!guards[i]) empty = i;
    else if (strcmp(guards[i]->path, path) == 0) existing = guards[i];
  }
  if (existing) {
    if (memcmp(existing->policy, proposed.policy, POLICY_BYTES) != 0) result = SQLITE_CANTOPEN;
    else if (existing->refs >= 1048576) result = SQLITE_FULL;
    else { ++existing->refs; ++existing->tickets; }
  } else if (empty < 0) result = SQLITE_FULL;
  else {
    Guard *guard = sqlite3_malloc64(sizeof(Guard));
    if (!guard) result = SQLITE_NOMEM;
    else { *guard = proposed; guard->refs = guard->tickets = 1; guards[empty] = guard; }
  }
  pthread_mutex_unlock(&registry_lock);
  if (result == SQLITE_OK) sqlite3_result_int(context, 1);
  else sqlite3_result_error_code(context, result);
}
static void unregister_path(sqlite3_context *context, int count, sqlite3_value **values) {
  (void)count;
  const char *path = path_argument(values[0]);
  int kind = -1;
  int result = SQLITE_MISUSE;
  pthread_mutex_lock(&registry_lock);
  Guard *guard = lookup(path, &kind);
  if (guard && kind == 0 && guard->tickets) {
    --guard->tickets; release_guard(guard); result = SQLITE_OK;
  }
  pthread_mutex_unlock(&registry_lock);
  if (result == SQLITE_OK) sqlite3_result_int(context, 1);
  else sqlite3_result_error_code(context, result);
}

__attribute__((visibility("default")))
int sqlite3_pipestream_init(sqlite3 *database, char **error, const sqlite3_api_routines *api) {
  (void)error;
  pthread_mutex_lock(&registry_lock);
  if (sqlite3_api && sqlite3_api != api) { pthread_mutex_unlock(&registry_lock); return SQLITE_MISUSE; }
  if (!sqlite3_api) { SQLITE_EXTENSION_INIT2(api); }
  pthread_mutex_unlock(&registry_lock);
  /* Re-audit Unix file controls and SHM growth when the pinned engine changes. */
  if (sqlite3_libversion_number() != SQLITE_VERSION_NUMBER) return SQLITE_MISMATCH;
  int result = sqlite3_create_function_v2(database, "pipestream_guard_register", 6, SQLITE_UTF8 | SQLITE_DIRECTONLY,
      NULL, register_path, NULL, NULL, NULL);
  if (result == SQLITE_OK) result = sqlite3_create_function_v2(database, "pipestream_guard_unregister", 1,
      SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, unregister_path, NULL, NULL, NULL);
  if (result != SQLITE_OK) {
    (void)sqlite3_create_function_v2(database, "pipestream_guard_register", 6, SQLITE_UTF8, NULL, NULL, NULL, NULL, NULL);
    (void)sqlite3_create_function_v2(database, "pipestream_guard_unregister", 1, SQLITE_UTF8, NULL, NULL, NULL, NULL, NULL);
    return result;
  }
  pthread_mutex_lock(&registry_lock);
  if (!parent_vfs) {
    parent_vfs = sqlite3_vfs_find("unix");
    long page = sysconf(_SC_PAGESIZE);
    if (!parent_vfs || parent_vfs->iVersion < 2 || page <= 0 || page > GRANULE) result = SQLITE_CANTOPEN;
    else {
      guarded_vfs = (sqlite3_vfs){.iVersion = 2, .szOsFile = (int)sizeof(GuardedFile),
        .mxPathname = parent_vfs->mxPathname, .zName = GUARD_NAME,
        .xOpen = open_file, .xDelete = delete_file, .xAccess = access_file,
        .xFullPathname = full_path, .xDlOpen = dl_open, .xDlError = dl_error,
        .xDlSym = dl_sym, .xDlClose = dl_close, .xRandomness = randomness,
        .xSleep = sleep_vfs, .xCurrentTime = time_vfs, .xGetLastError = last_error,
        .xCurrentTimeInt64 = time_int};
      result = sqlite3_vfs_register(&guarded_vfs, 0);
    }
    if (result != SQLITE_OK) parent_vfs = NULL;
  }
  pthread_mutex_unlock(&registry_lock);
  if (result != SQLITE_OK) {
    (void)sqlite3_create_function_v2(database, "pipestream_guard_register", 6, SQLITE_UTF8, NULL, NULL, NULL, NULL, NULL);
    (void)sqlite3_create_function_v2(database, "pipestream_guard_unregister", 1, SQLITE_UTF8, NULL, NULL, NULL, NULL, NULL);
  }
  return result == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : result;
}

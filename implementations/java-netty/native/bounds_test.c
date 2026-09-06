/* Direct SQLite file methods, not a mock filesystem or a protocol oracle. */
#include <sqlite3.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define CHECK(condition) do { if (!(condition)) { fprintf(stderr, "line %d: %s\n", __LINE__, #condition); exit(1); } } while (0)
#define OK(call) CHECK((call) == SQLITE_OK)
#define CAP 65536
#define MAIN_CAP (2 * 1024 * 1024)

static sqlite3_int64 length(const char *path) {
  struct stat info;
  return stat(path, &info) == 0 ? info.st_size : 0;
}

static void make_policy(const char *path, unsigned char *policy) {
  memcpy(policy, "PSJDB002", 8);
  for (int i = 0; i < 4; ++i) {
    uint64_t value = i == 0 ? MAIN_CAP : CAP;
    for (int j = 7; j >= 0; --j) { policy[8 + i * 8 + j] = (unsigned char)(value & 255); value >>= 8; }
  }
  /* This lower-level guard accepts already-validated bytes from Java. The Java
     integration tests separately require the SHA-256 policy checksum. */
  int fd = open(path, O_WRONLY | O_CREAT | O_EXCL, 0600);
  CHECK(fd >= 0);
  CHECK(write(fd, policy, 72) == 72);
  CHECK(fsync(fd) == 0);
  CHECK(close(fd) == 0);
}

static void check_images(sqlite3 *database) {
  OK(sqlite3_exec(database, "CREATE TABLE images(image BLOB NOT NULL) STRICT; INSERT INTO images VALUES(zeroblob(4))", NULL, NULL, NULL));
  CHECK(sqlite3_exec(database, "SELECT pipestream_blob_replace('images','image',1,x'01020304')", NULL, NULL, NULL) == SQLITE_ERROR);
  CHECK(sqlite3_exec(database, "SELECT pipestream_wal_ceiling(1)", NULL, NULL, NULL) == SQLITE_ERROR);
  OK(sqlite3_exec(database, "BEGIN IMMEDIATE", NULL, NULL, NULL));
  CHECK(sqlite3_exec(database, "SELECT pipestream_wal_ceiling(-1)", NULL, NULL, NULL) == SQLITE_MISMATCH);
  CHECK(sqlite3_exec(database, "SELECT pipestream_wal_ceiling(65537)", NULL, NULL, NULL) == SQLITE_MISMATCH);
  OK(sqlite3_exec(database, "SELECT pipestream_wal_ceiling(65536)", NULL, NULL, NULL));
  CHECK(sqlite3_exec(database, "SELECT pipestream_blob_replace('images','image',1,x'0102')", NULL, NULL, NULL) == SQLITE_ERROR);
  CHECK(sqlite3_exec(database, "SELECT pipestream_blob_replace('images','image',1,zeroblob(16777217))", NULL, NULL, NULL) == SQLITE_TOOBIG);
  CHECK(sqlite3_exec(database, "SELECT pipestream_blob_replace('images'||char(0),'image',1,x'01020304')", NULL, NULL, NULL) == SQLITE_MISMATCH);
  OK(sqlite3_exec(database, "SELECT pipestream_blob_replace('images','image',1,x'01020304')", NULL, NULL, NULL));
  sqlite3_stmt *statement = NULL;
  OK(sqlite3_prepare_v2(database, "SELECT hex(image) FROM images", -1, &statement, NULL));
  CHECK(sqlite3_step(statement) == SQLITE_ROW);
  CHECK(strcmp((const char *)sqlite3_column_text(statement, 0), "01020304") == 0);
  OK(sqlite3_finalize(statement));
  OK(sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL));
  OK(sqlite3_prepare_v2(database, "SELECT hex(image) FROM images", -1, &statement, NULL));
  CHECK(sqlite3_step(statement) == SQLITE_ROW);
  CHECK(strcmp((const char *)sqlite3_column_text(statement, 0), "00000000") == 0);
  OK(sqlite3_finalize(statement));
}

int main(int argc, char **argv) {
  CHECK(argc == 2);
  char directory[] = "/tmp/pipestream-sqlite-native-XXXXXX";
  CHECK(mkdtemp(directory));
  char path[256], policy_path[280], uri[340], sidecar[280];
  (void)snprintf(path, sizeof(path), "%s/state.db", directory);
  (void)snprintf(policy_path, sizeof(policy_path), "%s.psjlimits", path);
  unsigned char policy[72] = {0};
  make_policy(policy_path, policy);
  sqlite3 *bootstrap = NULL, *database = NULL;
  OK(sqlite3_open(":memory:", &bootstrap));
  OK(sqlite3_enable_load_extension(bootstrap, 1));
  char *error = NULL;
  int loaded = sqlite3_load_extension(bootstrap, argv[1], "sqlite3_pipestream_init", &error);
  if (loaded != SQLITE_OK) fprintf(stderr, "%s\n", error);
  sqlite3_free(error);
  OK(loaded);
  OK(sqlite3_enable_load_extension(bootstrap, 0));
  sqlite3_stmt *statement = NULL;
  OK(sqlite3_prepare_v2(bootstrap, "SELECT pipestream_guard_register(?,?,?,?,?,?)", -1, &statement, NULL));
  OK(sqlite3_bind_text(statement, 1, path, -1, SQLITE_STATIC));
  for (int i = 0; i < 4; ++i) OK(sqlite3_bind_int(statement, i + 2, i == 0 ? MAIN_CAP : CAP));
  OK(sqlite3_bind_blob(statement, 6, policy, 72, SQLITE_STATIC));
  CHECK(sqlite3_step(statement) == SQLITE_ROW);
  OK(sqlite3_finalize(statement));
  (void)snprintf(uri, sizeof(uri), "file:%s?vfs=pipestream-java-bounded-unix-v2", path);
  OK(sqlite3_open_v2(uri, &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL));
  OK(sqlite3_prepare_v2(bootstrap, "SELECT pipestream_guard_unregister(?)", -1, &statement, NULL));
  OK(sqlite3_bind_text(statement, 1, path, -1, SQLITE_STATIC));
  CHECK(sqlite3_step(statement) == SQLITE_ROW);
  OK(sqlite3_finalize(statement));
  OK(sqlite3_close(bootstrap)); /* Registered VFS code must remain loaded. */
  OK(sqlite3_exec(database, "CREATE TABLE retained(value INTEGER)", NULL, NULL, NULL));
  check_images(database);

  sqlite3_vfs *vfs = sqlite3_vfs_find("pipestream-java-bounded-unix-v2");
  CHECK(vfs && vfs != sqlite3_vfs_find(NULL));
  sqlite3_file *file = NULL;
  OK(sqlite3_file_control(database, "main", SQLITE_FCNTL_FILE_POINTER, &file));
  CHECK(file && file->pMethods->iVersion == 2 && !file->pMethods->xFetch);
  sqlite3_int64 before = length(path);
  unsigned char byte = 0;
  CHECK(file->pMethods->xWrite(file, &byte, 1, MAIN_CAP) == SQLITE_FULL);
  CHECK(file->pMethods->xWrite(file, &byte, 1, INT64_MAX) == SQLITE_FULL);
  CHECK(file->pMethods->xWrite(file, &byte, 1, -1) == SQLITE_FULL);
  CHECK(file->pMethods->xWrite(file, &byte, -1, 0) == SQLITE_FULL);
  CHECK(file->pMethods->xTruncate(file, MAIN_CAP + 1) == SQLITE_FULL);
  CHECK(file->pMethods->xTruncate(file, -1) == SQLITE_FULL);
  sqlite3_int64 hint = MAIN_CAP + 1;
  CHECK(file->pMethods->xFileControl(file, SQLITE_FCNTL_SIZE_HINT, &hint) == SQLITE_FULL);
  hint = MAIN_CAP;
  OK(file->pMethods->xFileControl(file, SQLITE_FCNTL_SIZE_HINT, &hint));
  CHECK(length(path) == before);
  hint = MAIN_CAP;
  OK(file->pMethods->xFileControl(file, SQLITE_FCNTL_MMAP_SIZE, &hint));
  CHECK(hint == 0);
  int chunk = MAIN_CAP;
  OK(file->pMethods->xFileControl(file, SQLITE_FCNTL_CHUNK_SIZE, &chunk));
  void volatile *mapped = NULL;
  CHECK(file->pMethods->xShmMap(file, 2, 32768, 1, &mapped) == SQLITE_FULL);
  CHECK(!mapped);
  CHECK(file->pMethods->xShmMap(file, -1, 32768, 1, &mapped) == SQLITE_IOERR_SHMSIZE);
  CHECK(file->pMethods->xShmMap(file, 0, 65536, 1, &mapped) == SQLITE_IOERR_SHMSIZE);
  CHECK(!mapped);
  OK(file->pMethods->xShmMap(file, 1, 32768, 1, &mapped));
  CHECK(mapped);
  (void)snprintf(sidecar, sizeof(sidecar), "%s-shm", path);
  CHECK(length(sidecar) <= CAP);
  OK(file->pMethods->xShmUnmap(file, 1));

  for (int kind = 0; kind < 2; ++kind) {
    (void)snprintf(sidecar, sizeof(sidecar), "%s%s", path, kind ? "-journal" : "-wal");
    OK(sqlite3_exec(database, kind ? "PRAGMA journal_mode=DELETE" : "PRAGMA journal_mode=WAL", NULL, NULL, NULL));
    OK(sqlite3_exec(database, "BEGIN IMMEDIATE; INSERT INTO retained VALUES(1)", NULL, NULL, NULL));
    /* Obtain SQLite's actual journal/WAL, including its pager-owned xOpen name.
       sqlite3_create_filename does not create that association. */
    sqlite3_file *journal = NULL;
    OK(sqlite3_file_control(database, "main", SQLITE_FCNTL_JOURNAL_POINTER, &journal));
    CHECK(journal && journal->pMethods);
    CHECK(journal->pMethods->iVersion == 1);
    if (!kind) {
      OK(sqlite3_exec(database, "SELECT pipestream_wal_ceiling(32)", NULL, NULL, NULL));
      CHECK(journal->pMethods->xWrite(journal, &byte, 1, 32) == SQLITE_FULL);
      CHECK(journal->pMethods->xTruncate(journal, 33) == SQLITE_FULL);
      hint = 33;
      CHECK(journal->pMethods->xFileControl(journal, SQLITE_FCNTL_SIZE_HINT, &hint) == SQLITE_FULL);
      OK(sqlite3_exec(database, "SELECT pipestream_wal_ceiling(65536)", NULL, NULL, NULL));
    }
    CHECK(journal->pMethods->xWrite(journal, &byte, 1, CAP) == SQLITE_FULL);
    CHECK(journal->pMethods->xTruncate(journal, CAP + 1) == SQLITE_FULL);
    OK(journal->pMethods->xFileControl(journal, SQLITE_FCNTL_CHUNK_SIZE, &chunk));
    OK(journal->pMethods->xWrite(journal, &byte, 1, CAP - 1));
    CHECK(length(sidecar) == CAP);
    /* The direct write is beyond the valid journal contents. Roll back before
       SQLite owns subsequent journal recovery or a mode change. */
    OK(sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL));
    if (!kind) OK(sqlite3_exec(database, "PRAGMA wal_checkpoint(TRUNCATE)", NULL, NULL, NULL));
  }
  sqlite3_file *foreign = sqlite3_malloc(vfs->szOsFile);
  CHECK(foreign);
  CHECK(vfs->xOpen(vfs, NULL, foreign, SQLITE_OPEN_TEMP_DB | SQLITE_OPEN_DELETEONCLOSE, NULL) == SQLITE_CANTOPEN);
  sqlite3_free(foreign);
  OK(sqlite3_close(database));
  /* File references were the final owners; unregistered reopen must now refuse. */
  CHECK(sqlite3_open_v2(uri, &database, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, NULL) == SQLITE_CANTOPEN);
  OK(sqlite3_close(database));
  CHECK(unlink(path) == 0);
  CHECK(unlink(policy_path) == 0);
  CHECK(rmdir(directory) == 0);
  puts("PASS guarded main/WAL/journal/shared-memory writes and growth bypass controls");
  return 0;
}

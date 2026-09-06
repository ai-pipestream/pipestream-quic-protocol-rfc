CREATE TABLE authority (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  name TEXT NOT NULL,
  last_generation INTEGER NOT NULL CHECK(last_generation >= 0),
  greatest_utc INTEGER NOT NULL CHECK(greatest_utc >= 0),
  policy BLOB NOT NULL,
  store_id BLOB NOT NULL CHECK(length(store_id)=16),
  payload_path TEXT
) STRICT;
CREATE TABLE owners (
  owner TEXT PRIMARY KEY NOT NULL,
  last_creation INTEGER NOT NULL CHECK(last_creation >= 0)
) STRICT;
CREATE TABLE sessions (
  generation INTEGER PRIMARY KEY CHECK(generation > 0),
  owner TEXT NOT NULL REFERENCES owners(owner),
  creation_sequence INTEGER NOT NULL CHECK(creation_sequence > 0),
  policy BLOB NOT NULL,
  limits BLOB NOT NULL,
  results INTEGER NOT NULL CHECK(results IN (0,1)),
  control_limit INTEGER NOT NULL CHECK(control_limit BETWEEN 4096 AND 1048576),
  object_limit INTEGER NOT NULL CHECK(object_limit >= 0),
  required_control INTEGER NOT NULL DEFAULT 4096 CHECK(required_control BETWEEN 4096 AND 1048576),
  required_object INTEGER NOT NULL DEFAULT 0 CHECK(required_object >= 0),
  entities INTEGER NOT NULL DEFAULT 0 CHECK(entities >= 0),
  operations INTEGER NOT NULL DEFAULT 0 CHECK(operations >= 0),
  last_scope INTEGER NOT NULL DEFAULT 0 CHECK(last_scope >= 0),
  revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)),
  UNIQUE(owner, creation_sequence)
) STRICT;
CREATE INDEX session_owner ON sessions(owner);
CREATE TABLE scopes (
  generation INTEGER NOT NULL REFERENCES sessions(generation),
  scope INTEGER NOT NULL CHECK(scope >= 0),
  producer INTEGER NOT NULL CHECK(producer IN (0,1)),
  parent BLOB,
  last_entity INTEGER NOT NULL DEFAULT 0 CHECK(last_entity >= 0),
  declared INTEGER NOT NULL DEFAULT 0 CHECK(declared >= 0),
  seal BLOB CHECK(seal IS NULL OR length(seal) = 32),
  cancelled INTEGER NOT NULL DEFAULT 0 CHECK(cancelled IN (0,1)),
  summary BLOB,
  PRIMARY KEY(generation, scope)
) STRICT;
CREATE TABLE work (
  row_id INTEGER PRIMARY KEY,
  generation INTEGER NOT NULL,
  scope INTEGER NOT NULL,
  producer INTEGER NOT NULL CHECK(producer IN (0,1)),
  entity INTEGER NOT NULL CHECK(entity > 0),
  revision INTEGER NOT NULL CHECK(revision > 0),
  view BLOB NOT NULL,
  UNIQUE(generation, scope, entity),
  FOREIGN KEY(generation, scope) REFERENCES scopes(generation, scope)
) STRICT;
CREATE TABLE operations (
  generation INTEGER NOT NULL REFERENCES sessions(generation),
  originator INTEGER NOT NULL CHECK(originator IN (0,1)),
  operation BLOB NOT NULL CHECK(length(operation) = 16),
  digest BLOB NOT NULL CHECK(length(digest) = 32),
  receipt BLOB NOT NULL,
  PRIMARY KEY(generation, originator, operation)
) STRICT;
CREATE TABLE payload_refs (
  object_key TEXT PRIMARY KEY NOT NULL CHECK(length(object_key)=32),
  generation INTEGER NOT NULL,
  scope INTEGER NOT NULL,
  entity INTEGER NOT NULL,
  purpose INTEGER NOT NULL CHECK(purpose IN (0,1)),
  FOREIGN KEY(generation,scope,entity) REFERENCES work(generation,scope,entity)
) STRICT;
PRAGMA application_id = 1347637825;
PRAGMA user_version = 2;

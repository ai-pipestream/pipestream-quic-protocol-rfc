package ai.pipestream.quic.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Model test attacking the semantic contract of {@link ObjectStore}: seeded
 * random operation sequences (put / seal / retire / list / listScopes / verify
 * / openRead, 1,000 operations per sequence, five fixed seeds) are executed identically against
 * {@link MemoryObjectStore} as the oracle and {@link FileObjectStore} as the
 * durable backend, and every observable result — success, {@link
 * StorageException.Kind} of failure, listed entity ids, listed scopes, seal
 * records, and read-back bytes — must match exactly on both. A driver-side
 * model tracks live content so seals are computed with the canonical content
 * digest over the real entity set (pinned decision 3) and sanity-checks the
 * oracle itself every few operations; any store-versus-store divergence fails
 * the test with the seed, operation index, operation, and both outcomes.
 * Deterministic: fixed seeds, no wall-clock dependencies, memory and
 * filesystem only.
 *
 * <p>The driver used to maintain an invariant that no scope with live state
 * ever sat below a sealed ancestor, working around a since-fixed
 * {@code FileObjectStore.retire} defect that recursively deleted child scopes.
 * Retirement is now per-scope on every backend (pinned decision 5), so the
 * driver mutates freely; {@link #sealedScopeRetirementLeavesChildScopesIntact}
 * pins the fixed semantics directly.
 */
final class PropertyModelStoreEquivalenceTest {
  @TempDir Path directory;

  /** Fixed seeds so every run is reproducible; each seed gets a fresh pair of stores. */
  private static final long[] SEEDS =
      {0x5eed_600dL, 0x5eed_600eL, 0x5eed_600fL, 0x5eed_6010L, 0x5eed_6011L};
  private static final int OPERATIONS = 1_000;
  private static final MessageDigest SHA_256 = sha256();

  @Test void fileStoreMatchesMemoryOracleOverRandomOperationSequences() throws Exception {
    for (long seed : SEEDS) {
      runSequence(seed);
    }
  }

  /**
   * Retiring a sealed scope removes exactly that scope's own state; child
   * scopes are independent containers and survive (pinned decision 5).
   */
  @Test
  void sealedScopeRetirementLeavesChildScopesIntact() throws Exception {
    var parent = ScopePath.of(0x51, 1);
    var child = ScopePath.of(0x51, 1, 2);
    byte[] parentPayload = new byte[] {1, 2, 3};
    byte[] childPayload = new byte[] {4, 5, 6};
    try (ObjectStore oracle = MemoryObjectStore.create();
         ObjectStore durable = FileObjectStore.open(directory.resolve("sealed-parent-repro"))) {
      for (ObjectStore store : new ObjectStore[] {oracle, durable}) {
        store.put(parent, 1, parentPayload, SHA_256.digest(parentPayload));
        store.put(child, 2, childPayload, SHA_256.digest(childPayload));
        store.sealScope(parent, SHA_256.digest(parentPayload), 1);
        store.retire(parent, 0);
      }
      assertEquals(List.of(), oracle.list(parent), "oracle: parent retired whole");
      assertEquals(List.of(), durable.list(parent), "durable: parent retired whole");
      assertEquals(List.of(2L), oracle.list(child), "oracle: child scope survives");
      assertEquals(List.of(2L), durable.list(child),
          "child scope must survive its parent's whole-scope retirement");
      assertArrayEquals(childPayload, readAll(durable, child, 2));
    }
  }

  private static byte[] readAll(ObjectStore store, ScopePath scope, long entityId)
      throws Exception {
    try (InputStream input = store.openRead(scope, entityId)) {
      return input.readAllBytes();
    }
  }

  // --------------------------------------------------------------------------
  // Driver-side model: live entities and the recorded seal, per scope.
  // --------------------------------------------------------------------------

  private static final class ScopeModel {
    final TreeMap<Long, byte[]> entities = new TreeMap<>();
    byte[] sealDigest;
    long sealCount;
  }

  private static final class Model {
    final Map<ScopePath, ScopeModel> scopes = new LinkedHashMap<>();

    ScopeModel scope(ScopePath path) {
      return scopes.computeIfAbsent(path, key -> new ScopeModel());
    }

    ScopeModel existing(ScopePath path) {
      return scopes.get(path);
    }

    void dropIfEmpty(ScopePath path) {
      ScopeModel model = scopes.get(path);
      if (model != null && model.sealDigest == null && model.entities.isEmpty()) {
        scopes.remove(path);
      }
    }
  }

  private static final class BothStores implements AutoCloseable {
    final ObjectStore oracle;
    final ObjectStore durable;

    BothStores(ObjectStore oracle, ObjectStore durable) {
      this.oracle = oracle;
      this.durable = durable;
    }

    @Override public void close() throws StorageException {
      StorageException failure = null;
      try {
        durable.close();
      } catch (StorageException closeFailure) {
        failure = closeFailure;
      }
      try {
        oracle.close();
      } catch (StorageException closeFailure) {
        if (failure == null) failure = closeFailure;
      }
      if (failure != null) throw failure;
    }
  }

  // --------------------------------------------------------------------------
  // Divergence detection: run one call on both stores, compare every observable.
  // --------------------------------------------------------------------------

  @FunctionalInterface
  private interface StoreCall<R> {
    R run(ObjectStore store) throws Exception;
  }

  /** One observed outcome: either a value or the StorageException kind thrown. */
  private static final class Outcome<R> {
    final R value;
    final StorageException.Kind failure;

    Outcome(R value, StorageException.Kind failure) {
      this.value = value;
      this.failure = failure;
    }

    boolean ok() {
      return failure == null;
    }

    String describe() {
      return failure == null ? "ok(" + render(value) + ")" : "throws " + failure;
    }
  }

  private static String render(Object value) {
    if (value == null) return "void";
    if (value instanceof byte[] bytes) return "bytes[" + bytes.length + "]";
    return value.toString();
  }

  private static boolean valuesEqual(Object left, Object right) {
    if (left instanceof byte[] a && right instanceof byte[] b) return Arrays.equals(a, b);
    // SealRecord is a record over a byte[]; its generated equals compares the
    // array by reference, so compare the record's parts semantically here.
    if (left instanceof ObjectStore.SealRecord x && right instanceof ObjectStore.SealRecord y) {
      return x.entityCount() == y.entityCount() && Arrays.equals(x.digest(), y.digest());
    }
    return java.util.Objects.equals(left, right);
  }

  private static <R> Outcome<R> observe(StoreCall<R> call, ObjectStore store) {
    try {
      return new Outcome<>(call.run(store), null);
    } catch (StorageException failure) {
      return new Outcome<>(null, failure.kind());
    } catch (Exception unexpected) {
      throw new AssertionError("unexpected checked exception " + unexpected, unexpected);
    }
  }

  private static <R> Outcome<R> both(long seed, int op, String description,
      ObjectStore oracle, ObjectStore durable, StoreCall<R> call) {
    Outcome<R> expected = observe(call, oracle);
    Outcome<R> actual = observe(call, durable);
    if (expected.ok() != actual.ok()) {
      fail("divergence [seed " + seed + " op " + op + "]: " + description
          + " — oracle " + expected.describe() + " but durable " + actual.describe());
    }
    if (expected.ok()) {
      if (!valuesEqual(expected.value, actual.value)) {
        fail("divergence [seed " + seed + " op " + op + "]: " + description
            + " — oracle " + expected.describe() + " but durable " + actual.describe());
      }
    } else if (expected.failure != actual.failure) {
      fail("divergence [seed " + seed + " op " + op + "]: " + description
          + " — oracle " + expected.describe() + " but durable " + actual.describe());
    }
    return expected;
  }

  // --------------------------------------------------------------------------
  // Operation sequence driver.
  // --------------------------------------------------------------------------

  private void runSequence(long seed) throws Exception {
    Random random = new Random(seed);
    Model model = new Model();
    List<ScopePath> scopePool = scopePool();
    try (BothStores stores = new BothStores(
        MemoryObjectStore.create(), FileObjectStore.open(directory.resolve("seed-" + seed)))) {
      for (int op = 0; op < OPERATIONS; op++) {
        int choice = random.nextInt(100);
        if (choice < 30) {
          putOp(seed, op, random, model, scopePool, stores, false);
        } else if (choice < 35) {
          putOp(seed, op, random, model, scopePool, stores, true);
        } else if (choice < 45) {
          sealOp(seed, op, random, model, scopePool, stores);
        } else if (choice < 57) {
          retireOp(seed, op, random, model, scopePool, stores);
        } else if (choice < 67) {
          verifyOp(seed, op, random, scopePool, stores);
        } else if (choice < 82) {
          listOp(seed, op, random, scopePool, stores);
        } else if (choice < 87) {
          both(seed, op, "listScopes", stores.oracle, stores.durable, store -> store.listScopes());
        } else {
          openReadOp(seed, op, random, scopePool, stores);
        }
        if (op % 50 == 49) {
          sanityCheckModel(seed, op, random, model, scopePool, stores.oracle);
        }
      }
      finalSweep(seed, model, stores);
    }
  }

  private static List<ScopePath> scopePool() {
    return List.of(
        ScopePath.root(0x51), ScopePath.root(0x52),
        ScopePath.of(0x51, 1), ScopePath.of(0x51, 2), ScopePath.of(0x52, 1),
        ScopePath.of(0x51, 1, 1), ScopePath.of(0x51, 1, 2), ScopePath.of(0x52, 1, 1));
  }

  private static ScopePath pickScope(Random random, List<ScopePath> pool) {
    // Mostly the shared pool so scopes collide; sometimes a fresh unseen scope
    // so both backends must agree on NOT_FOUND paths too.
    if (random.nextInt(100) < 5) {
      return ScopePath.root(random.nextLong() & 0xffff_ffffL);
    }
    return pool.get(random.nextInt(pool.size()));
  }

  private static long pickEntityId(Random random) {
    // Bias small ids so retire cursors hit live entities often.
    if (random.nextInt(100) < 60) return random.nextInt(32);
    return random.nextLong() & StoreLayout.MAX_ENTITY_ID;
  }

  private static byte[] randomPayload(Random random) {
    byte[] payload = new byte[random.nextInt(201)];
    random.nextBytes(payload);
    return payload;
  }

  private static long pickCursor(Random random) {
    return switch (random.nextInt(4)) {
      case 0 -> 0;
      case 1 -> random.nextInt(32);
      case 2 -> random.nextLong() & StoreLayout.MAX_ENTITY_ID;
      default -> StoreLayout.MAX_ENTITY_ID;
    };
  }

  private static void putOp(long seed, int op, Random random, Model model,
      List<ScopePath> pool, BothStores stores, boolean corruptDigest) {
    ScopePath scope = pickScope(random, pool);
    long entityId = pickEntityId(random);
    byte[] payload = randomPayload(random);
    byte[] submittedDigest = SHA_256.digest(payload);
    ScopeModel scopeModel = model.existing(scope);
    boolean sealed = scopeModel != null && scopeModel.sealDigest != null;
    byte[] expectation = null;
    if (!sealed && scopeModel != null && scopeModel.entities.containsKey(entityId)) {
      expectation = scopeModel.entities.get(entityId);
    }
    if (corruptDigest && !sealed) {
      // Submit the digest of different bytes: both backends must answer CORRUPT
      // and leave any recorded content untouched.
      byte[] other = randomPayload(random);
      submittedDigest = SHA_256.digest(other);
    }
    byte[] digestForCall = submittedDigest;
    both(seed, op, "put " + scope.render() + " id " + entityId + " payload " + payload.length
            + (corruptDigest ? " badDigest" : ""),
        stores.oracle, stores.durable, store -> {
          store.put(scope, entityId, payload, digestForCall);
          return null;
        });
    if (sealed || corruptDigest) {
      return; // model unchanged on ALREADY_SEALED and digest-mismatch CORRUPT
    }
    if (expectation != null) {
      if (!Arrays.equals(expectation, payload)) {
        return; // conflicting content: CORRUPT, model unchanged
      }
      return; // idempotent no-op
    }
    model.scope(scope).entities.put(entityId, payload);
    model.dropIfEmpty(scope);
  }

  private static void sealOp(long seed, int op, Random random, Model model,
      List<ScopePath> pool, BothStores stores) {
    ScopePath scope = pickScope(random, pool);
    ScopeModel scopeModel = model.existing(scope);
    boolean sealed = scopeModel != null && scopeModel.sealDigest != null;
    byte[] digest;
    long count;
    String flavor;
    if (sealed && random.nextBoolean()) {
      // Identical re-seal: idempotent no-op success on both.
      digest = scopeModel.sealDigest;
      count = scopeModel.sealCount;
      flavor = "reSealIdentical";
    } else if (sealed) {
      // Conflicting re-seal: CORRUPT on both, model unchanged.
      digest = scopeModel.sealDigest;
      count = scopeModel.sealCount;
      if (random.nextBoolean()) {
        digest = digest.clone();
        digest[random.nextInt(32)] ^= 1;
        flavor = "reSealConflictingDigest";
      } else {
        count = count + 1;
        flavor = "reSealConflictingCount";
      }
    } else {
      // First seal: usually the canonical digest over live entities, sometimes
      // a wrong count or wrong digest recorded verbatim (verify then reports
      // CORRUPT on both).
      boolean canonical = random.nextInt(100) < 70;
      List<StoreLayout.EntityDigest> live = liveDigests(scopeModel);
      digest = StoreLayout.scopeDigest(live);
      count = live.size();
      if (!canonical) {
        if (random.nextBoolean()) {
          count = live.size() + 1 + random.nextInt(3);
          flavor = "sealWrongCount";
        } else {
          digest = digest.clone();
          digest[random.nextInt(32)] ^= 1;
          flavor = "sealWrongDigest";
        }
      } else {
        flavor = "sealCanonical";
      }
    }
    byte[] digestForCall = digest.clone();
    long countForCall = count;
    both(seed, op, "seal " + scope.render() + " " + flavor + " count " + count,
        stores.oracle, stores.durable, store -> {
          store.sealScope(scope, digestForCall, countForCall);
          return null;
        });
    if (sealed && !flavor.equals("reSealIdentical")) {
      return; // conflicting re-seal rejected; model unchanged
    }
    if (!sealed) {
      ScopeModel updated = model.scope(scope);
      updated.sealDigest = digest;
      updated.sealCount = count;
    }
  }

  private static List<StoreLayout.EntityDigest> liveDigests(ScopeModel scopeModel) {
    List<StoreLayout.EntityDigest> live = new ArrayList<>();
    if (scopeModel != null) {
      for (Map.Entry<Long, byte[]> entity : scopeModel.entities.entrySet()) {
        live.add(new StoreLayout.EntityDigest(entity.getKey(), SHA_256.digest(entity.getValue())));
      }
    }
    return live;
  }

  private static void retireOp(long seed, int op, Random random, Model model,
      List<ScopePath> pool, BothStores stores) {
    ScopePath scope = pickScope(random, pool);
    long cursor = pickCursor(random);
    ScopeModel scopeModel = model.existing(scope);
    boolean sealed = scopeModel != null && scopeModel.sealDigest != null;
    both(seed, op, "retire " + scope.render() + " cursor " + cursor
            + (sealed ? " (sealed)" : ""),
        stores.oracle, stores.durable, store -> {
          store.retire(scope, cursor);
          return null;
        });
    if (scopeModel == null) {
      return; // NOT_FOUND on both
    }
    if (sealed) {
      scopesRemoved(model, scope);
      return;
    }
    if (scopeModel.entities.isEmpty()) {
      return; // empty unsealed scope: NOT_FOUND on both
    }
    scopeModel.entities.headMap(cursor, true).clear();
    model.dropIfEmpty(scope);
  }

  /** A sealed scope retires whole: entities and seal record all go away. */
  private static void scopesRemoved(Model model, ScopePath scope) {
    model.scopes.remove(scope);
  }

  private static void verifyOp(long seed, int op, Random random, List<ScopePath> pool,
      BothStores stores) {
    ScopePath scope = pickScope(random, pool);
    both(seed, op, "verify " + scope.render(), stores.oracle, stores.durable,
        store -> store.verifyScope(scope));
  }

  private static void listOp(long seed, int op, Random random, List<ScopePath> pool,
      BothStores stores) {
    ScopePath scope = pickScope(random, pool);
    both(seed, op, "list " + scope.render(), stores.oracle, stores.durable,
        store -> store.list(scope));
  }

  private static void openReadOp(long seed, int op, Random random, List<ScopePath> pool,
      BothStores stores) {
    ScopePath scope = pickScope(random, pool);
    long entityId = pickEntityId(random);
    both(seed, op, "openRead " + scope.render() + " id " + entityId,
        stores.oracle, stores.durable, store -> {
          try (InputStream input = store.openRead(scope, entityId)) {
            return input.readAllBytes();
          }
        });
  }

  /** Guards against model drift: the oracle must agree with the driver's model. */
  private static void sanityCheckModel(long seed, int op, Random random, Model model,
      List<ScopePath> pool, ObjectStore oracle) {
    ScopePath scope = pool.get(random.nextInt(pool.size()));
    ScopeModel scopeModel = model.existing(scope);
    List<Long> expected = scopeModel == null ? List.of() : List.copyOf(scopeModel.entities.keySet());
    List<Long> actual;
    try {
      actual = oracle.list(scope);
    } catch (StorageException failure) {
      throw new AssertionError("oracle list failed [seed " + seed + " op " + op + "]: "
          + failure.kind(), failure);
    }
    assertEquals(expected, actual, "driver model drifted from oracle [seed " + seed + " op " + op
        + " scope " + scope.render() + "] — the test model, not the store, is wrong");
  }

  /** After the last operation every remaining observable must still match. */
  private static void finalSweep(long seed, Model model, BothStores stores) {
    List<ScopePath> oracleScopes = both(seed, OPERATIONS, "final listScopes",
        stores.oracle, stores.durable, ObjectStore::listScopes).value;
    for (ScopePath scope : oracleScopes) {
      List<Long> ids = both(seed, OPERATIONS, "final list " + scope.render(),
          stores.oracle, stores.durable, store -> store.list(scope)).value;
      both(seed, OPERATIONS, "final verify " + scope.render(),
          stores.oracle, stores.durable, store -> store.verifyScope(scope));
      for (long entityId : ids) {
        ScopeModel scopeModel = model.existing(scope);
        byte[] expected = scopeModel == null ? null : scopeModel.entities.get(entityId);
        byte[] actual = both(seed, OPERATIONS, "final openRead " + scope.render() + " id " + entityId,
            stores.oracle, stores.durable, store -> {
              try (InputStream input = store.openRead(scope, entityId)) {
                return input.readAllBytes();
              }
            }).value;
        assertTrue(expected != null && Arrays.equals(expected, actual),
            "final sweep [seed " + seed + "] scope " + scope.render() + " id " + entityId
                + " content must match the driver model");
      }
    }
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }
}

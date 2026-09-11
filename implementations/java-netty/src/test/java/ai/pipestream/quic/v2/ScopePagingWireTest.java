package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Section 12.8 scope paging over the wire (S12-288 to S12-290): pages return IDs strictly greater
 * than {@code after-entity} in increasing order, at most the requested number; {@code more}
 * reports IDs beyond the page in that snapshot; an unsealed scope may grow between requests; an
 * empty page is not completeness evidence, only the seal is.
 */
@Timeout(120)
class ScopePagingWireTest {
  @TempDir static Path directory;
  static DurableTestPki pki;
  static Map<Records.Digest, String> principals;
  static final Records.Policy POLICY = new Records.Policy(20_000, 60_000, 120_000);

  @BeforeAll
  static void certificates() throws Exception {
    pki = DurableTestPki.generate(directory, List.of("alice"));
    principals = pki.principals(List.of("alice"));
  }

  static final class Session implements AutoCloseable {
    final DurableHost host;
    final DurableServer server;
    final RawDurablePeer peer;

    Session(String name) throws Exception {
      // Cumulative record funding under the default 256 MiB file policy refuses a scope
      // near 256 members with LIMIT_EXCEEDED "SQLite file capacity exhausted" (README,
      // Launchers); the 300-member walk below needs the operator funding V2Main exposes.
      host =
          DurableHost.initialize(
              directory.resolve(name),
              V2Main.configuration(
                  Map.of(
                      "authority", "issuer-a",
                      "result-authority", "localhost:7443",
                      "db-mib", "1024",
                      "wal-mib", "256")),
              ReferenceApplications.all(),
              DurableHost.OwnerPolicy.fromPrincipals(() -> principals, true),
              DurableHost.UtcClock.system(true));
      server =
          DurableServer.start(
              new InetSocketAddress("127.0.0.1", 0),
              pki.server(principals),
              host,
              DurableOptions.defaults());
      peer = new RawDurablePeer(server.address(), pki.client("alice"), 65_536);
      peer.negotiate(RawDurablePeer.offer(List.of(DURABLE_WORK, RESULT_DELIVERY), 1 << 20));
      assertInstanceOf(Binding.class, peer.call(new Create(peer.request(), 1, POLICY)));
    }

    Records.Declared declare(int operation, long from, long to, boolean seal) throws Exception {
      List<Long> ids = LongStream.rangeClosed(from, to).boxed().toList();
      Message answer =
          peer.call(
              new Declare(peer.request(), DurableServerTest.operation(operation), 0, ids, seal));
      DeclarationResponse response =
          assertInstanceOf(DeclarationResponse.class, answer, "declare " + from + ".." + to + " answered " + answer);
      return assertInstanceOf(Records.Declared.class, response.receipt().outcome());
    }

    PageResponse page(long after, int limit) throws Exception {
      PageResponse page =
          assertInstanceOf(PageResponse.class, peer.call(new Page(peer.request(), 0, after, limit)));
      assertEquals(0, page.scope());
      assertEquals(0, page.producer());
      assertNull(page.parent());
      long previous = after;
      for (Entry entry : page.entries()) {
        assertTrue(entry.entity() > previous, "not strictly increasing after " + after);
        previous = entry.entity();
      }
      assertTrue(page.entries().size() <= limit, "page exceeds the requested limit");
      return page;
    }

    @Override
    public void close() throws java.io.IOException {
      peer.close();
      server.close();
      host.close();
    }
  }

  static List<Long> ids(PageResponse page) {
    List<Long> ids = new ArrayList<>();
    for (Entry entry : page.entries()) ids.add(entry.entity());
    return ids;
  }

  @Test
  void aSealedScopeBeyondOnePageIsWalkedWithAfterEntityAndMore() throws Exception {
    try (Session session = new Session("sealed-300")) {
      // 300 members need two declarations (256 per list); only the last one seals.
      session.declare(1, 1, 256, false);
      Records.Declared sealed = session.declare(2, 257, 300, true);
      assertNotNull(sealed.seal());
      assertEquals(300, sealed.declared());

      PageResponse first = session.page(0, 256);
      assertEquals(LongStream.rangeClosed(1, 256).boxed().toList(), ids(first));
      assertTrue(first.more(), "256 of 300 returned but more() is false");
      assertTrue(first.sealed());
      assertEquals(300, first.declared());

      PageResponse second = session.page(256, 256);
      assertEquals(LongStream.rangeClosed(257, 300).boxed().toList(), ids(second));
      assertFalse(second.more(), "last page still reports more()");
      assertTrue(second.sealed());
      assertEquals(sealed.seal(), second.seal(), "final page carries the declared seal");

      // A smaller limit and a mid-range cursor obey the same rules.
      PageResponse middle = session.page(100, 10);
      assertEquals(LongStream.rangeClosed(101, 110).boxed().toList(), ids(middle));
      assertTrue(middle.more());
      // Past the end: empty, nothing more, still sealed.
      PageResponse past = session.page(300, 256);
      assertTrue(past.entries().isEmpty());
      assertFalse(past.more());
      assertTrue(past.sealed());
    }
  }

  @Test
  void anUnsealedScopeGrowsBetweenPagesAndAnEmptyPageProvesNothing() throws Exception {
    try (Session session = new Session("growing")) {
      session.declare(1, 1, 10, false);
      PageResponse ten = session.page(0, 256);
      assertEquals(LongStream.rangeClosed(1, 10).boxed().toList(), ids(ten));
      assertFalse(ten.more());
      assertFalse(ten.sealed());
      assertNull(ten.seal());

      // An empty page on the unsealed scope: not completeness evidence.
      PageResponse empty = session.page(10, 256);
      assertTrue(empty.entries().isEmpty());
      assertFalse(empty.more());
      assertFalse(empty.sealed());
      assertNull(empty.seal());

      // The scope grows after that empty page, and the continuation sees the growth.
      session.declare(2, 11, 15, false);
      PageResponse grown = session.page(10, 256);
      assertEquals(LongStream.rangeClosed(11, 15).boxed().toList(), ids(grown));
      assertFalse(grown.sealed());

      // Only the seal establishes final membership.
      Records.Declared sealed = session.declare(3, 16, 16, true);
      PageResponse last = session.page(15, 256);
      assertEquals(List.of(16L), ids(last));
      assertTrue(last.sealed());
      assertEquals(sealed.seal(), last.seal());
      assertEquals(16, last.declared());
      PageResponse whole = session.page(0, 256);
      assertEquals(16, whole.entries().size());
      assertEquals(sealed.seal(), whole.seal());
    }
  }
}

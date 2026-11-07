package socs.network.node;

import org.junit.After;
import org.junit.Test;
import socs.network.message.LSA;
import socs.network.message.LinkDescription;
import socs.network.message.SOSPFPacket;
import socs.network.util.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.*;

/** Real-socket regression tests for rollback and the request-ID / accepted-session race. */
public class RouterLifecycleTest {
  private final List<Router> routers = new ArrayList<>();
  private final List<Path> configs = new ArrayList<>();

  private static Field field(String name) throws Exception {
    Field f = Router.class.getDeclaredField(name);
    f.setAccessible(true);
    return f;
  }

  private static Object call(Router r, String method, Class<?>[] types, Object... args) throws Exception {
    Method m = Router.class.getDeclaredMethod(method, types);
    m.setAccessible(true);
    return m.invoke(r, args);
  }

  private static void command(Router r, String text) throws Exception {
    call(r, "runCommand", new Class<?>[]{String.class}, text);
  }

  private Router router(int number) throws Exception {
    Path config = Files.createTempFile("lsr-lifecycle-", ".conf");
    configs.add(config);
    Files.write(config, ("socs.network.router.ip=\"10.92.0." + number + "\"\n")
        .getBytes(StandardCharsets.UTF_8));
    Router r = new Router(new Configuration(config.toString()));
    // Test transport stays local; production process-address discovery is unchanged.
    r.rd.processIPAddress = "127.0.0.1";
    routers.add(r);
    return r;
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(12);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail("Timed out waiting for router state");
      Thread.sleep(15);
    }
  }

  private static Link link(Router r, String ip) {
    try {
      return (Link) call(r, "findLink", new Class<?>[]{String.class}, ip);
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static int seq(Router r) {
    for (LSA lsa : r.lsd.deepCopy()) {
      if (r.rd.simulatedIPAddress.equals(lsa.linkStateID)) return lsa.lsaSeqNumber;
    }
    throw new AssertionError("Missing self LSA");
  }

  private static boolean noticesDone(Router r) {
    try {
      synchronized (field("portLock").get(r)) {
        return ((Map<?, ?>) field("pendingDisconnects").get(r)).isEmpty();
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static RouterDescription description(Router r) {
    RouterDescription d = new RouterDescription();
    d.simulatedIPAddress = r.rd.simulatedIPAddress;
    d.processIPAddress = r.rd.processIPAddress;
    d.processPortNumber = r.rd.processPortNumber;
    return d;
  }

  /** Models an earlier attach whose acceptance was lost before the other side installed it. */
  private static Link installOneSide(Router owner, Router peer, String id) throws Exception {
    Link l = new Link(owner.rd, description(peer), 3, id);
    l.router2.status = RouterStatus.TWO_WAY;
    synchronized (field("portLock").get(owner)) {
      int port = (Integer) call(owner, "reservePort", new Class<?>[0]);
      assertTrue((Boolean) call(owner, "commitPort", new Class<?>[]{int.class, Link.class}, port, l));
      owner.lsd.addLinkToLSA(peer.rd.simulatedIPAddress, port, 3);
    }
    return l;
  }

  private static SOSPFPacket packet(Router from, Router to, String id, short type, String message) {
    SOSPFPacket p = new SOSPFPacket();
    p.srcIP = from.rd.simulatedIPAddress;
    p.dstIP = to.rd.simulatedIPAddress;
    p.srcProcessIP = from.rd.processIPAddress;
    p.srcProcessPort = from.rd.processPortNumber;
    p.linkId = id;
    p.sospfType = type;
    p.message = message;
    p.weight = 3;
    return p;
  }

  private static SOSPFPacket exchange(Router to, SOSPFPacket p) throws Exception {
    try (Socket s = new Socket(to.rd.processIPAddress, to.rd.processPortNumber)) {
      s.setSoTimeout(4000);
      ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
      out.flush();
      ObjectInputStream in = new ObjectInputStream(s.getInputStream());
      out.writeObject(p);
      out.flush();
      return (SOSPFPacket) in.readObject();
    }
  }

  @After public void close() throws Exception {
    for (Router r : routers) {
      ((ServerSocket) field("serverSocket").get(r)).close();
      ((ExecutorService) field("weightUpdates").get(r)).shutdownNow();
      ((ExecutorService) field("topologyTasks").get(r)).shutdownNow();
    }
    for (Path config : configs) Files.deleteIfExists(config);
  }

  @Test(timeout = 25000) public void failedConnectFreesPortAndRetriesPeerCleanup() throws Exception {
    Router r = router(1);
    command(r, "start");
    int before = seq(r);
    try (HandshakePeer peer = new HandshakePeer()) {
      peer.dropCleanupAckOnce = true;
      command(r, peer.connectCommand());
      assertTrue(peer.thirdHello.await(8, TimeUnit.SECONDS));
      String failedId = peer.activeId;
      peer.releaseHello.countDown();
      await(() -> peer.cleanupRequests >= 2 && link(r, peer.ip) == null && noticesDone(r));
      assertEquals(1, peer.cleanups);
      assertNull(peer.activeId);
      assertEquals(before, seq(r));
      peer.failConfirmation = false;
      command(r, peer.connectCommand());
      await(() -> link(r, peer.ip) != null && link(r, peer.ip).router2.status == RouterStatus.TWO_WAY);
      assertNotEquals(failedId, link(r, peer.ip).linkId);
      assertEquals(before + 1, seq(r));
    }
  }

  @Test(timeout = 20000) public void ordinaryAttachStillSurvivesStartHandshakeFailure() throws Exception {
    Router r = router(2);
    try (HandshakePeer peer = new HandshakePeer()) {
      command(r, peer.connectCommand().replaceFirst("connect", "attach"));
      await(() -> link(r, peer.ip) != null);
      peer.releaseHello.countDown();
      command(r, "start");
      assertNotNull(link(r, peer.ip));
      assertNotEquals(RouterStatus.TWO_WAY, link(r, peer.ip).router2.status);
      assertEquals(0, peer.cleanupRequests);
    }
  }

  @Test(timeout = 20000) public void lostAttachAcceptanceAlsoCancelsPeerSession() throws Exception {
    Router r = router(10);
    command(r, "start");
    int before = seq(r);
    try (HandshakePeer peer = new HandshakePeer()) {
      peer.dropAttachReplyOnce = true;
      command(r, peer.connectCommand());
      await(() -> peer.cleanupRequests > 0 && noticesDone(r));
      assertNull(link(r, peer.ip));
      assertNull(peer.activeId);
      assertEquals(1, peer.cleanups);
      assertEquals(before, seq(r));
    }
  }

  @Test(timeout = 15000) public void rejectedConnectLeavesExistingPeerSessionAlone() throws Exception {
    Router a = router(11), b = router(12);
    Link existing = installOneSide(b, a, UUID.randomUUID().toString());
    int before = seq(b);
    command(a, "start");
    // The peer rejects this different cost without asking for approval.
    call(a, "processConnect", new Class<?>[]{String.class, short.class, String.class, short.class},
        b.rd.processIPAddress, b.rd.processPortNumber, b.rd.simulatedIPAddress, (short) 7);
    assertNull(link(a, b.rd.simulatedIPAddress));
    assertSame(existing, link(b, a.rd.simulatedIPAddress));
    assertEquals(before, seq(b));
    assertTrue(noticesDone(a));
  }

  @Test(timeout = 20000) public void concurrentSuccessfulHandshakeIsNotRolledBack() throws Exception {
    Router r = router(3);
    command(r, "start");
    try (HandshakePeer peer = new HandshakePeer()) {
      CompletableFuture<Void> connecting = CompletableFuture.runAsync(() -> {
        try {
          call(r, "processConnect", new Class<?>[]{String.class, short.class, String.class, short.class},
              "127.0.0.1", (short) peer.listener.getLocalPort(), peer.ip, (short) 3);
        } catch (Exception e) { throw new RuntimeException(e); }
      });
      assertTrue(peer.thirdHello.await(8, TimeUnit.SECONDS));
      // The opposite direction completes while the original direction is missing its ACK.
      peer.completeReverseHandshake(r);
      Link established = link(r, peer.ip);
      assertEquals(RouterStatus.TWO_WAY, established.router2.status);
      peer.releaseHello.countDown();
      connecting.get(8, TimeUnit.SECONDS);
      assertSame(established, link(r, peer.ip));
      assertEquals(0, peer.cleanupRequests);
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 20000) public void quitCancelsAdoptedSessionUsingOriginalRequestId() throws Exception {
    Router a = router(4), b = router(5);
    String actualId = UUID.randomUUID().toString(), requestId = UUID.randomUUID().toString();
    installOneSide(b, a, actualId);
    int before = seq(b);
    SOSPFPacket accepted = exchange(b, packet(a, b, requestId, Router.TYPE_ATTACH, null));
    assertEquals(actualId, accepted.linkId);
    // The ACK has left B, but A is still in the pre-installation stage when quit snapshots it.
    synchronized (field("portLock").get(a)) {
      ((Map<String, Link>) field("pendingAttachLinks").get(a))
          .put(requestId, new Link(a.rd, description(b), 3, requestId));
    }
    assertTrue((Boolean) call(a, "shutdownRouter", new Class<?>[0]));
    assertNull(link(b, a.rd.simulatedIPAddress));
    assertEquals(before + 1, seq(b));
    assertTrue(((Map<?, ?>) field("acceptedAttachRequests").get(b)).isEmpty());
    SOSPFPacket duplicate = exchange(b, packet(a, b, requestId, Router.TYPE_ROUTER_DOWN, "ROUTER_DOWN"));
    assertEquals("ROUTER_DOWN_ACK", duplicate.message);
    assertEquals(before + 1, seq(b));
  }

  @Test(timeout = 15000) public void retiredAliasCannotRemoveReplacementOrBeReaccepted() throws Exception {
    Router a = router(6), b = router(7);
    String oldId = UUID.randomUUID().toString(), requestId = UUID.randomUUID().toString();
    installOneSide(b, a, oldId);
    assertEquals(oldId, exchange(b, packet(a, b, requestId, Router.TYPE_ATTACH, null)).linkId);
    assertEquals("LINK_DOWN_ACK", exchange(b, packet(a, b, requestId, Router.TYPE_LINK_DOWN, "LINK_DOWN")).message);
    Link replacement = installOneSide(b, a, UUID.randomUUID().toString());
    int before = seq(b);
    assertEquals("ROUTER_DOWN_ACK", exchange(b, packet(a, b, requestId, Router.TYPE_ROUTER_DOWN, "ROUTER_DOWN")).message);
    assertEquals("REJECTED", exchange(b, packet(a, b, requestId, Router.TYPE_ATTACH, null)).message);
    assertSame(replacement, link(b, a.rd.simulatedIPAddress));
    assertEquals(before, seq(b));
  }

  @Test(timeout = 15000) public void adoptedSessionStillRequiresMatchingPeerEndpoint() throws Exception {
    Router a = router(8), b = router(9);
    String requestId = UUID.randomUUID().toString();
    Link actual = installOneSide(b, a, UUID.randomUUID().toString());
    exchange(b, packet(a, b, requestId, Router.TYPE_ATTACH, null));
    int before = seq(b);
    SOSPFPacket wrongEndpoint = packet(a, b, requestId, Router.TYPE_ROUTER_DOWN, "ROUTER_DOWN");
    wrongEndpoint.srcProcessPort = 1;
    assertEquals("ROUTER_DOWN_REJECTED", exchange(b, wrongEndpoint).message);
    assertSame(actual, link(b, a.rd.simulatedIPAddress));
    assertEquals(before, seq(b));
  }

  /** A peer that can lose HELLO confirmation and the first cleanup ACK, using real sockets. */
  private static final class HandshakePeer implements AutoCloseable {
    final String ip = "10.92.9.1";
    final ServerSocket listener;
    final CountDownLatch thirdHello = new CountDownLatch(1), releaseHello = new CountDownLatch(1);
    volatile boolean failConfirmation = true, dropCleanupAckOnce, dropAttachReplyOnce;
    volatile String activeId;
    volatile int cleanupRequests, cleanups;

    HandshakePeer() throws Exception {
      ServerSocket candidate = null;
      for (int p = 22000; p < 32000; p++) {
        try { candidate = new ServerSocket(p); break; } catch (IOException ignored) { }
      }
      if (candidate == null) throw new IOException("No peer port available");
      listener = candidate;
      Thread accept = new Thread(() -> {
        while (!listener.isClosed()) {
          try {
            Socket s = listener.accept();
            Thread handler = new Thread(() -> handle(s));
            handler.setDaemon(true);
            handler.start();
          } catch (IOException e) { return; }
        }
      });
      accept.setDaemon(true);
      accept.start();
    }

    String connectCommand() {
      return "connect 127.0.0.1 " + listener.getLocalPort() + " " + ip + " 3";
    }

    SOSPFPacket reply(SOSPFPacket request) {
      SOSPFPacket p = new SOSPFPacket();
      p.srcIP = ip;
      p.dstIP = request.srcIP;
      p.srcProcessIP = "127.0.0.1";
      p.srcProcessPort = (short) listener.getLocalPort();
      p.sospfType = request.sospfType;
      p.linkId = request.linkId;
      return p;
    }

    void handle(Socket socket) {
      try (Socket s = socket; ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream())) {
        out.flush();
        ObjectInputStream in = new ObjectInputStream(s.getInputStream());
        SOSPFPacket request = (SOSPFPacket) in.readObject(), response = reply(request);
        switch (request.sospfType) {
          case Router.TYPE_ATTACH:
            activeId = request.linkId;
            if (dropAttachReplyOnce) { dropAttachReplyOnce = false; return; }
            response.message = Router.ATTACH_ACCEPTED;
            response.weight = request.weight;
            break;
          case Router.TYPE_HELLO:
            out.writeObject(response);
            out.flush();
            in.readObject();
            thirdHello.countDown();
            releaseHello.await(10, TimeUnit.SECONDS);
            if (failConfirmation) return;
            response = reply(request); // a new object, not a cached HELLO back-reference
            response.sospfType = Router.TYPE_LSU;
            response.lsaArray = new Vector<>();
            LSA lsa = new LSA();
            lsa.linkStateID = ip;
            lsa.lsaSeqNumber = Integer.MIN_VALUE + 1;
            LinkDescription edge = new LinkDescription();
            edge.linkID = request.srcIP;
            edge.weight = 3;
            lsa.links.add(edge);
            response.lsaArray.add(lsa);
            break;
          case Router.TYPE_LINK_DOWN:
            synchronized (this) {
              cleanupRequests++;
              if (request.linkId.equals(activeId)) { activeId = null; cleanups++; }
              if (dropCleanupAckOnce) { dropCleanupAckOnce = false; return; }
            }
            response.message = "LINK_DOWN_ACK";
            break;
          default:
            return; // ordinary flooded LSAUPDATE has no reply
        }
        out.writeObject(response);
        out.flush();
      } catch (Exception ignored) {
        // Closing test sockets deliberately interrupts incomplete exchanges.
      }
    }

    void completeReverseHandshake(Router target) throws Exception {
      SOSPFPacket hello = new SOSPFPacket();
      hello.sospfType = Router.TYPE_HELLO;
      hello.srcIP = ip;
      hello.dstIP = target.rd.simulatedIPAddress;
      hello.srcProcessIP = "127.0.0.1";
      hello.srcProcessPort = (short) listener.getLocalPort();
      hello.linkId = activeId;
      try (Socket s = new Socket(target.rd.processIPAddress, target.rd.processPortNumber)) {
        s.setSoTimeout(4000);
        ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
        out.flush();
        ObjectInputStream in = new ObjectInputStream(s.getInputStream());
        out.writeObject(hello);
        out.flush();
        assertEquals(Router.TYPE_HELLO, ((SOSPFPacket) in.readObject()).sospfType);
        out.writeObject(hello);
        out.flush();
        assertEquals(Router.TYPE_LSU, ((SOSPFPacket) in.readObject()).sospfType);
      }
    }

    @Override public void close() throws Exception {
      releaseHello.countDown();
      listener.close();
    }
  }
}

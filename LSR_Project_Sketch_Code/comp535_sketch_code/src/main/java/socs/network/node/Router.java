package socs.network.node;

import socs.network.message.SOSPFPacket;
import socs.network.util.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Vector;
import socs.network.message.LSA;
 



public class Router {

  // MessageStats type, counts the number of various message types.
  private MessageCount stats = new MessageCount();


  // ---------------------------------------------------------------------------------------
  // Packet types carried in SOSPFPacket.sospfType. 0/1/2 are defined by the starter code;
  // 3 is added for the attach request/reply so it is never mistaken for a HELLO.
  // ---------------------------------------------------------------------------------------
  // 
  static final short TYPE_HELLO = 0;     // 3-way handshake
  static final short TYPE_LSU = 1;          // linkState Update 
  static final short TYPE_APP_MESSAGE = 2;  // application message 
  static final short TYPE_ATTACH = 3;       // attach request and its reply
  static final short TYPE_WEIGHT_UPDATE = 4; // direct-neighbor weight request/reply
  static final short TYPE_LINK_DOWN = 5; // explicit adjacency-loss notification/reply
  private static final String LINK_DOWN = "LINK_DOWN";
  private static final String LINK_DOWN_ACK = "LINK_DOWN_ACK";
  private static final String FINAL_LSAUPDATE = "FINAL_LSAUPDATE";
  private static final String FINAL_LSAUPDATE_ACK = "FINAL_LSAUPDATE_ACK";
  private static final int DISCONNECT_ATTEMPTS = 3;
  private static final String WEIGHT_REQUEST = "WEIGHT_REQUEST";
  private static final String WEIGHT_ACCEPTED = "WEIGHT_ACCEPTED";
  private static final String WEIGHT_REJECTED = "WEIGHT_REJECTED";
  private static final int WEIGHT_UPDATE_ATTEMPTS = 3;

  // Preserve terminal command order while keeping Y/N and other commands responsive.
  private final ExecutorService weightUpdates = Executors.newSingleThreadExecutor(task -> {
    Thread worker = new Thread(task, "weight-updates");
    worker.setDaemon(true);
    return worker;
  });

  private final ExecutorService topologyTasks = Executors.newCachedThreadPool(task -> {
    Thread worker = new Thread(task, "topology-notification");
    worker.setDaemon(true);
    return worker;
  });

  // SOSPFPacket.message inside attach reply 
  static final String ATTACH_ACCEPTED = "ACCEPTED";
  static final String ATTACH_REJECTED = "REJECTED";

  // Upper bound on how long one HELLO handshake waits for the other routers (ms).
  private static final int HELLO_TIMEOUT_MS = 5000;

  protected LinkStateDatabase lsd;

  RouterDescription rd = new RouterDescription();

  // assuming that all routers are with 4 ports
  Link[] ports = new Link[4];

  // Other routers connect to this socket. It stays open until the router quits. 
  private ServerSocket serverSocket;

  // PA3: only an explicitly completed local start enables outgoing connect.
  // Receiving HELLO from another router does not set this flag.
  private volatile boolean started = false;
  private volatile boolean shuttingDown = false;

  // Guards ports[], reservedPorts[] 
  private final Object portLock = new Object();

  // Guarded by portLock. Retired IDs prevent delayed messages from resurrecting old sessions.
  private final Set<String> retiredLinkIds = new HashSet<>();
  private final Map<String, DisconnectNotice> pendingDisconnects = new HashMap<>();
  // A quit must also retire requests waiting for a remote user's Y/N decision.
  private final Map<String, Link> pendingAttachLinks = new HashMap<>();

  private static class DisconnectNotice {
    final Link link;
    final int port;
    CompletableFuture<Boolean> attempt;
    DisconnectNotice(Link link, int port) { this.link = link; this.port = port; }
  }


   // reservedPorts[i] == true means port i is still empty but promised to an attach that is
   // waiting for a Y/N answer. Guarantees that when the answer finally is
   // "yes" there is still a free slot for the link. (We cannot simply hold portLock while
   // waiting - two routers attaching each other would then deadlock, because 
   // both side need to check port list: findLink() and that required lock)
  private final boolean[] reservedPorts = new boolean[4];

  
  // It's a lock for connection handle thread, that is currently asking Y/N question. From the
  // moment the question printed util the reply sent to the other router. Other thread wait for the 
  // lock, so we prevent the conflict.
  private final Object attachApprovalLock = new Object();

  
  // A letter box
  private volatile CompletableFuture<Boolean> pendingAttachDecision;
  private String pendingAttachId; // Guarded by portLock; cancels an unanswered retired request.

  // It's a HashSet that keep tracking the current attach (those haven't receive response)
  // which make sure won't have duplicate attach after the attach request been sent
  private final Set<String> outgoingAttachRouters =Collections.synchronizedSet(new HashSet<String>());

  // =======================================================================================
  // Construction / startup
  // =======================================================================================

  public Router(Configuration config) {
    rd.simulatedIPAddress = config.getString("socs.network.router.ip");
    lsd = new LinkStateDatabase(rd);

    try {
      rd.processIPAddress = InetAddress.getLocalHost().getHostAddress(); // obtain the real host IP address
      serverSocket = bindFreePort();  //can't use ServerSocket(0) here
      rd.processPortNumber = (short) serverSocket.getLocalPort();
    } catch (IOException e) {
      throw new RuntimeException("cannot start router: " + e.getMessage(), e);
    }

    System.out.println("========================================");
    System.out.println("Process IP    : " + rd.processIPAddress);
    System.out.println("Process Port  : " + rd.processPortNumber);
    System.out.println("Simulated IP  : " + rd.simulatedIPAddress);
    System.out.println("========================================");

    // Now we create an new thread for the requestHandler()
    Thread reqHandleThread = new Thread(() -> requestHandler());
    // start an new thread
    reqHandleThread.setDaemon(true);
    reqHandleThread.start();
  }


  // Method that help to find a free port on your computer and create a ServerSocket
  private static ServerSocket bindFreePort() throws IOException {
    Random random = new Random();
    for (int attempt = 0; attempt < 200; attempt++) {
      int port = 10000 + random.nextInt(Short.MAX_VALUE - 10000); // 10000 .. 32766
      try {
        return new ServerSocket(port);
      } catch (IOException portInUse) {
        // try another one
      }
    }
    throw new IOException("no free port found in range 10000-32766");
  }


  // =======================================================================================
  // Port list helpers (all access to ports[] / reservedPorts[] need these methods)
  // =======================================================================================

  /** Returns the link whose remote end has the given simulated IP, or null. */
  private Link findLink(String simulatedIP) {
    synchronized (portLock) {
      for (Link link : ports) {
        if (link != null && link.router2.simulatedIPAddress.equals(simulatedIP)) {
          return link;
        }
      }
      return null;
    }
  }

  /** Reserves the first port that is neither occupied nor reserved. Returns -1 if none. */
  private int reservePort() {
    synchronized (portLock) {
      if (shuttingDown) return -1;
      for (int i = 0; i < ports.length; i++) {
        if (ports[i] == null && !reservedPorts[i]) {
          reservedPorts[i] = true;
          return i;
        }
      }
      return -1;
    }
  }

  /** Gives a reservation back without storing a link (attach rejected or failed). */
  private void releasePort(int port) {
    synchronized (portLock) {
      reservedPorts[port] = false;
    }
  }

  /**
   * Stores link in the reserved port and clears the reservation.
   * Returns false and stores nothing if a link to the same router already exists.
   * That happens when two routers attach each other at the same time and the other
   * direction was accepted first; keeping only the first link is what makes a simultaneous
   * attach safe. The caller must then release port from the reservation.
   */
  private boolean commitPort(int port, Link link) {
    synchronized (portLock) {
      // EDGE CASE: an acceptance may arrive after quit cleared all reservations.
      if (shuttingDown) return false;
      if (!reservedPorts[port] || ports[port] != null) {
      throw new IllegalStateException("Port not reserved or it's unexpectedly occupied.");
    }
      if (retiredLinkIds.contains(link.linkId)
          || findLink(link.router2.simulatedIPAddress) != null) {
        return false;
      }
      ports[port] = link;
      reservedPorts[port] = false;
      return true;
    }
  }

  /** Copy of all links, so callers can do network I/O without holding portLock.
   *  It make sure you copy the latest list, but after that the list could be updated
   */
  private List<Link> snapshotLinks() {
    synchronized (portLock) {
      List<Link> result = new ArrayList<>();
      for (Link link : ports) {
        if (link != null) {
          result.add(link); // we get it's ref
        }
      }
      return result;
    }
  }

  /**
   * Removes exactly this link object from the port table and frees its port.
   * Compares by reference (ports[i] == link)
   */
  private boolean removeLink(Link link) {
    synchronized (portLock) {
      for (int i = 0; i < ports.length; i++) {
        if (ports[i] == link) {
          ports[i] = null;
          retiredLinkIds.add(link.linkId);
          link.pendingWeightUpdate = null;
          link.router2.status = null;
          lsd.removeLinkFromLSA(link.router2.simulatedIPAddress);
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Moves the neighbour's state forward (null -> INIT -> TWO_WAY) and prints the transition msg.
   * Never moves backwards!!! so running start again on an already TWO_WAY neighbour
   * does not print any msg.
   * 
   * PA2 modification: advanceNeighborStatus return BOOLEAN !
   */
  private boolean advanceNeighborStatus(Link link, RouterStatus target) {
    boolean changed;
    synchronized (portLock) {
      if (shuttingDown || getPort(link) < 0) return false;
      RouterStatus current = link.router2.status;
      changed = current == null || current.ordinal() < target.ordinal();
      if (changed) { // check if the changed of status is legal
        link.router2.status = target;
      }
    }
    if (changed) {
      System.out.println("set " + link.router2.simulatedIPAddress + " STATE to " + target + ";");
    }
    return changed;
  }

  // =======================================================================================
  // Packet / stream helpers
  // =======================================================================================

  /** A packet of the given type with all fields filled in. */
  private SOSPFPacket newPacket(short typeMessage, String dstSimulatedIP) {
    SOSPFPacket packet = new SOSPFPacket();
    packet.sospfType = typeMessage;
    packet.srcProcessIP = rd.processIPAddress;
    packet.srcProcessPort = rd.processPortNumber;
    packet.srcIP = rd.simulatedIPAddress;
    packet.dstIP = dstSimulatedIP;
    packet.routerID = rd.simulatedIPAddress; // ? not sure
    packet.neighborID = rd.simulatedIPAddress; // sender's simulated IP 
    return packet;
  }

  private SOSPFPacket linkPacket(short type, Link link) {
    SOSPFPacket packet = newPacket(type, link.router2.simulatedIPAddress);
    packet.linkId = link.linkId;
    return packet;
  }

  /**
   * Opens the object output stream and pushes its header to the peer right away.
   * IMPORTANT : An ObjectInputStream constructor blocks until it has
   * read the header written by the peer's ObjectOutputStream; if both sides opened their
   * input stream first they would wait for each other forever.
   */
  private static ObjectOutputStream openOutput(Socket socket) throws IOException {
    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
    out.flush();
    return out;
  }

  private static Socket openSocket(String processIP, short processPort) throws IOException {
    Socket socket = new Socket();
    try {
      // EDGE CASE: bound connection setup and reads, including the stream header.
      // Setting SO_TIMEOUT after constructing ObjectInputStream is too late.
      socket.connect(new InetSocketAddress(processIP, processPort), HELLO_TIMEOUT_MS);
      socket.setSoTimeout(HELLO_TIMEOUT_MS);
      return socket;
    } catch (IOException | IllegalArgumentException e) {
      socket.close();
      throw e;
    }
  }

  /** Reads one SOSPFPacket; anything else on the wire is treated as an I/O error. */
  private static SOSPFPacket readPacket(ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    Object received = in.readObject();
    if (!(received instanceof SOSPFPacket)) {
      throw new IOException("unexpected object on the wire: "
          + (received == null ? "null" : received.getClass().getName()));
    }
    return (SOSPFPacket) received;
  }

  //---------------------------PA2 new helper method--------------------

  private boolean isTwoWay(Link link) {
    synchronized (portLock) {
      // EDGE CASE: router1 is shared local rd; HELLO only advances router2.status
      // for this neighbor. Checking router1 would reject every valid link.
      return link != null && getPort(link) >= 0
          && link.router2.status == RouterStatus.TWO_WAY;
    }
  }

  private int getPort(Link link) {
    synchronized (portLock) {
      for (int i=0; i<ports.length; i++) {
        if (ports[i] == link) {
          return i;
        }
      }
      return -1;
    }
  }

  private boolean addNeighborToLSA(Link link) {
    synchronized (portLock) {
      int port = getPort(link);
      // EDGE CASE: port 0 is valid; -1 means the link is no longer installed.
      // Keep the checks and insertion together during concurrent start commands.
      if (!shuttingDown && isTwoWay(link) && port >= 0) {
        return lsd.addLinkToLSA(link.router2.simulatedIPAddress, port, link.weight);
      }
      return false;
    }
  }


  /**
   * Send a packet to neighbor (we have it's routerDescription inside the link)
   * Don't need to wait for peer's reply
   */
  private boolean sendOneWayMessage(RouterDescription neighbor, SOSPFPacket packet) {
    try (Socket socket = openSocket(neighbor.processIPAddress, neighbor.processPortNumber);
         ObjectOutputStream out = openOutput(socket);
         ObjectInputStream peerInput = new ObjectInputStream(socket.getInputStream())) {
      // Reading the peer's stream header ensures it opened its output before we
      // close. No packet reply is expected; try-with-resources closes both streams.
      out.writeObject(packet);
      out.flush();
      stats.countSent(packet.sospfType);
      return true;
    } catch (IOException e) {
      System.out.println("Cannot send to " + neighbor.simulatedIPAddress + ": " + e.getMessage());
      return false;
    }
  }

  /**
   * Send the LSA update to all neighbors with status TWO_WAY except the 
   * exception IP (this happen when you retransmit neighbor's message
   * you don't need to send it back to it again)
   */
  private void broadcastLSAUpdate(Vector<LSA> LSAs, String exception) {
    for (Link link : snapshotLinks()) {
      RouterDescription neighbor = link.router2;
      // EDGE CASE: locally originated broadcasts have exception == null.
      if (!isTwoWay(link) || neighbor.simulatedIPAddress.equals(exception)) {
        continue;
      }
      SOSPFPacket packet = newPacket(TYPE_LSU, neighbor.simulatedIPAddress);
      packet.lsaArray = LSAs;
      sendOneWayMessage(neighbor, packet);
    }
  }

  /**
   * When own router received the LSA update
   * Check if there is any real update for LSA (new seq number or LSA
   * that haven't exist inside the _store)
   */
  private void receiveLSAUpdate(SOSPFPacket packet) {
    List<LSA> updates = this.lsd.merge(packet.lsaArray);
    if (!updates.isEmpty()) {
      broadcastLSAUpdate(new Vector<>(updates), packet.srcIP);
    }
  }


  /**
   *  Find the next Link for reaching the destination IP, according to the shotest path
   * return null if no path or all no TWO_WAY neighbor.
   * IF the destination IP is own IP?
   */

  private Link nextHopLink(String destinationIP) {
    Path path = lsd.findShortestPathTo(destinationIP);
    // EDGE CASE: an unreachable destination has no Path; a path to this router
    // exists but has no next hop. Neither should dereference null.
    if (path == null || path.nextHop() == null) {
      return null;
    }
    Link link = findLink(path.nextHop());
    if (link == null || !isTwoWay(link)) {
      return null;
    }
    return link;
  }

  // -------------------------------------------------------------------------------------



  // =======================================================================================
  // Incoming connections
  // =======================================================================================

  /**
   * Listener loop: accept a connection and hand it to a fresh thread, so that a request
   * that takes a long time (e.g. waiting for the user's Y/N) never blocks other routers.
   */
  private void requestHandler() {
    while (!serverSocket.isClosed()) {
      try {
        Socket socket = serverSocket.accept();
        Thread handler = new Thread(() -> connectionHandler(socket));
        handler.setDaemon(true);
        handler.start();
      } catch (IOException e) {
        if (serverSocket.isClosed()) {
          return; // quit was called
        }
        System.out.println("Accept failed: " + e.getMessage());
      }
    }
  }

  /** Reads the first packet of a connection and dispatches on its type. */
  private void connectionHandler(Socket socket) {
    try (Socket s = socket) {
      s.setSoTimeout(HELLO_TIMEOUT_MS);
      try (ObjectOutputStream out = openOutput(s);
           ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

        SOSPFPacket packet = readPacket(in);
        stats.countReceived(packet.sospfType);
        switch (packet.sospfType) {
          case TYPE_ATTACH:
            handleAttachRequest(packet, out);
            break;
          case TYPE_HELLO:
            handleHello(packet, s, in, out);
            break;
          case TYPE_LSU:
            if (FINAL_LSAUPDATE.equals(packet.message)) {
              handleFinalLSAUpdate(packet, out);
            } else {
              receiveLSAUpdate(packet); // step1: merge update step2: broadcast update
            }
            break;
          case TYPE_APP_MESSAGE:
            handleApplicationMessage(packet);
            break;
          case TYPE_WEIGHT_UPDATE:
            handleWeightUpdate(packet, out);
            break;
          case TYPE_LINK_DOWN:
            handleLinkDown(packet, out);
            break;
          default:
            System.out.println("received unsupported packet type " + packet.sospfType
                + " from " + packet.srcIP + ";");
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      System.out.println("connection error: " + e.getMessage());
    }
  }

  // =======================================================================================
  // Attach
  // =======================================================================================

  /**
   * attach the link to the remote router, which is identified by the given simulated ip;
   * to establish the connection via socket, you need to indentify the process IP and process Port;
   * additionally, weight is the cost to transmitting data through the link
   * <p/>
   * NOTE: this command should not trigger link database synchronization
   */

  private void processAttach(String processIP, short processPort,
                             String simulatedIP, short weight) {
    attachLink(processIP, processPort, simulatedIP, weight);
  }

  /** Shared attach phase; null means THIS request did not establish a link. */
  private Link attachLink(String processIP, short processPort,
                          String simulatedIP, short weight) {
    if (shuttingDown) return null;
    // EDGE CASE: Dijkstra needs nonnegative weights. Validate before reserving
    // a port or sending a request; zero-cost links remain valid.
    if (weight < 0 || processPort <= 0) {
      System.out.println("Cannot attach: weight must be nonnegative and process port positive.");
      return null;
    }
    if (rd.simulatedIPAddress.equals(simulatedIP)) {
      System.out.println("Cannot attach to yourself.");
      return null;
    }
    // A rapid reconnect must first finish removal of the old session at this same peer.
    if (!settlePriorDisconnects(simulatedIP, processPort)) return null;
    synchronized (portLock) {
      if (shuttingDown) return null;
      if (findLink(simulatedIP) != null) {
        System.out.println("Cannot attach: a link to " + simulatedIP + " already exists.");
        return null;
      }
      if (!outgoingAttachRouters.add(simulatedIP)) {
        System.out.println("An attach request to " + simulatedIP + " is already pending.");
        return null;
      }
    }

    int port = -1;
    boolean committed = false;
    String requestId = null;
    try {
      port = reservePort();
      if (port == -1) {
        System.out.println("Cannot attach: all ports are in use.");
        return null;
      }

      try (Socket socket = openSocket(processIP, processPort);
           ObjectOutputStream out = openOutput(socket);
           ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

        SOSPFPacket request = newPacket(TYPE_ATTACH, simulatedIP);
        request.weight = weight;
        request.linkId = UUID.randomUUID().toString();
        requestId = request.linkId;
        RouterDescription pendingPeer = new RouterDescription();
        pendingPeer.processIPAddress = processIP;
        pendingPeer.processPortNumber = processPort;
        pendingPeer.simulatedIPAddress = simulatedIP;
        synchronized (portLock) {
          if (shuttingDown) return null;
          pendingAttachLinks.put(requestId, new Link(rd, pendingPeer, weight, requestId));
        }
        out.writeObject(request);
        out.flush();  // after we send the message we need to count number of TYPE_ATTACH it sent
        stats.countSent(TYPE_ATTACH);  // count the TYPE_ATTACH


        // EDGE CASE: human approval can take longer than HELLO_TIMEOUT_MS.
        // Connection/header reads are bounded, but the Y/N wait is deliberate.
        socket.setSoTimeout(0);

        // Blocks until the remote user has typed Y or N.
        SOSPFPacket reply = readPacket(in);
        stats.countReceived(reply.sospfType);
        if (reply.sospfType != TYPE_ATTACH || !simulatedIP.equals(reply.srcIP)
            || !rd.simulatedIPAddress.equals(reply.dstIP)) {
          System.out.println("Attach failed: unexpected reply.");
          return null;
        }

        if (ATTACH_ACCEPTED.equals(reply.message)) {
          if (reply.weight != weight || reply.linkId == null || reply.linkId.isEmpty()) {
            System.out.println("Attach failed: inconsistent link weight in reply.");
            return null;
          }
          RouterDescription remote = new RouterDescription();
          remote.processIPAddress = processIP;
          remote.processPortNumber = processPort;
          remote.simulatedIPAddress = simulatedIP;
          remote.status = null; // attached, but no HELLO exchanged yet
          Link link = new Link(rd, remote, weight, reply.linkId);
          synchronized (portLock) {
            if (shuttingDown) return null;
            committed = commitPort(port, link);
            if (!committed) {
              // EDGE CASE: an incoming attach may have installed the same link.
              // Reuse it only when both requests agree on endpoint and cost.
              link = findLink(simulatedIP);
              if (link == null || link.weight != weight
                  || !link.linkId.equals(reply.linkId)
                  || link.router2.processPortNumber != processPort) {
                System.out.println("Attach failed: conflicting concurrent request.");
                return null;
              }
            }
          }
          System.out.println("Your attach request has been accepted;");
          return link;
        } else if (findLink(simulatedIP) != null) {
          // Simultaneous attach: the peer rejected our request, but we accepted theirs.
          System.out.println("Your attach request has been rejected;"
              + " (a link to " + simulatedIP + " already exists from the other direction)");
        } else { 
          System.out.println("Your attach request has been rejected;");
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      System.out.println("Attach failed: " + e.getMessage());
    } finally {
      synchronized (portLock) { pendingAttachLinks.remove(requestId); }
      if (port != -1 && !committed) {
        releasePort(port); // release the reserved slot
      }
      outgoingAttachRouters.remove(simulatedIP);
    }
    return null;
  }

  /** The smaller simulated IP wins crossed requests, so costs cannot differ at each end. */
  private boolean preferOutgoingAttach(String remoteIP) {
    return outgoingAttachRouters.contains(remoteIP)
        && rd.simulatedIPAddress.compareTo(remoteIP) < 0;
  }

  /**
   * process request from the remote router.
   * For example: when router2 tries to attach router1. Router1 can decide whether it will accept this request.
   * Runs on a connection handler thread. Asks the user through askUserYesNo()
   * stores the link on "Y", and always sends exactly one reply to the requester.
   */
  private void handleAttachRequest(SOSPFPacket request, ObjectOutputStream out) throws IOException {
    // initialise an new packet and let the msg = rejected
    SOSPFPacket reply = newPacket(TYPE_ATTACH, request.srcIP);
    reply.message = ATTACH_REJECTED;
    reply.weight = request.weight;
    reply.linkId = request.linkId;

    // Only one request ask the user at a time. The lock is held until the reply has
    // been sent, so a second request can never pick up the answer before the first one.
    synchronized (attachApprovalLock) {
      int port = -1;
      boolean committed = false;
      try {
        Link existing;
        if (shuttingDown) {
          // No user approval or new adjacency is allowed during graceful shutdown.
          reply.message = ATTACH_REJECTED;
        } else if (!rd.simulatedIPAddress.equals(request.dstIP)) {
          // The requester typed the wrong simulated IP for this process.
          System.out.println("received attach request from " + request.srcIP
              + " addressed to " + request.dstIP + " (not me); rejected automatically;");

        } else if (request.weight < 0 || request.srcIP == null
            || request.linkId == null || request.linkId.isEmpty() || isRetiredLink(request.linkId)
            || rd.simulatedIPAddress.equals(request.srcIP) || request.srcProcessPort <= 0) {
          // EDGE CASE: validate incoming requests as well as local commands.
          System.out.println("Attach rejected: invalid neighbor or link weight.");
        } else if ((existing = findLink(request.srcIP)) != null) {
          // Keep this reference: disconnect may remove it before a second lookup.
          if (existing.weight == request.weight
              && existing.router2.processPortNumber == request.srcProcessPort) {
            reply.message = ATTACH_ACCEPTED;
            reply.weight = existing.weight;
            reply.linkId = existing.linkId;
          } else {
            System.out.println("Attach rejected: an existing link has a different endpoint or weight.");
          }

        } else if (preferOutgoingAttach(request.srcIP)) {
          System.out.println("Attach rejected: keeping the outgoing request to " + request.srcIP + ";");

        } else if ((port = reservePort()) == -1) { // there no free port
          System.out.println("received HELLO from " + request.srcIP
              + "; all ports are in use, rejected automatically;");

        } else {
          System.out.println("received HELLO from " + request.srcIP + ";");

          if (askUserYesNo(request.linkId)) {
            RouterDescription remote = new RouterDescription();
            remote.processIPAddress = request.srcProcessIP;
            remote.processPortNumber = request.srcProcessPort;
            remote.simulatedIPAddress = request.srcIP;
            remote.status = null; // attached, but no HELLO exchanged yet
            synchronized (portLock) {
              // EDGE CASE: an outgoing request may have started while Y/N was pending.
              if (!shuttingDown && !preferOutgoingAttach(request.srcIP)
                  && !retiredLinkIds.contains(request.linkId)) {
                committed = commitPort(port, new Link(rd, remote, request.weight, request.linkId));
                Link installed = findLink(request.srcIP);
                if (installed != null && installed.weight == request.weight
                    && installed.router2.processPortNumber == request.srcProcessPort) {
                  reply.message = ATTACH_ACCEPTED;
                  reply.linkId = installed.linkId;
                }
              }
            }
            System.out.println(ATTACH_ACCEPTED.equals(reply.message)
                ? "You accepted the attach request;" : "Attach rejected: conflicting concurrent request.");
          } else {
            System.out.println("You rejected the attach request;");
          }
        }
      } finally {
        if (port != -1 && !committed) {
          releasePort(port);
        }
      }

      out.writeObject(reply);
      out.flush();
      stats.countSent(TYPE_ATTACH);// when you reply the attach req, add to the stats
    }
  }

  /**
   * Publishes a decision box for the terminal thread and blocks until the user has typed
   * Y or N. Must be called while holding attachApprovalLock.
   */
  private boolean askUserYesNo(String linkId) {
    CompletableFuture<Boolean> decision = new CompletableFuture<>();
    synchronized (portLock) {
      if (shuttingDown || retiredLinkIds.contains(linkId)) return false;
      pendingAttachDecision = decision;
      pendingAttachId = linkId;
    }
    // EDGE CASE: publish the answer box BEFORE printing the question; otherwise
    // a fast Y/N may be parsed as a command and leave the requester waiting.
    System.out.println("Do you accept this request? (Y/N)");
    // it could be checked by other thread. At each iteration of the terminal thread
    // it would check if they receive this box.  Once they recieve they need to handle the request
    // and give an response
    try {
      return decision.get();
    } catch (InterruptedException | ExecutionException e) {
      return false;
    } finally {
      synchronized (portLock) {
        if (pendingAttachDecision == decision) pendingAttachDecision = null;
        if (linkId.equals(pendingAttachId)) pendingAttachId = null;
      }
    }
  }




  // =======================================================================================
  // start (HELLO handshake)
  // =======================================================================================

  // �������������������PA2�н����޸�

  /**
   * broadcast Hello to neighbors
   * <p/>
   * Performs the three-message HELLO handshake with every attached router. Each handshake
   * is short and needs no user input, so this runs directly on the terminal thread.
   */

  /**
   *  process start will do the 3-way handshake with all it's neighbors
   *  for those neighbors who haven't do the 3-way handshake (we add this
   * new connection to our lsa and also ask neighbor to add this link to theirs LSA)
   */

  // HELLO establishes neighbors; each start also exchanges the current full LSDB.
  private void processStart() {
    if (shuttingDown) return;
    List<Link> links = snapshotLinks();
    boolean complete = true;
    for (Link link : links) {
      if (!helloHandshake(link)) {
        complete = false;
      }
    }
    // EDGE CASE: repeated start and joining existing networks must exchange the
    // full LSDB even if this router's own LSA did not change.
    broadcastLSAUpdate(lsd.deepCopy(), null);

    if (!outgoingAttachRouters.isEmpty()) {
      complete = false;
      System.out.println("Note: some attach requests are still waiting for an answer;"
          + " run start again once they are accepted.");
    }
    if (complete) {
      synchronized (portLock) {
        if (shuttingDown) return;
        started = true;
      }
      if (links.isEmpty()) {
        System.out.println("Router started; no attached routers.");
      }
    } else {
      System.out.println("Start incomplete; run start again after the pending requests or handshake failures.");
    }
  }

  /**
   * Initiator side of the handshake 
   * Three HELLOs, followed by the peer's LSDB on the same socket as confirmation.
   * Returns handshake success, not whether an LSA entry was newly inserted.
   */

  // Shared by start and connect; callers advertise the completed adjacency.
  private boolean helloHandshake(Link link) {
    if (shuttingDown || getPort(link) < 0) return false;
    RouterDescription neighbor = link.router2;
    try (Socket socket = openSocket(neighbor.processIPAddress, neighbor.processPortNumber);
         ObjectOutputStream out = openOutput(socket);
         ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
      socket.setSoTimeout(HELLO_TIMEOUT_MS); // setup the terminate time

      // 1st HELLO
      out.writeObject(linkPacket(TYPE_HELLO, link));
      out.flush();
      stats.countSent(TYPE_HELLO); // <------- +1

      // 2nd HELLO (the neighbour's answer)
      SOSPFPacket reply = readPacket(in); 
      stats.countReceived(reply.sospfType);
      if (reply.sospfType != TYPE_HELLO || !neighbor.simulatedIPAddress.equals(reply.srcIP)
          || !link.linkId.equals(reply.linkId)
          || !rd.simulatedIPAddress.equals(reply.dstIP)) {
        System.out.println("start: unexpected reply from " + neighbor.simulatedIPAddress + ";");
        return false;
      }
      System.out.println("received HELLO from " + reply.srcIP + ";");
      // 3rd HELLO, so the neighbour also reaches TWO_WAY
      out.writeObject(linkPacket(TYPE_HELLO, link));
      out.flush();
      stats.countSent(TYPE_HELLO);  // <----- +1
      // PA3: wait until the peer has processed the third HELLO. Otherwise a
      // successful write alone could falsely report a complete connect.
      SOSPFPacket confirmation = readPacket(in);
      stats.countReceived(confirmation.sospfType);
      if (confirmation.sospfType != TYPE_LSU || confirmation.lsaArray == null
          || !link.linkId.equals(confirmation.linkId)
          || !neighbor.simulatedIPAddress.equals(confirmation.srcIP)
          || !rd.simulatedIPAddress.equals(confirmation.dstIP)) {
        System.out.println("HELLO failed: missing LSDB confirmation from " + neighbor.simulatedIPAddress);
        return false;
      }
      if (getPort(link) < 0) {
        return false;
      }
      advanceNeighborStatus(link, RouterStatus.TWO_WAY);
      // EDGE CASE: both ends may start simultaneously. Always try the idempotent
      // insertion, regardless of which thread advanced this neighbor's status.
      addNeighborToLSA(link);
      receiveLSAUpdate(confirmation);
      return isTwoWay(link);

    } catch (ConnectException e) {
      // "Connection refused": no process is listening on the neighbour's port any more, so
      // it has quit. Drop the dead link so the port is freed 
      // Other errors (eg: timeout) keep the link, the neighbour may only be slow.
      if (removeLink(link)) {
        System.out.println("start: " + neighbor.simulatedIPAddress
            + " is unreachable (connection refused); link removed;");
      }
      return false;
    } catch (IOException | ClassNotFoundException e) {
      System.out.println("start: cannot reach " + neighbor.simulatedIPAddress
          + ": " + e.getMessage());
      return false;
    }
  }

  /**
   * Responder side of the handshake, running on a connection-handler thread:
   * got HELLO (neighbour becomes INIT), send HELLO, wait for the final HELLO
   * (neighbour becomes TWO_WAY).
   */
  private void handleHello(SOSPFPacket first, Socket socket,
                           ObjectInputStream in, ObjectOutputStream out)
      throws IOException, ClassNotFoundException {
    Link link = findLink(first.srcIP); // check if the connection exist
    if (shuttingDown || link == null || !rd.simulatedIPAddress.equals(first.dstIP)
        || !link.linkId.equals(first.linkId) || link.router2.processPortNumber != first.srcProcessPort) {
      System.out.println("received HELLO from " + first.srcIP + " but it is not attached; ignored;");
      return;
    }

    System.out.println("received HELLO from " + first.srcIP + ";");
    advanceNeighborStatus(link, RouterStatus.INIT); // before attach it's null, at the first time it receive Hello it become INIT 

    out.writeObject(linkPacket(TYPE_HELLO, link));
    out.flush();
    stats.countSent(TYPE_HELLO); // <------------ +1

    socket.setSoTimeout(HELLO_TIMEOUT_MS);
    SOSPFPacket third = readPacket(in);
    stats.countReceived(third.sospfType);
    if (third.sospfType != TYPE_HELLO || !first.srcIP.equals(third.srcIP)
        || !link.linkId.equals(third.linkId)
        || !rd.simulatedIPAddress.equals(third.dstIP)) {
      System.out.println("start: unexpected packet from " + first.srcIP + ";");
      return;
    }
    System.out.println("received HELLO from " + third.srcIP + ";");
    if (getPort(link) < 0) {
      return;
    }
    advanceNeighborStatus(link, RouterStatus.TWO_WAY);
    boolean changed = addNeighborToLSA(link);
    // Confirm completion before flooding, which may involve slow neighbors.
    // Always exchange the full LSDB, including on a repeated handshake.
    SOSPFPacket update = linkPacket(TYPE_LSU, link);
    update.lsaArray = lsd.deepCopy();
    out.writeObject(update);
    out.flush();
    stats.countSent(TYPE_LSU);
    if (changed) {
      broadcastLSAUpdate(update.lsaArray, null);
    }
  }

  // =======================================================================================
  // neighbors / quit
  // =======================================================================================

  /**
   * output the neighbors of the routers
   */
  private void processNeighbors() {
    List<String> neighbors = new ArrayList<>();
    synchronized (portLock) {
      for (Link link : ports) {
        if (link != null && link.router2.status == RouterStatus.TWO_WAY) {
          neighbors.add(link.router2.simulatedIPAddress);
        }
      }
    }
    for (String ip : neighbors) {
      System.out.println(ip);
    }
  }

  /**
   * disconnect with all neighbors and quit the program
   */
  private void processQuit() {
    if (shutdownRouter()) System.exit(0);
  }

  /**
   * Freeze local topology, deliver the final self-LSA and notify every saved peer.
   * Returns false for an overlapping quit, which must not terminate the first one's work.
   */
  private boolean shutdownRouter() {
    List<DisconnectNotice> notices;
    Vector<LSA> finalLSAs = new Vector<>();
    synchronized (portLock) {
      if (shuttingDown) return false;
      shuttingDown = true;
      started = false;
      // Include earlier disconnects and outgoing attaches still waiting for Y/N.
      Map<String, DisconnectNotice> targets = new HashMap<>(pendingDisconnects);
      for (Map.Entry<String, Link> pending : pendingAttachLinks.entrySet()) {
        targets.putIfAbsent(pending.getKey(), new DisconnectNotice(pending.getValue(), -1));
      }
      for (int i = 0; i < ports.length; i++) {
        Link link = ports[i];
        if (link != null) {
          targets.putIfAbsent(link.linkId, new DisconnectNotice(link, i));
          link.pendingWeightUpdate = null;
          link.router2.status = null;
          ports[i] = null;
        }
        reservedPorts[i] = false;
      }
      notices = new ArrayList<>(targets.values());
      for (DisconnectNotice notice : notices) retiredLinkIds.add(notice.link.linkId);
      // EDGE CASE: withdraw all incident edges together, incrementing own seq only once.
      // This also creates a newer final LSA for an isolated / not-yet-started router.
      finalLSAs.add(lsd.withdrawOwnLinks());
      if (pendingAttachDecision != null) {
        pendingAttachDecision.complete(false);
        pendingAttachDecision = null;
      }
    }
    weightUpdates.shutdownNow();
    System.out.println("Broadcasting final LSAUPDATE");
    List<CompletableFuture<Void>> deliveries = new ArrayList<>();
    for (DisconnectNotice notice : notices) {
      deliveries.add(CompletableFuture.runAsync(() -> {
        // ports[] is already empty. Address the saved peer directly, including non-TWO_WAY links.
        sendFinalLSAUpdate(notice.link, finalLSAs);
        // Failure notification is distinct from the self-LSA and is still attempted if its ACK is lost.
        notifyLinkDown(notice);
      }, topologyTasks));
    }
    try {
      // Neighbors run independently. Bound the whole shutdown even if a peer never answers.
      CompletableFuture.allOf(deliveries.toArray(new CompletableFuture<?>[0]))
          .get(HELLO_TIMEOUT_MS * 20L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.out.println("Shutdown notifications interrupted; some neighbors are unconfirmed");
    } catch (ExecutionException | TimeoutException e) {
      System.out.println("Shutdown notifications incomplete; some neighbors are unconfirmed");
    }
    try {
      serverSocket.close();
    } catch (IOException ignored) {
      // nothing useful to do while exiting
    }
    topologyTasks.shutdownNow();
    System.out.println("Router shutting down.");
    return true;
  }

  /** Retries the identical final LSA; retransmission never increments its sequence number. */
  private boolean sendFinalLSAUpdate(Link link, Vector<LSA> finalLSAs) {
    SOSPFPacket request = linkPacket(TYPE_LSU, link);
    request.message = FINAL_LSAUPDATE;
    request.lsaArray = finalLSAs;
    for (int attempt = 0; attempt < DISCONNECT_ATTEMPTS; attempt++) {
      try (Socket socket = openSocket(link.router2.processIPAddress, link.router2.processPortNumber);
           ObjectOutputStream out = openOutput(socket);
           ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
        out.writeObject(request);
        out.flush();
        stats.countSent(TYPE_LSU);
        SOSPFPacket reply = readPacket(in);
        stats.countReceived(reply.sospfType);
        if (reply.sospfType == TYPE_LSU && FINAL_LSAUPDATE_ACK.equals(reply.message)
            && link.linkId.equals(reply.linkId) && link.router2.simulatedIPAddress.equals(reply.srcIP)
            && rd.simulatedIPAddress.equals(reply.dstIP) && reply.lsaArray != null
            && reply.lsaArray.size() == 1 && reply.lsaArray.firstElement() != null
            && rd.simulatedIPAddress.equals(reply.lsaArray.firstElement().linkStateID)
            && reply.lsaArray.firstElement().lsaSeqNumber == finalLSAs.firstElement().lsaSeqNumber) {
          return true;
        }
      } catch (IOException | ClassNotFoundException e) {
        // A lost ACK is not evidence that the final LSA was not installed.
      }
    }
    System.out.println("Final LSAUPDATE unconfirmed for " + link.router2.simulatedIPAddress);
    return false;
  }

  /** Install first, acknowledge even a duplicate, and flood only accepted newer LSAs. */
  private void handleFinalLSAUpdate(SOSPFPacket packet, ObjectOutputStream out) throws IOException {
    if (!rd.simulatedIPAddress.equals(packet.dstIP) || packet.srcIP == null
        || rd.simulatedIPAddress.equals(packet.srcIP) || packet.srcProcessPort <= 0
        || packet.linkId == null || packet.linkId.isEmpty() || packet.lsaArray == null
        || packet.lsaArray.size() != 1) throw new IOException("invalid final LSAUPDATE");
    LSA finalLSA = packet.lsaArray.firstElement();
    if (finalLSA == null || !packet.srcIP.equals(finalLSA.linkStateID) || finalLSA.links == null
        || finalLSA.links.stream().anyMatch(ld -> ld == null || ld.weight < 0
            || !packet.srcIP.equals(ld.linkID))) throw new IOException("invalid final self-LSA");
    // This is deliberately independent of local adjacency state: simultaneous quit/disconnect
    // or a lost ACK may have already retired this link. Session cleanup belongs to LINK_DOWN.
    List<LSA> updates = lsd.merge(packet.lsaArray);
    SOSPFPacket reply = newPacket(TYPE_LSU, packet.srcIP);
    reply.message = FINAL_LSAUPDATE_ACK;
    reply.linkId = packet.linkId;
    reply.lsaArray = packet.lsaArray;
    try {
      out.writeObject(reply);
      out.flush();
      stats.countSent(TYPE_LSU);
    } finally {
      if (!updates.isEmpty()) broadcastLSAUpdate(new Vector<>(updates), packet.srcIP);
    }
  }

  // =======================================================================================
  // PA2: detect computes a path from the current LSDB.
  // =======================================================================================

  /**
   * output the shortest path to the given destination ip
   * <p/>
   * format: source ip address  -> ip address -> ... -> destination ip
   *
   * @param destinationIP the ip adderss of the destination simulated router
   */
  private void processDetect(String destinationIP) {
    String path = lsd.getShortestPath(destinationIP);
    if (path == null) {
      System.out.println("Path not found");
    } else {
      System.out.println("Path found: " + path);
    }
  }

  /**
   * disconnect with the router identified by the given destination ip address
   * Notice: this command should trigger the synchronization of database
   *
   * @param portNumber the port number which the link attaches at
   */
  private void processDisconnect(int portNumber) {
    List<DisconnectNotice> notices = new ArrayList<>();
    boolean removed = false;
    synchronized (portLock) {
      if (shuttingDown) return;
      if (portNumber < 0 || portNumber >= ports.length) {
        System.out.println("Cannot disconnect: port must be between 0 and 3");
        return;
      }
      Link link = ports[portNumber];
      if (link != null) {
        DisconnectNotice notice = new DisconnectNotice(link, portNumber);
        removeLink(link); // Port, own LSA and pending weight state change atomically.
        pendingDisconnects.put(link.linkId, notice);
        notices.add(notice);
        removed = true;
      } else {
        // Retry unconfirmed notices for this old slot without touching any newer adjacency.
        for (DisconnectNotice notice : pendingDisconnects.values()) {
          if (notice.port == portNumber) notices.add(notice);
        }
        if (notices.isEmpty()) {
          System.out.println("Cannot disconnect: port " + portNumber + " is unused");
          return;
        }
      }
    }
    if (removed) {
      System.out.println("Port " + portNumber + " disconnected");
      System.out.println("Broadcasting LSAUPDATE");
      // Notify the peer and flood to remaining neighbors independently; neither holds portLock.
      executeTopologyTask(() -> broadcastLSAUpdate(lsd.deepCopy(), null));
    }
    for (DisconnectNotice notice : notices) queueDisconnectNotice(notice);
  }

  private boolean isRetiredLink(String id) {
    synchronized (portLock) { return retiredLinkIds.contains(id); }
  }

  private boolean executeTopologyTask(Runnable task) {
    try {
      topologyTasks.execute(task);
      return true;
    } catch (RejectedExecutionException e) {
      // An earlier disconnect may reach this enqueue after quit has finished cleanup.
      if (!shuttingDown) throw e;
      return false;
    }
  }

  private CompletableFuture<Boolean> queueDisconnectNotice(DisconnectNotice notice) {
    CompletableFuture<Boolean> attempt;
    synchronized (portLock) {
      if (pendingDisconnects.get(notice.link.linkId) != notice) {
        return CompletableFuture.completedFuture(true);
      }
      if (notice.attempt != null && !notice.attempt.isDone()) return notice.attempt;
      attempt = new CompletableFuture<>();
      notice.attempt = attempt;
    }
    boolean queued = executeTopologyTask(() -> {
      boolean confirmed = notifyLinkDown(notice);
      synchronized (portLock) {
        if (confirmed) pendingDisconnects.remove(notice.link.linkId);
      }
      attempt.complete(confirmed);
    });
    if (!queued) attempt.complete(false);
    return attempt;
  }

  private boolean settlePriorDisconnects(String simulatedIP, short processPort) {
    List<DisconnectNotice> notices = new ArrayList<>();
    synchronized (portLock) {
      for (DisconnectNotice notice : pendingDisconnects.values()) {
        RouterDescription peer = notice.link.router2;
        if (simulatedIP.equals(peer.simulatedIPAddress) && processPort == peer.processPortNumber) {
          notices.add(notice);
        }
      }
    }
    for (DisconnectNotice notice : notices) {
      try {
        if (queueDisconnectNotice(notice).get(HELLO_TIMEOUT_MS * 4L, TimeUnit.MILLISECONDS)) continue;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException | TimeoutException e) {
        // Keep the saved notice available to retry without resurrecting its session.
      }
      System.out.println("Cannot attach: previous disconnect is unconfirmed; retry disconnect " + notice.port);
      return false;
    }
    return true;
  }

  private boolean notifyLinkDown(DisconnectNotice notice) {
    Link link = notice.link;
    SOSPFPacket request = linkPacket(TYPE_LINK_DOWN, link);
    request.message = LINK_DOWN;
    String failure = "missing confirmation";
    for (int attempt = 0; attempt < DISCONNECT_ATTEMPTS; attempt++) {
      try (Socket socket = openSocket(link.router2.processIPAddress, link.router2.processPortNumber);
           ObjectOutputStream out = openOutput(socket);
           ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
        out.writeObject(request);
        out.flush();
        stats.countSent(TYPE_LINK_DOWN);
        SOSPFPacket reply = readPacket(in);
        stats.countReceived(reply.sospfType);
        if (reply.sospfType == TYPE_LINK_DOWN && LINK_DOWN_ACK.equals(reply.message)
            && link.linkId.equals(reply.linkId) && link.router2.simulatedIPAddress.equals(reply.srcIP)
            && rd.simulatedIPAddress.equals(reply.dstIP)) return true;
        failure = "unexpected confirmation";
      } catch (IOException | ClassNotFoundException e) {
        failure = e.getMessage();
      }
    }
    System.out.println("Disconnect notification unconfirmed for " + link.router2.simulatedIPAddress
        + ": " + failure + (shuttingDown ? "" : "; retry disconnect " + notice.port));
    return false;
  }

  private void handleLinkDown(SOSPFPacket request, ObjectOutputStream out) throws IOException {
    SOSPFPacket reply = newPacket(TYPE_LINK_DOWN, request.srcIP);
    reply.linkId = request.linkId;
    reply.message = "LINK_DOWN_REJECTED";
    boolean removed = false;
    synchronized (portLock) {
      Link link = findLink(request.srcIP);
      if (rd.simulatedIPAddress.equals(request.dstIP) && request.srcIP != null
          && !rd.simulatedIPAddress.equals(request.srcIP) && request.srcProcessPort > 0
          && LINK_DOWN.equals(request.message) && request.linkId != null && !request.linkId.isEmpty()
          && (link == null || !link.linkId.equals(request.linkId)
              || link.router2.processPortNumber == request.srcProcessPort)) {
        if (link != null && link.linkId.equals(request.linkId)) removed = removeLink(link);
        // ACK duplicate/old notifications too, while leaving a reconnected link untouched.
        retiredLinkIds.add(request.linkId);
        // Cancel an unanswered incoming attach for this session; its handler releases the reservation.
        if (request.linkId.equals(pendingAttachId) && pendingAttachDecision != null) {
          pendingAttachDecision.complete(false);
          pendingAttachDecision = null;
        }
        reply.message = LINK_DOWN_ACK;
      }
    }
    try {
      out.writeObject(reply);
      out.flush();
      stats.countSent(TYPE_LINK_DOWN);
    } finally {
      if (removed) broadcastLSAUpdate(lsd.deepCopy(), null);
    }
  }

  /**
   * attach the link to the remote router, which is identified by the given simulated ip;
   * to establish the connection via socket, you need to indentify the process IP and process Port;
   * additionally, weight is the cost to transmitting data through the link
   * <p/>
   * This command does trigger the link database synchronization
   */
  private void processConnect(String processIP, short processPort,
                              String simulatedIP, short weight) {
    if (shuttingDown) return;
    if (!started) {
      System.out.println("Cannot connect: run start first and let it complete.");
      return;
    }
    Link link = attachLink(processIP, processPort, simulatedIP, weight);
    if (link == null) {
      return; // EDGE CASE: never handshake an old link after a rejected request.
    }
    if (!helloHandshake(link)) {
      // Keep a timed-out attach for a later start retry; unilateral rollback
      // would leave the peer's port occupied. No success is printed here.
      System.out.println("Connect failed: HELLO handshake incomplete; run start to retry if still attached.");
      return;
    }
    System.out.println("Link established with " + simulatedIP);
    System.out.println("Broadcasting LSAUPDATE");
    // Both components must learn each other's existing topology, not just the new edge.
    broadcastLSAUpdate(lsd.deepCopy(), null);
  }

  // =======================================================================================
  // PA3: explicit weight synchronization (separate from LSA flooding / failure detection)
  // =======================================================================================

  private static int compareWeightVersion(long version, String origin,
                                           long otherVersion, String otherOrigin) {
    int order = Long.compare(version, otherVersion);
    return order != 0 ? order : origin.compareTo(otherOrigin);
  }

  /** Caller holds portLock; never install a weight from a stale or duplicate version. */
  private boolean applyWeightUpdate(Link link, int weight, long version, String origin) {
    if (shuttingDown || getPort(link) < 0) return false;
    link.weightClock = Math.max(link.weightClock, version);
    if (compareWeightVersion(version, origin, link.weightVersion, link.weightOrigin) <= 0) {
      return false;
    }
    link.weight = weight;
    link.weightVersion = version;
    link.weightOrigin = origin;
    return lsd.updateLinkWeight(link.router2.simulatedIPAddress, weight);
  }

  private SOSPFPacket weightRequest(Link link, int weight, long version, String origin) {
    SOSPFPacket packet = linkPacket(TYPE_WEIGHT_UPDATE, link);
    packet.message = WEIGHT_REQUEST;
    packet.weight = weight;
    packet.weightVersion = version;
    packet.weightOrigin = origin;
    packet.weightRequestId = version + ":" + origin;
    return packet;
  }

  /** Only the two endpoints may originate versions for this adjacency. */
  private boolean validWeightVersion(SOSPFPacket packet, Link link) {
    return packet.weight >= 0 && packet.weightVersion > 0 && packet.weightOrigin != null
        && link.linkId.equals(packet.linkId)
        && (packet.weightOrigin.equals(rd.simulatedIPAddress)
            || packet.weightOrigin.equals(link.router2.simulatedIPAddress));
  }

  /** The receiver resolves srcIP to its own slot; the sender's port index is never transmitted. */
  private void handleWeightUpdate(SOSPFPacket request, ObjectOutputStream out) throws IOException {
    SOSPFPacket reply = newPacket(TYPE_WEIGHT_UPDATE, request.srcIP);
    reply.message = WEIGHT_REJECTED;
    reply.weightRequestId = request.weightRequestId;
    reply.linkId = request.linkId;
    boolean changed = false;
    synchronized (portLock) {
      Link link = findLink(request.srcIP);
      if (!shuttingDown && link != null && rd.simulatedIPAddress.equals(request.dstIP)
          && link.router2.processPortNumber == request.srcProcessPort
          && WEIGHT_REQUEST.equals(request.message) && validWeightVersion(request, link)
          && (request.weightVersion + ":" + request.weightOrigin).equals(request.weightRequestId)) {
        int order = compareWeightVersion(request.weightVersion, request.weightOrigin,
            link.weightVersion, link.weightOrigin);
        // Equal versions must refer to the same value. Older requests get the current value.
        if (order != 0 || request.weight == link.weight) {
          changed = applyWeightUpdate(link, request.weight, request.weightVersion, request.weightOrigin);
          reply.message = WEIGHT_ACCEPTED;
          reply.weight = link.weight;
          reply.weightVersion = link.weightVersion;
          reply.weightOrigin = link.weightOrigin;
        }
      }
    }
    try {
      // Confirm before potentially slow flooding. A lost ACK is safe to retry with the same version.
      out.writeObject(reply);
      out.flush();
      stats.countSent(TYPE_WEIGHT_UPDATE);
    } finally {
      // Even if ACK transmission fails, advertise the change that was already accepted locally.
      if (changed) broadcastLSAUpdate(lsd.deepCopy(), null);
    }
  }

  private SOSPFPacket exchangeWeightUpdate(Link link, SOSPFPacket request)
      throws IOException, ClassNotFoundException {
    RouterDescription neighbor = link.router2;
    try (Socket socket = openSocket(neighbor.processIPAddress, neighbor.processPortNumber);
         ObjectOutputStream out = openOutput(socket);
         ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
      out.writeObject(request);
      out.flush();
      stats.countSent(TYPE_WEIGHT_UPDATE);
      SOSPFPacket reply = readPacket(in);
      stats.countReceived(reply.sospfType);
      if (reply.sospfType != TYPE_WEIGHT_UPDATE || !WEIGHT_ACCEPTED.equals(reply.message)
          || !neighbor.simulatedIPAddress.equals(reply.srcIP)
          || !rd.simulatedIPAddress.equals(reply.dstIP)
          || !request.weightRequestId.equals(reply.weightRequestId) || !validWeightVersion(reply, link)) {
        throw new IOException("invalid or rejected weight confirmation");
      }
      int order = compareWeightVersion(reply.weightVersion, reply.weightOrigin,
          request.weightVersion, request.weightOrigin);
      if (order < 0 || (order == 0 && reply.weight != request.weight)) {
        throw new IOException("inconsistent weight confirmation");
      }
      return reply;
    }
  }

  /**
   * update the weight of a specific port.
   * This change should trigger synchronization of the Link State Database by sending
   * a Link State Advertisement (LSA) update to all neighboring routers in the topology.
   *
   * @param portNumber the port number (0-3) to update
   * @param newWeight the new weight/cost for the link attached to this port
   */
  private void processUpdate(int portNumber, int newWeight, Link expectedLink) {
    Link link;
    synchronized (portLock) {
      if (shuttingDown) return;
      if (portNumber < 0 || portNumber >= ports.length || newWeight < 0) {
        System.out.println("Cannot update: port must be between 0 and 3 and weight must be nonnegative");
        return;
      }
      link = ports[portNumber];
      if (link == null) {
        System.out.println("Cannot update: port " + portNumber + " is unused");
        return;
      }
      if (link != expectedLink) {
        System.out.println("Cannot update: link changed while command was queued");
        return;
      }
    }
    // This lock is never acquired by an incoming handler. Do not hold portLock during network I/O.
    synchronized (link.weightUpdateLock) {
      SOSPFPacket request;
      synchronized (portLock) {
        if (shuttingDown || ports[portNumber] != link) {
          System.out.println("Cannot update: link changed before the update started");
          return;
        }
        if (link.pendingWeightUpdate != null && link.pendingWeightUpdate.weight == newWeight) {
          request = link.pendingWeightUpdate;
        } else {
          // EDGE CASE: a previous ACK may have been lost. Even if the requested value equals
          // our old local value, send a NEWER version to override that unconfirmed remote update.
          if (link.pendingWeightUpdate == null && link.weight == newWeight) {
            System.out.println("Link weight unchanged");
            return;
          }
          if (link.weightClock == Long.MAX_VALUE) {
            System.out.println("Cannot update: weight version counter exhausted");
            return;
          }
          request = weightRequest(link, newWeight, ++link.weightClock, rd.simulatedIPAddress);
          link.pendingWeightUpdate = request;
        }
      }
      String failure = "concurrent update still settling";
      for (int attempt = 0; attempt < WEIGHT_UPDATE_ATTEMPTS; attempt++) {
        synchronized (portLock) {
          if (shuttingDown || ports[portNumber] != link) {
            System.out.println("Cannot update: link changed while awaiting confirmation");
            return;
          }
        }
        try {
          SOSPFPacket reply = exchangeWeightUpdate(link, request);
          boolean settled;
          boolean changed;
          int currentWeight;
          synchronized (portLock) {
            if (shuttingDown || ports[portNumber] != link) {
              System.out.println("Cannot update: link changed while awaiting confirmation");
              return;
            }
            changed = applyWeightUpdate(link, reply.weight, reply.weightVersion, reply.weightOrigin);
            settled = compareWeightVersion(link.weightVersion, link.weightOrigin,
                reply.weightVersion, reply.weightOrigin) == 0;
            currentWeight = link.weight;
            if (settled) {
              link.pendingWeightUpdate = null;
            } else {
              // An incoming concurrent update won while this request was in flight.
              // Send the winning value back rather than overwriting it with a late ACK.
              request = weightRequest(link, link.weight, link.weightVersion, link.weightOrigin);
              link.pendingWeightUpdate = request;
            }
          }
          if (settled) {
            System.out.println(currentWeight == newWeight ? "Link weight updated"
                : "Weight update superseded; current weight: " + currentWeight);
            System.out.println("Broadcasting LSAUPDATE");
            broadcastLSAUpdate(lsd.deepCopy(), null);
            return;
          }
          if (changed) broadcastLSAUpdate(lsd.deepCopy(), null);
        } catch (IOException | ClassNotFoundException e) {
          failure = e.getMessage();
        }
      }
      // A timeout cannot prove that the peer did not apply the request. Keep its version
      // for an idempotent retry; do not roll back blindly or pretend both endpoints succeeded.
      synchronized (portLock) {
        if (ports[portNumber] != link) {
          System.out.println("Cannot update: link changed while awaiting confirmation");
          return;
        }
      }
      System.out.println("Weight update unconfirmed: " + failure
          + "; retry update " + portNumber + " " + newWeight);
    }
  }

  /**
   * send an application-level message from this router to the destination router.
   * The message must be forwarded hop-by-hop according to the current shortest path.
   * <p/>
   * When you run send, the window of the router where you run the command should print:
   * "Sending message to <Destination IP>"
   * <p/>
   * For each intermediate router on the shortest path (excluding the source and destination),
   * the router window should print:
   * "Forwarding message from <Source IP> to <Destination IP>"
   * <p/>
   * When the destination router receives the message, the router window should print:
   * "Received message from <Source IP>;"
   * "Message: <Message>"
   *
   * @param destinationIP the simulated IP address of the destination router
   * @param message the message content to send
   */ // <-------------------------------------PA2
  private void processSend(String destinationIP, String message) {
    if (destinationIP.equals(rd.simulatedIPAddress)) {
      // EDGE CASE: PA2 clarification requires rejecting this router as destination.
      // Return before creating a packet, printing its payload, or updating counters.
      System.out.println("Cannot send to yourself.");
      return;
    }
    Link link = nextHopLink(destinationIP);
    if (link  == null) {
      System.out.println("Path not found");
      return;
    }
    SOSPFPacket packet = newPacket(TYPE_APP_MESSAGE, destinationIP);
    packet.message = message;
    System.out.println("Sending message to " + destinationIP);
    sendOneWayMessage(link.router2, packet);
  }

  /**
   * handle incoming application message packet.
   * This method should be called when a router receives a SOSPFPacket with sospfType = 2 (Application Message).
   * <p/>
   * If this router is the destination (packet.dstIP equals this router's simulatedIPAddress):
   * - Print "Received message from <Source IP>;"
   * - Print "Message: <Message>"
   * <p/>
   * If this router is an intermediate router:
   * - Print "Forwarding message from <Source IP> to <Destination IP>"
   * - Forward the packet to the next hop on the shortest path to the destination
   * - Do NOT print or inspect the message payload
   *
   * @param packet the received application message packet
   */ // <-------------------------- PA2
  private void handleApplicationMessage(socs.network.message.SOSPFPacket packet) {
    // router is the destination
    if (packet.dstIP.equals(rd.simulatedIPAddress)) {
      System.out.println("Received message from " + packet.srcIP + ";");
      System.out.println("Message: " + packet.message);
      return;
    }

    // If the router not the destination 
    Link nextLink = nextHopLink(packet.dstIP);
    if (nextLink == null) {
      System.out.println("Path not found");
      return;
    }
    System.out.println("Forwarding message from " + packet.srcIP + " to " + packet.dstIP);
    System.out.println("Next hop: " + nextLink.router2.simulatedIPAddress);
    // EDGE CASE: across multiple hops, preserve the original source and payload.
    // The intermediate router only uses dstIP to choose its next link.
    sendOneWayMessage(nextLink.router2, packet);
  }

  // =======================================================================================
  // Terminal
  // =======================================================================================

  /**
   * Command loop of the terminal thread. Every iteration reads one line; if a remote attach
   * request is waiting for an answer, that line is the Y/N answer, otherwise it is a command.
   */
  public void terminal() {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    try {
      while (true) {
        System.out.print(">> ");
        String command = br.readLine();
        if (command == null) {
          processQuit(); // end of input (Ctrl-D / Ctrl-Z): behave like quit
          return;
        }
        command = command.trim();
        if (command.isEmpty()) {
          continue;
        }

        // EDGE CASE: quit must remain available even while an attach asks for Y/N.
        if (command.equals("quit")) {
          processQuit();
          return;
        }

        // The line answers a pending attach request, not a command.
        CompletableFuture<Boolean> decision = pendingAttachDecision;
        if (decision != null) {
          if (command.equalsIgnoreCase("Y")) {
            // Clear the box BEFORE completing it: the waiting handler wakes up on complete()
            // and may immediately publish the next question, which must not be wiped out.
            pendingAttachDecision = null;
            decision.complete(true);
          } else if (command.equalsIgnoreCase("N")) {
            pendingAttachDecision = null;
            decision.complete(false);
          } else {
            System.out.println("Please enter Y or N.");
          }
          continue;
        }

        try {
          runCommand(command);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
          System.out.println("Invalid arguments: " + command);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private boolean hasArgumentCount(String[] arguments, int expected, String usage) {
    if (arguments.length == expected) {
      return true;
    }
    System.out.println("Usage: " + usage);
    return false;
  }

  /** Parses one command line and runs the matching process* method. */
  private void runCommand(String command) {
    if (shuttingDown) return;
    command = command.trim();
    if (command.isEmpty()) {
      return;
    }
    // EDGE CASE: consecutive spaces or tabs are valid command separators.
    // Check argument counts before indexing; an incomplete command must not crash.
    String[] arguments = command.split("\\s+");
    switch (arguments[0]) {
      case "detect":
        if (hasArgumentCount(arguments, 2, "detect [Destination IP]")) {
          processDetect(arguments[1]);
        }
        break;
      case "disconnect":
        if (hasArgumentCount(arguments, 2, "disconnect [Port Number]")) {
          processDisconnect(Integer.parseInt(arguments[1]));
        }
        break;
      case "quit":
        if (hasArgumentCount(arguments, 1, "quit")) {
          processQuit();
        }
        break;
      case "stats":
        if (hasArgumentCount(arguments, 1, "stats")) {
          System.out.println(stats.reportMessageCount());
        }
        break;
      case "attach":
        if (hasArgumentCount(arguments, 5,
            "attach [Process IP] [Process Port] [Simulated IP] [Link Weight]")) {
          String processIP = arguments[1];
          short processPort = Short.parseShort(arguments[2]);
          String simulatedIP = arguments[3];
          short weight = Short.parseShort(arguments[4]);
          // Human approval must not block this terminal from handling incoming Y/N.
          Thread attach = new Thread(
              () -> processAttach(processIP, processPort, simulatedIP, weight),
              "attach-" + simulatedIP);
          attach.setDaemon(true);
          attach.start();
        }
        break;
      case "start":
        if (hasArgumentCount(arguments, 1, "start")) {
          processStart();
        }
        break;
      case "lsd":
        if (hasArgumentCount(arguments, 1, "lsd")) {
          System.out.println(lsd);
        }
        break;
      case "connect":
        if (hasArgumentCount(arguments, 5,
            "connect [Process IP] [Process Port] [Simulated IP] [Link Weight]")) {
          String processIP = arguments[1];
          short processPort = Short.parseShort(arguments[2]);
          String simulatedIP = arguments[3];
          short weight = Short.parseShort(arguments[4]);
          if (!started) {
            System.out.println("Cannot connect: run start first and let it complete.");
            break;
          }
          // EDGE CASE: keep the terminal available for incoming Y/N during crossed connects.
          Thread connect = new Thread(
              () -> processConnect(processIP, processPort, simulatedIP, weight),
              "connect-" + simulatedIP);
          connect.setDaemon(true);
          connect.start();
        }
        break;
      case "neighbors":
        if (hasArgumentCount(arguments, 1, "neighbors")) {
          processNeighbors();
        }
        break;
      case "send":
        // EDGE CASE: tokenize only the command and destination; preserve spaces
        // and tabs inside the application payload rather than rebuilding the text.
        String[] messageArguments = command.split("\\s+", 3);
        if (hasArgumentCount(messageArguments, 3, "send [Destination IP] [Message]")) {
          processSend(messageArguments[1], messageArguments[2]);
        }
        break;
      case "update":
        if (hasArgumentCount(arguments, 3, "update [Port Number] [New Weight]")) {
          int portNumber = Integer.parseInt(arguments[1]);
          int newWeight = Integer.parseInt(arguments[2]);
          Link expectedLink;
          synchronized (portLock) {
            expectedLink = portNumber >= 0 && portNumber < ports.length ? ports[portNumber] : null;
          }
          weightUpdates.execute(() -> processUpdate(portNumber, newWeight, expectedLink));
        }
        break;
      default:
        System.out.println("Unknown command: " + command);
    }
  }
}

package socs.network.node;

import socs.network.message.SOSPFPacket;
import java.util.UUID;

public class Link {

  RouterDescription router1;
  RouterDescription router2;
  int weight;  // <- new field record for link's weight
  final String linkId; // Shared attach-session ID; a reconnect always uses a new one.

  // Guarded by Router.portLock. Versions order changes independently of LSA sequence numbers.
  long weightClock = 0;
  long weightVersion = 0;
  String weightOrigin = "";
  SOSPFPacket pendingWeightUpdate;
  // Serializes outgoing updates only; incoming handlers never take this lock.
  final Object weightUpdateLock = new Object();

  public Link(RouterDescription r1, RouterDescription r2, int w) {
    this(r1, r2, w, UUID.randomUUID().toString());
  }

  public Link(RouterDescription r1, RouterDescription r2, int w, String id) {
    router1 = r1;
    router2 = r2;
    weight = w;
    linkId = id;
  }
}

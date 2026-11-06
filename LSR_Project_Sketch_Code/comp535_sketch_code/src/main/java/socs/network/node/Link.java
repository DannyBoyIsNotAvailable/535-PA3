package socs.network.node;

import socs.network.message.SOSPFPacket;

public class Link {

  RouterDescription router1;
  RouterDescription router2;
  int weight;  // <- new field record for link's weight

  // Guarded by Router.portLock. Versions order changes independently of LSA sequence numbers.
  long weightClock = 0;
  long weightVersion = 0;
  String weightOrigin = "";
  SOSPFPacket pendingWeightUpdate;
  // Serializes outgoing updates only; incoming handlers never take this lock.
  final Object weightUpdateLock = new Object();

  public Link(RouterDescription r1, RouterDescription r2, int w) {
    router1 = r1;
    router2 = r2;
    weight = w;
  }
}

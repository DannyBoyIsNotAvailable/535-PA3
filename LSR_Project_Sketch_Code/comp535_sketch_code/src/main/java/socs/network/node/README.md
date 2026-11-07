# PA2 and PA3 Implementation Notes

This README describes PA2 and the PA3 `connect`, `update`, `disconnect` and graceful `quit` extensions. Run all commands below from the Java project directory containing `pom.xml`, `src/`, and `conf/`. Disconnect and quit use explicit adjacency-loss notifications as documented below.

## Collaboration Statement

Danny Chou and Rui Zhang each independently implemented all parts of the code. Danny Chou was then responsible for debugging the code and checking edge cases, and Rui Zhang was responsible for writing this README. We both carried out the final testing of the program together.

## Requirements

- JDK 8 or later and Maven 3 or later.
- Run one Java process per router, using a different configuration file in each terminal.
- Rebuild after downloading or changing source code. Generated `target/` and `out/` files are not versioned.

## Build

From the directory containing `pom.xml`:

```text
mvn clean package assembly:single
```

This produces `target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar`.

## Start five routers

Open five terminals in the Java project directory. Run one command per terminal:

```text
java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router1.conf
java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router2.conf
java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router3.conf
java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router4.conf
java -jar target/COMP535-1.0-SNAPSHOT-jar-with-dependencies.jar conf/router5.conf
```

Each process prints its **Process IP**, **Process Port**, and **Simulated IP**. Copy these values from the destination terminal when attaching. Process ports may change after a restart.

| Configuration | Simulated IP |
|---|---|
| `router1.conf` | `192.168.1.1` |
| `router2.conf` | `192.168.1.100` |
| `router3.conf` | `192.168.2.1` |
| `router4.conf` | `192.168.3.1` |
| `router5.conf` | `192.168.4.1` |

**Process IP + Process Port** identify the real TCP endpoint. **Simulated IP** identifies a router in the topology and is used by LSA, `detect`, and `send`. The four local link slots (0-3) are different from process ports. No router address is hardcoded to a loopback address.

## Commands

Type these at the router's `>>` prompt. Brackets describe placeholders: replace them with actual values and do not type the brackets. A process port must be a number.

| Command | Behavior |
|---|---|
| `attach [Process IP] [Process Port] [Simulated IP] [Link Weight]` | Request a link using the destination's displayed identifiers. The receiver answers `Y` or `N`. Weight must be nonnegative. |
| `start` | Perform HELLO handshakes and exchange LSDB snapshots with TWO_WAY neighbors. |
| `connect [Process IP] [Process Port] [Simulated IP] [Link Weight]` | After a completed local `start`, request a new link, complete HELLO and synchronize/flood the LSDB automatically. The receiver answers `Y` or `N`. |
| `update [Port Number] [New Weight]` | Synchronize the weight of an occupied local slot (0-3) with its direct neighbor, update both endpoints' LSAs and flood the changes. New weight is a nonnegative Java int. |
| `disconnect [Port Number]` | Remove an occupied local slot (0-3), notify its peer to remove the same adjacency, and flood both revised self-originated LSAs. |
| `neighbors` | Print directly connected TWO_WAY neighbors. |
| `lsd` | Display the local link-state database. |
| `detect [Destination Simulated IP]` | Compute and print the weighted shortest path. |
| `send [Destination Simulated IP] [Message]` | Deliver an application message along the shortest path. The message may contain spaces. |
| `stats` | Display sent/received counts for HELLO, LSAUPDATE, APP_MESSAGE, ATTACH, WEIGHT_UPDATE and LINK_DOWN. |
| `quit` | Withdraw all local adjacencies, deliver the final self-originated LSA, notify peers to clean up, then close the listener and exit. |

`attach` stores the approved link but does not synchronize the LSDB. Wait for acceptance before running `start`. Commands accept repeated spaces or tabs between arguments and report usage for missing or extra arguments. `send` preserves whitespace inside the message body.

## PA3 connect

- Run `start` on the initiating router first. An isolated router can complete `start` and then use `connect`. A first start with failed handshakes or pending outgoing attach requests must be retried before connect is enabled. Receiving another router's HELLO does not count as executing local start; the receiver of connect does not itself need to have run start.
- Connect shares the attach validation and port reservation code. Rejected requests, duplicate/pending links, self-links, full ports, negative weights and invalid process ports do not proceed to HELLO. Zero weight and local port slot 0 are valid. Process ports use the existing positive Java `short` range (1-32767).
- Attach and connect run in background threads, keeping both terminals available to answer incoming Y/N questions. If requests cross, the outgoing request from the lexicographically smaller simulated IP takes precedence. This prevents the two ends from accepting different link weights. Reservations count toward capacity; if crossed requests exhaust the remaining slots, retry after the rejected request releases its reservation.
- After three HELLO messages, the responder adds the link to its own LSA and returns a full LSDB in an LSAUPDATE on the same socket. This confirms processing of the third HELLO. The initiator then adds its own link, merges the received database and broadcasts its current full LSDB. Each endpoint increments only its own LSA sequence number, once per newly added link. Repeated HELLO/start does not duplicate the link or increase that sequence again.
- Both ends exchange their existing topology so connect can join two running network components. Accepted new/newer LSAs are flooded to all TWO_WAY neighbors except their incoming neighbor. `detect` and `send` compute paths from the current LSDB.
- A HELLO timeout prints failure, never `Link established`. An already accepted attach is kept for a later `start` retry; a refused TCP connection removes the local Link as before. Connection/header/HELLO reads time out after 5000 ms; the human Y/N decision deliberately has no read deadline. All routers in a run should use this rebuilt PA3 version because it adds an LSAUPDATE confirmation to the HELLO socket.

To try connect, start two routers, run `start` on the initiator, then enter `connect` with the other router's displayed identifiers and a weight. Answer `Y` on the receiver. The initiator prints `Link established with ...` and `Broadcasting LSAUPDATE`. Check `neighbors`, `lsd`, `detect` and `send` on both sides; also connect two existing chains and test paths/messages between their far endpoints.

## PA3 update weight

- `update 1 12` changes the link in this router's slot 1 to cost 12. The peer finds its own Link using the sender's simulated IP; slot numbers can differ between the two endpoints. Ports outside 0-3, unused ports, negative weights and invalid numeric arguments are rejected. Weight 0 and weights up to 2147483647 are accepted. An unchanged, confirmed weight is a no-op.
- A WEIGHT_UPDATE request/reply (packet type 4) explicitly synchronizes both Link objects. A normal LSAUPDATE alone only replaces remote LSAs and cannot perform this local adjacency change. The receiver applies an accepted weight to its Link and existing self-originated LinkDescription, sends confirmation, then floods its revised LSA. The initiator applies the confirmed result and broadcasts its database. Each actual change to an advertised local edge increments that endpoint's own LSA sequence once. Equal-weight versions do not increase it. Incoming LSAs never change the sequence number of the local router's LSA.
- Each link orders weight changes by `(counter, origin simulated IP)`; the greater tuple wins. The counter is a logical clock, not an LSA sequence number or wall-clock timestamp. Duplicate requests return the current accepted value without changing LSA sequence numbers. Old requests cannot overwrite newer weights. Conflicting values for the same version are rejected. Concurrent updates at opposite endpoints reconcile to the same winning tuple, including when an older confirmation arrives after an incoming newer update.
- Outgoing update commands run in a single daemon worker in terminal order. The terminal remains available for Y/N and other commands. A per-link lock serializes outgoing operations, but incoming handlers never acquire it. `portLock` protects short local state/LSA changes and is released before network I/O.
- Each exchange uses the existing 5000 ms connection/read deadlines. Up to three exchanges are attempted; a lost confirmation retries the same version, so the receiver does not apply it twice. If confirmation is still unavailable, print `Weight update unconfirmed` and retain the pending request. A timeout does not prove that the peer did not change: do not blindly roll back. Repeating the same update retries its version; requesting a different weight creates a newer version, even if that value equals the old local weight. Only a confirmed outcome prints `Link weight updated` and `Broadcasting LSAUPDATE`, without trailing periods. A concurrent winning value different from the requested value is reported explicitly.
- Updating an attached link before HELLO is allowed: both Link objects change, but an unestablished adjacency is not inserted into the LSA early. The later HELLO advertises the agreed cost. Weight changes do not trigger failure detection or remove a neighbor. All routers must run the rebuilt version because SOSPFPacket now includes weight version and request identity fields.

For a routing check, create a chain plus a more expensive alternate route. Increase the weight of an edge on the original shortest path with `update`, wait for `Link weight updated`, then run `detect` and `send` at a different router. Verify the alternate path is selected, and decrease the edge weight again to restore the original path. Test updates from either end, different local port indices, duplicate/zero/large values, simultaneous updates and lost-confirmation retries.

## PA3 disconnect and adjacency-loss notification

- `disconnect 1` removes the Link currently occupying local slot 1. Ports outside 0-3 and unused ports are rejected. Removal sets the slot to null, clears pending weight state and removes the corresponding LinkDescription from the router's own LSA under the same port lock. Removing an advertised edge increments that router's own LSA sequence once. The self-link is retained. An attached but unadvertised edge is also removable; it does not create an LSA entry or spuriously change its sequence.
- The initiating terminal prints `Port 1 disconnected` and `Broadcasting LSAUPDATE`, without trailing periods. Its listener and other links remain operational. Flooding to remaining TWO_WAY neighbors and notification of the removed peer run independently in background tasks; no port lock is held during network I/O.
- **Failure-detection mechanism for disconnect:** an explicit LINK_DOWN request/reply (packet type 5) is sent using the saved peer process address after local removal. The receiver resolves its local Link from the sender's simulated IP, verifies the adjacency session and removes its own Link/LinkDescription. It sends LINK_DOWN_ACK before flooding its revised LSA to remaining neighbors. A repeated notification is acknowledged without another deletion or LSA sequence increment. If both endpoints disconnect simultaneously, each removes its local state once and acknowledges the other's notice.
- Each attach request proposes a fresh UUID adjacency ID; its acceptance establishes the shared ID at both endpoints. HELLO, weight requests/confirmations and LINK_DOWN carry that ID. Removed IDs are retained as tombstones for the life of the router process. A delayed old disconnect or weight message cannot remove/change a reconnected Link, even when the same simulated IP, process port or local slot is reused. A delayed attach acceptance/HELLO cannot resurrect a retired adjacency.
- Notifications use the existing 5000 ms connection/read deadlines with up to three attempts. Lost confirmations retry the same adjacency ID. If still unconfirmed, local removal remains in effect and the saved notice remains available: repeat `disconnect` on its now-empty old slot to retry it, without incrementing the LSA sequence again. An immediate attach/connect to the same peer process first waits for/retries the saved cleanup notice (up to 20 seconds per pending notice), and fails explicitly if cleanup is unconfirmed. Reconnecting to a different process port does not wait on the old process. A late retry can only reference the old ID.
- Update commands capture their Link when queued and check that it is still installed before sending and after receiving confirmation. Thus a queued operation or delayed ACK cannot modify a newly connected neighbor in the same slot. Incoming weight requests from retired sessions are rejected. No weight timeout alone disconnects a link.
- Normal per-message TCP sockets close after each exchange; their closure is not interpreted as adjacency loss. This implementation uses explicit notifications for graceful disconnect and quit, not heartbeat detection of unannounced process crashes. All participating routers must use the rebuilt protocol version.
- After convergence, `detect` and `send` use the reduced topology automatically. If another route exists they use it; otherwise they print `Path not found`. Both freed ports can be used by later attach/connect commands.

For a demo, build a five-router chain with an alternate edge, disconnect a chain edge and check that both endpoints free their slots and advertise removal. Check `detect`/`send` from a third router, then disconnect a bridge to partition the topology and check `Path not found`. Reconnect with a different weight, replay an old notification, and verify that the new adjacency survives. Also test port 0, invalid/empty slots, repeated/simultaneous disconnect, update on a removed port, and disconnect during an in-flight weight update.

## PA3 graceful quit and adjacency-loss notification

- `quit` first sets a shutdown flag under `portLock`. It saves the current links, outstanding disconnect notices and outgoing attach requests awaiting approval. It clears all local slots/reservations, retires the saved adjacency IDs, cancels pending weight state and rejects any unanswered incoming attach. A single `withdrawOwnLinks()` operation removes all neighbor LinkDescriptions, retains the self-link and increments the local LSA sequence once, including for an isolated or not-yet-started router. The final LSA is a deep copy and stays unchanged across retries.
- The router prints `Broadcasting final LSAUPDATE`. Since its ports are now empty, it sends directly to the saved peer process addresses rather than using the normal port-based broadcaster. Attached neighbors without a completed HELLO are also notified. Peers are processed independently in parallel. For each saved adjacency, the final LSA exchange precedes its LINK_DOWN exchange.
- **Final LSA delivery:** a normal LSAUPDATE (packet type 1) with message `FINAL_LSAUPDATE` carries only the quitting router's revised self-originated LSA and the adjacency ID. The receiver uses the existing sequence comparison: install only an unknown/newer LSA and flood only accepted updates to TWO_WAY neighbors except the immediate sender. It sends `FINAL_LSAUPDATE_ACK` on the same socket after merging, before flooding. A repeated final packet is acknowledged without reinstalling/reflooding an equal/older LSA. The ACK echoes the adjacency ID and final LSA identity/sequence; it confirms processing at that receiver, not global convergence.
- **Failure-detection mechanism for quit:** after the final LSA exchange, reuse the explicit LINK_DOWN request/reply (type 5) for each saved adjacency. This is separate from LSA delivery. Each receiving neighbor removes its own matching Link and LinkDescription, frees its slot, increments its own LSA sequence once if an advertised edge was removed, and floods its revised LSA to remaining neighbors. An unanswered incoming attach for the retired ID is cancelled too. Duplicate or overlapping disconnect/quit notifications are acknowledged without another removal or sequence increment; retired session IDs protect replacement links.
- Both exchanges use the existing 5000 ms connect/read deadlines and at most three attempts each. A lost final ACK retransmits the identical LSA/sequence; a lost LINK_DOWN_ACK retransmits the same adjacency ID. Even if final delivery remains unconfirmed, LINK_DOWN is still attempted. Failed exchanges are reported explicitly. All per-neighbor jobs are awaited with a shared 100-second upper bound; only then is the listener closed, the notification executor stopped, `Router shutting down.` printed and the process terminated. Ordinary successful shutdown completes as soon as those local deliveries are confirmed; it does not wait for whole-network convergence. EOF uses the same shutdown path. `quit` is accepted even at a pending Y/N prompt.
- The shutdown flag is checked at reservation, commit, HELLO/LSA insertion and weight-application points, not only at command entry. Late acceptances or weight/HELLO confirmations cannot recreate a link or modify the final self-LSA. Queued weight work is cancelled. An overlapping quit returns without closing the first shutdown's sockets or incrementing its sequence again. A disconnect racing with quit cannot enqueue work into a stopped executor unchecked.
- Explicit notifications cover graceful commands. They do not guarantee cleanup after arbitrary process crashes or persistent network failure. An unconfirmed notification is reported before exit, and no heartbeat is claimed. Final LSA/ACK packets count as LSAUPDATE messages; cleanup packets/ACKs count as LINK_DOWN. Every participant must run the rebuilt version to understand the final-update confirmation.

For a demo, build a five-router chain with an alternate edge, quit an intermediate router and check `detect`/`send` on survivors: the quit router is unreachable and traffic between remaining routers uses the alternate route. Inspect `lsd` to verify the final withdrawn LSA and both direct neighbors' own revised LSAs; reuse their freed slots with attach/connect. Also test isolated/unstarted quit, attached-only neighbors, simultaneous quits, a pending Y/N request, overlapping disconnect or weight update, and dropped confirmations. Expected normal quit output is:

```text
Broadcasting final LSAUPDATE
Router shutting down.
```

## Example validation

Build the chain R1-R2-R3-R4-R5, with weights 2, 3, 1 and 2, using the displayed process endpoints. Run `start` on the routers needed to establish each adjacency, wait for synchronization, then run on R1:

```text
detect 192.168.4.1
send 192.168.4.1 Hi Router 5
```

The path should be:

```text
Path found: 192.168.1.1 -> (2) 192.168.1.100 -> (3) 192.168.2.1 -> (1) 192.168.3.1 -> (2) 192.168.4.1
```

For example, R2 forwards without displaying the message payload:

```text
Forwarding message from 192.168.1.1 to 192.168.4.1
Next hop: 192.168.2.1
```

R5 displays:

```text
Received message from 192.168.1.1;
Message: Hi Router 5
```

An unreachable destination prints `Path not found`. For a fresh run with exactly one application message along this chain, R1 has one APP_MESSAGE sent; R2, R3 and R4 each have one received and one sent; R5 has one received. HELLO and LSAUPDATE totals depend on the actual exchange sequence.

Repeat `start`, `detect`, `send`, and `stats` without restarting the processes. Also test a costly direct edge, a cycle, and a router joining an established topology. Repeating `start` must not duplicate an unchanged local link advertisement.

## Implementation notes

- `Router.java` implements command handling, HELLO, LSAUPDATE propagation, hop-by-hop delivery and counter updates.
- `LinkStateDatabase.java` stores the latest known LSA per simulated router and computes weighted paths using Dijkstra with `long` distances.
- `MessageCount.java` protects counters with a shared initialized lock. A broadcast counts once per neighbor. Intermediate application forwarding counts both receipt and transmission.
- `Path.java` stores the router sequence and edge weights and exposes the next hop.
- PA3 weight synchronization is implemented by `processUpdate()`, `exchangeWeightUpdate()`, `handleWeightUpdate()` and `applyWeightUpdate()` in Router, and `updateLinkWeight()` in LinkStateDatabase. Link stores per-adjacency version/pending state; SOSPFPacket carries the explicit request/reply fields.
- Only unknown or newer remote LSAs are installed and forwarded. Old/equal sequence numbers and incoming copies of the local router's own LSA are ignored.
- LSDB snapshots and accepted/forwarded updates are deep copies. Database access, link slots and statistics have separate synchronization.
- Network connections and reads use a 5000 ms timeout before object-stream input is constructed. An outgoing attach deliberately waits without a read deadline while the remote user decides Y/N.
- Edge-case comments are marked `EDGE CASE` in the source. Link slot zero and zero-cost edges are valid. Negative costs are rejected. Unknown paths, local delivery, repeated handshakes and concurrent database access are handled explicitly.

## Design walkthrough

1. **`start` and LSDB broadcast.** When a router runs `start`, `processStart()` performs the three-way HELLO handshake with every attached neighbor, which moves each of them to TWO_WAY. As required by PA2, the router then broadcasts its link-state database (LSDB) to all of its TWO_WAY neighbors.

2. **Responder side of the handshake.** The receiving router handles the handshake in a connection-handler thread: it receives a HELLO, replies with a HELLO, waits for the final HELLO, and then also marks the neighbor as TWO_WAY. It adds a new link to its own LSA with `addNeighborToLSA()` and sends a full LSDB in an LSAUPDATE on the handshake socket to confirm completion. If the link was newly added, it also broadcasts the updated database. The initiator waits for this confirmation, installs its own LSA entry and merges the received LSDB before its final broadcast. The full database is exchanged on repeated handshakes too.

3. **Receiving an LSAUPDATE.** Every message that carries LSAs has `sospfType = TYPE_LSU`. When the connection handler receives a packet of this type, it passes it to `receiveLSAUpdate(packet)`. The router merges the LSAs into its LSDB: an LSA is accepted only if its router is unknown or its sequence number is greater than that of the stored copy, and all other LSAs are discarded. If nothing was accepted, propagation stops here. Otherwise, the router forwards the accepted LSAs to all TWO_WAY neighbors except the one it received them from.

4. **Sending an application message.** `processSend()` sends a `TYPE_APP_MESSAGE` packet toward the given destination IP. It calls `nextHopLink(destIP)`, which runs Dijkstra's algorithm on the topology stored in the local LSDB to find the lowest-cost path to the destination. The router then sends the packet to the first neighbor on that path.

5. **Receiving an application message.** When the connection handler receives a `TYPE_APP_MESSAGE` packet, it calls `handleApplicationMessage()`. If the destination IP is this router, the message is delivered and its payload is printed. Otherwise, the router calls `nextHopLink(destIP)` to compute the best path from its own LSDB and forwards the packet it received to the next neighbor on that path, leaving the original source IP and payload unchanged.

6. **Counting received and handshake messages.** We added `stats.countReceived(packet.sospfType)` to the `connectionHandler` method from PA1, so the first packet of every incoming connection is counted by type. Any further message exchanged on the same connection must be counted inside the method that handles it:
   - In `attachLink()` (shared by attach and connect), the requester counts one ATTACH sent when it sends the request and one ATTACH received when the reply arrives. The reply has to be counted here because the Y/N answer comes back on the same connection and never passes through the connection handler.
   - `processStart()` calls `helloHandshake()` for each neighbor, which counts every HELLO it sends and receives. On the responder side, the first HELLO is counted by the connection handler, and the HELLOs sent and received after it are counted inside the handler method.
   - `handleHello()` counts the LSAUPDATE confirmation it sends on the handshake socket; `helloHandshake()` counts its receipt.

7. **Counting in `sendOneWayMessage()`.** `sendOneWayMessage()` counts every message it sends, by type. Application messages that are sent or forwarded therefore do not need to be counted again in `processSend()` or `handleApplicationMessage()`, and the same holds for every LSAUPDATE that is broadcast or forwarded.

8. **The `Path` class.** To record the path produced by Dijkstra's algorithm, we created a `Path` class. It keeps two lists: the simulated IPs of the routers along the path, and the cost of each link between consecutive routers. The router uses this information to find the next hop (`nextHopLink`) and to print the path for `detect`.

9. **`MessageCount` and the `lsd` command.** The `MessageCount` class counts how many messages of each type the router has sent and received. Typing `stats` calls the router's `MessageCount.reportMessageCount()` and prints these counts. Typing `lsd` runs `System.out.println(lsd)`; because `LinkStateDatabase` overrides `toString()`, this prints the records stored in the LSDB.

10. **Sequence numbers.** Whenever a neighbor becomes TWO_WAY and the link is added to the router's own LSA with `addLinkToLSA()`, the router increments the sequence number of its own LSA. The sequence numbers of other routers' LSAs are never incremented locally. They are only compared in `merge()`: if the stored copy has a smaller sequence number, it is replaced by the incoming LSA.



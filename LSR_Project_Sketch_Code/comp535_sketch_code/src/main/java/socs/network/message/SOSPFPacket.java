package socs.network.message;

import java.io.*;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

public class SOSPFPacket implements Serializable {

  //for inter-process communication
  public String srcProcessIP;
  public short srcProcessPort;

  //simulated IP address
  public String srcIP;
  public String dstIP;

  //common header
  // 0 HELLO, 1 LSAUPDATE, 2 APP_MESSAGE, 3 ATTACH, 4 LINK_DOWN, 5 ROUTER_DOWN, 6 WEIGHT_UPDATE.
  public short sospfType;
  public String routerID;

  //used by HELLO message to identify the sender of the message
  //e.g. when router A sends HELLO to its neighbor, it has to fill this field with its own
  //simulated IP address
  public String neighborID; //neighbor's simulated IP address

  //used by LSAUPDATE
  public Vector<LSA> lsaArray = null;

  //used by Application Message
  public String message; //user inputted message payload
  
  // used to send the weight for one link (这个是双向的) new field!
  public int weight;

  // Direct-neighbor WEIGHT_UPDATE request/reply (type 6).
  // The origin breaks ties between simultaneous updates with the same version counter.
  public long weightVersion;
  public String weightOrigin;
  public String weightRequestId;

  // Identifies one attach session for HELLO, weight updates, LINK_DOWN (4) and ROUTER_DOWN (5).
  public String linkId;

}

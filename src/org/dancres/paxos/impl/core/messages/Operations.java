package org.dancres.paxos.impl.core.messages;

public interface Operations {
    public static final int HEARTBEAT = 0;
    public static final int HEARTBEAT_ECHO = 1;
    public static final int POST = 2;
    public static final int COLLECT = 3;
    public static final int LAST = 4;
    public static final int BEGIN = 5;
    public static final int ACCEPT = 6;
    public static final int SUCCESS = 7;
    public static final int ACK = 8;
    public static final int OLDROUND = 9;
    public static final int PROPOSER_REQ = 10;
}

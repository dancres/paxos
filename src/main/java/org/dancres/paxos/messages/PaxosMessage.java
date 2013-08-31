package org.dancres.paxos.messages;

import java.util.EnumSet;

public interface PaxosMessage {
    public enum Classification {
        ACCEPTOR_LEARNER, CLIENT, LEADER, FAILURE_DETECTOR, RECOVERY
    };

    public int getType();
    public long getSeqNum();
    public EnumSet<Classification> getClassifications();
}

package org.dancres.paxos.messages;

import java.util.EnumSet;

public interface PaxosMessage {
    enum Classification {
        ACCEPTOR_LEARNER, CLIENT, LEADER, FAILURE_DETECTOR, RECOVERY
    }

    int getType();
    long getSeqNum();
    EnumSet<Classification> getClassifications();

    interface Types {
        int HEARTBEAT = 0;
        int OUTOFDATE = 1;
        int ENVELOPE = 2;
        int COLLECT = 3;
        int LAST = 4;
        int BEGIN = 5;
        int ACCEPT = 6;
        int LEARNED = 7;
        int OLDROUND = 9;
        int NEED = 10;
        int EVENT = 11;
    }
}

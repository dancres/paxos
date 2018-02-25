package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Constants;

import java.util.EnumSet;

/**
 * Returned by an AL in response to a NEED indicating that in its opinion the NEED'y is too far behind to recover
 * via log records and should use a checkpoint to bring itself back into sync.
 */
public class OutOfDate implements PaxosMessage {
    public OutOfDate() {
    }

    public EnumSet<Classification> getClassifications() {
        return EnumSet.of(Classification.RECOVERY);
    }

    public long getSeqNum() {
        // No meaningful seqnum
        //
        return Constants.RECOVERY_SEQ;
    }

    public int getType() {
        return Types.OUTOFDATE;
    }

    public String toString() {
        return "OutOfDate";
    }
}

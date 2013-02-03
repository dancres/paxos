package org.dancres.paxos.messages;

import org.dancres.paxos.VoteOutcome;

public class Event implements PaxosMessage {
    private final VoteOutcome _vote;

    public Event(VoteOutcome anOutcome) {
        _vote = anOutcome;
    }

    public VoteOutcome getOutcome() {
        return _vote;
    }

    public int getType() {
        return Operations.EVENT;
    }

    public long getSeqNum() {
        return _vote.getSeqNum();
    }

    public short getClassification() {
        return CLIENT;
    }
}

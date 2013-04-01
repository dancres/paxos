package org.dancres.paxos;

public interface Completion {
    public void complete(VoteOutcome anOutcome);
}

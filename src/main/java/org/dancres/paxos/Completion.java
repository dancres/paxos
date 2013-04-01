package org.dancres.paxos;

public interface Completion {
    /**
     * FOR COMPLETION STEPS ONLY, DO NOT ATTEMPT TO INITIATE FURTHER ROUNDS FROM WITHIN THIS CALL AS THEY WILL
     * LIKELY DEADLOCK
     *
     * @todo Remove the deadlocks (see method comment)
     *
     * @param anOutcome
     */
    public void complete(VoteOutcome anOutcome);
}

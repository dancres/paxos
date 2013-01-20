package org.dancres.paxos.impl;

public interface Instance {
    /**
     * Instance reaches COLLECT after SUBMITTED unless we're applying multi-paxos when it will reach BEGIN instead.
     *
     * In BEGIN we attempt to reserve a slot in the sequence of operations. Transition to LEARNED after emitting begin
     * to see if the slot was granted.
     *
     * In LEARNED, Instance has sent a BEGIN and now determines if it has secured the slot associated with the sequence
     * number. If the slot was secured, a value will be sent to all members of the current instance after which there
     * will be a transition to COMMITTED.
     *
     * In EXIT a paxos instance was completed successfully, clean up is all that remains.
     *
     * In ABORT a paxos instance failed for some reason (which will be found in </code>_outcome</code>).
     *
     * In SUBMITTED, Instance has been given a value and should attempt to complete a paxos instance.
     *
     * In SHUTDOWN, we do a little cleanup and halt, processing no messages etc.
     */
    public enum State {
        INITIAL, SUBMITTED, COLLECT, BEGIN, SUCCESS, EXIT, ABORT, SHUTDOWN
    }

    public State getState();

    public long getRound();

    public long getSeqNum();
}

package org.dancres.paxos.impl;

import org.dancres.paxos.VoteOutcome;

import java.util.Deque;

/**
 * Provides access to pertinent information relating to a single instance of Paxos.
 */
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
     * In ABORT a paxos instance failed for some reason (which will be found in <code>_outcome</code>).
     *
     * In SUBMITTED, Instance has been given a value and should attempt to complete a paxos instance.
     *
     * In SHUTDOWN, we do a little cleanup and halt, processing no messages etc.
     */
    enum State {
        INITIAL, SUBMITTED, COLLECT, BEGIN, SUCCESS, EXIT, ABORT, SHUTDOWN
    }

    State getState();

    long getRound();

    long getSeqNum();

    /**
     * There is usually a single outcome, good or bad. However, there may be as yet uncompleted ballots and in such
     * a case the leader will be expected to drive those to completion. When this happens, the value submitted in
     * the current request may be superseded by another "hanging" from the uncompleted ballots. The fact that this
     * has occurred is disclosed in the form of two outcomes. The first will be a report of an OTHER_VALUE, the second
     * will be a report of VALUE. The former will be the value in the current request, the latter will be the
     * "hanging" value from a previous ballot.
     *
     * @return the set of outcomes that occurred at this ballot
     */
    Deque<VoteOutcome> getOutcomes();
}

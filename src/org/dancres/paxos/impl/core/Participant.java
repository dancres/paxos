package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the acceptor/learner role for a particular instance of paxos.
 *
 * @author dan
 */
class Participant {
    private Logger _logger = LoggerFactory.getLogger(Participant.class);
    
    private long _seqNum;
    private long _lastRound = 0;
    private long _lastNodeId = Long.MIN_VALUE;
    private byte[] _value = null;

    private AcceptorLearnerState _state;

    Participant(long aSeqNum, AcceptorLearnerState aState) {
        _seqNum = aSeqNum;
        _state = aState;
    }

    /**
     * @todo When we process success and send ACK we cannot throw the participant away immediately
     * because other failures may cause the round to abort dictating a retry by the leader.
     * We must wait a settle time before clearing out.  We should be able to junk it when we see rounds for the next
     * entry (or one several entry's further on - possibly current seqnum plus total number of possible leaders + 1).
     * Note of course we're supposed to keep a log on persistent storage for our state, so we can junk the participant
     * and restore on iniital completion.
     *
     * @todo if we receive a BEGIN or COLLECT that invalidates our old round it would make sense to see if the nodeId is
     * superior to ours.  If that is the case, another leader is active and we should abort our leader for the proposal
     * if we have one running.  An alternative is to send in the OLDROUND message the node id so the proposer can
     * decide for itself to cease chatter and inform it's client of a new leader.
     *
     * @todo When we send Ack in response to Success we can inform listeners of the new value.
     */
    PaxosMessage process(PaxosMessage aMessage) {
        switch (aMessage.getType()) {
            case Operations.COLLECT : {
                Collect myCollect = (Collect) aMessage;
                if (myCollect.supercedes(_lastRound, _lastNodeId)) {
                    long myMostRecentRound = _lastRound;

                    _lastRound = myCollect.getRndNumber();
                    _lastNodeId = myCollect.getNodeId();

                    return new Last(_seqNum, myMostRecentRound, _value);
                } else {
                    // Another collect has already arrived with a higher priority, tell the proposer it has competition
                    //
                    return new OldRound(_seqNum, _lastNodeId, _lastRound);
                }
            }
            case Operations.BEGIN : {
                Begin myBegin = (Begin) aMessage;

                // If the begin matches the last round of a collect we're fine
                //
                if (myBegin.originates(_lastRound, _lastNodeId)) {
                    _value = myBegin.getValue();

                    return new Accept(_seqNum, _lastRound);
                } else if (myBegin.precedes(_lastRound, _lastNodeId)) {

                    // A new collect was received since the collect for this begin, tell the proposer it's got competition
                    //
                    return new OldRound(_seqNum, _lastNodeId, _lastRound);
                } else {
                    // Be slient - we didn't see the collect, value hasn't take account of us
                    //
                    _logger.info("Missed collect, going silent: " + _seqNum + " [ " + myBegin.getRndNumber() + " ]");
                }
            }
            case Operations.SUCCESS : {
                Success mySuccess = (Success) aMessage;

                _logger.info("Learnt value: " + mySuccess.getSeqNum());

                return new Ack(mySuccess.getSeqNum());
            }
            default : throw new RuntimeException("Unexpected message");
        }
    }
}

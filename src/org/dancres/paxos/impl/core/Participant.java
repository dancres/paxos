package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                    return new OldRound(_seqNum, _lastRound);
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

                    // A new round has arrived and invalidated the collect we're associated with
                    //
                    return new OldRound(_seqNum, _lastRound);
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

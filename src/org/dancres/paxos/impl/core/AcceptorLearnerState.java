package org.dancres.paxos.impl.core;

import java.util.Map;
import java.util.HashMap;
import org.dancres.paxos.impl.core.messages.Begin;
import org.dancres.paxos.impl.core.messages.Collect;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @todo Collapse participants out - recover seqNum/value pairs from the store.  Modify Last to return low water mark and max seen sequence number.
 *
 * @author dan
 */
class AcceptorLearnerState {
    private Logger _logger = LoggerFactory.getLogger(AcceptorLearnerState.class);

    private Map<Long, Participant> _participants = new HashMap<Long, Participant>();
    private Collect _lastCollect = Collect.INITIAL;

    /**
     * When we receive a success, if it's seqNum is this field + 1, increment this field.  Acts as the low watermark for leader recovery, essentially
     * we want to recover from the last contiguous sequence number in the stream of paxos instances.
     */
    private long _lowSeqNumWatermark = -1;

    /**
     * Records the most recent seqNum we've seen in a BEGIN or SUCCESS message.  We may see a SUCCESS without BEGIN but that's okay
     * as the leader must have had sufficient majority to get agreement so we can just agree, update this count and update the
     * value/seqNum store.
     */
    private long _highSeqNumWatermark = -1;

    private Participant newParticipant(long aSeqNum) {
        synchronized(_participants) {
            Participant myPart = getParticipant(aSeqNum);

            if (myPart == null) {
                myPart = new Participant(aSeqNum, this);
                _participants.put(new Long(aSeqNum), myPart);
            }

            return myPart;
        }
    }

    /**
     * @todo Decide what to do if we don't find a participant
     */
    private Participant getParticipant(long aSeqNum) {
        synchronized(_participants) {
            Participant myPart = _participants.get(new Long(aSeqNum));

            return myPart;
        }
    }

    void updateLowWatermark(long aSeqNum) {
        synchronized(this) {
            if ((_lowSeqNumWatermark + 1) == aSeqNum) {
                _lowSeqNumWatermark = aSeqNum;

                _logger.info("Low watermark:" + aSeqNum);
            }

        }
    }

    long getLowWatermark() {
        synchronized(this) {
            return _lowSeqNumWatermark;
        }
    }

    void updateHighWatermark(long aSeqNum) {
        synchronized(this) {
            if (_highSeqNumWatermark < aSeqNum) {
                _highSeqNumWatermark = aSeqNum;

                _logger.info("High watermark:" + aSeqNum);
            }
        }
    }

    long getHighWatermark() {
        synchronized(this) {
            return _highSeqNumWatermark;
        }
    }

    /**
     * @param aCollect should be tested to see if it supercedes the current COLLECT
     * @return the old collect if it's superceded or null
     */
    Collect supercedes(Collect aCollect) {
        synchronized(this) {
            if (aCollect.supercedes(_lastCollect)) {
                Collect myOld = _lastCollect;
                _lastCollect = aCollect;

                return myOld;
            } else {
                return null;
            }
        }
    }

    Collect getLastCollect() {
        synchronized(this) {
            return _lastCollect;
        }
    }

    boolean originates(Begin aBegin) {
        synchronized(this) {
            return aBegin.originates(_lastCollect);
        }
    }

    boolean precedes(Begin aBegin) {
        synchronized(this) {
            return aBegin.precedes(_lastCollect);
        }
    }

    public PaxosMessage process(PaxosMessage aMessage) {
        switch (aMessage.getType()) {
            case Operations.COLLECT : {
                Participant myPart = newParticipant(aMessage.getSeqNum());
                return myPart.process(aMessage);
            }
            case Operations.BEGIN : {
                Participant myPart = newParticipant(aMessage.getSeqNum());
                assert(myPart != null);

                return myPart.process(aMessage);
            }

            case Operations.SUCCESS : {
                Participant myPart = getParticipant(aMessage.getSeqNum());
                assert(myPart != null);

                return myPart.process(aMessage);
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }
}

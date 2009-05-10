package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.Accept;
import org.dancres.paxos.impl.core.messages.Ack;
import org.dancres.paxos.impl.core.messages.Begin;
import org.dancres.paxos.impl.core.messages.Collect;
import org.dancres.paxos.impl.core.messages.Last;
import org.dancres.paxos.impl.core.messages.OldRound;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Success;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dan
 */
public class AcceptorLearnerState {
    private static Logger _logger = LoggerFactory.getLogger(AcceptorLearnerState.class);

    private Collect _lastCollect = Collect.INITIAL;
    private LogStorage _storage;

    /**
     * When we receive a success, if it's seqNum is this field + 1, increment this field.  Acts as the low watermark for leader recovery, essentially
     * we want to recover from the last contiguous sequence number in the stream of paxos instances.
     */
    private long _lowSeqNumWatermark = LogStorage.EMPTY_LOG;

    /**
     * Records the most recent seqNum we've seen in a BEGIN or SUCCESS message.  We may see a SUCCESS without BEGIN but that's okay
     * as the leader must have had sufficient majority to get agreement so we can just agree, update this count and update the
     * value/seqNum store.
     */
    private long _highSeqNumWatermark = LogStorage.EMPTY_LOG;

    public AcceptorLearnerState(LogStorage aStore) {
        _storage = aStore;
    }

    private LogStorage getStorage() {
        return _storage;
    }

    private void updateLowWatermark(long aSeqNum) {
        synchronized(this) {
            if ((_lowSeqNumWatermark + 1) == aSeqNum) {
                _lowSeqNumWatermark = aSeqNum;

                _logger.info("Low watermark:" + aSeqNum);
            }

        }
    }

    private long getLowWatermark() {
        synchronized(this) {
            return _lowSeqNumWatermark;
        }
    }

    private void updateHighWatermark(long aSeqNum) {
        synchronized(this) {
            if (_highSeqNumWatermark < aSeqNum) {
                _highSeqNumWatermark = aSeqNum;

                _logger.info("High watermark:" + aSeqNum);
            }
        }
    }

    private long getHighWatermark() {
        synchronized(this) {
            return _highSeqNumWatermark;
        }
    }

    /**
     * @param aCollect should be tested to see if it supercedes the current COLLECT
     * @return the old collect if it's superceded or null
     */
    private Collect supercedes(Collect aCollect) {
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

    private Collect getLastCollect() {
        synchronized(this) {
            return _lastCollect;
        }
    }

    private boolean originates(Begin aBegin) {
        synchronized(this) {
            return aBegin.originates(_lastCollect);
        }
    }

    private boolean precedes(Begin aBegin) {
        synchronized(this) {
            return aBegin.precedes(_lastCollect);
        }
    }

    public PaxosMessage process(PaxosMessage aMessage) {
        long mySeqNum = aMessage.getSeqNum();

        _logger.info("AcceptorLearnerState got [ " + mySeqNum + " ] : " + aMessage);

        switch (aMessage.getType()) {
            case Operations.COLLECT : {
                Collect myCollect = (Collect) aMessage;
                Collect myOld = supercedes(myCollect);

                if (myOld != null) {
                    return new Last(mySeqNum, getLowWatermark(), getHighWatermark(), myOld.getRndNumber(),
                            getStorage().get(mySeqNum));
                } else {
                    // Another collect has already arrived with a higher priority, tell the proposer it has competition
                    //
                    Collect myLastCollect = getLastCollect();
                    return new OldRound(mySeqNum, myLastCollect.getNodeId(), myLastCollect.getRndNumber());
                }
            }
            case Operations.BEGIN : {
                Begin myBegin = (Begin) aMessage;

                // If the begin matches the last round of a collect we're fine
                //
                if (originates(myBegin)) {
                    getStorage().put(mySeqNum, myBegin.getValue());
                    updateHighWatermark(myBegin.getSeqNum());
                    return new Accept(mySeqNum, getLastCollect().getRndNumber());
                } else if (precedes(myBegin)) {
                    // A new collect was received since the collect for this begin, tell the proposer it's got competition
                    //
                    Collect myLastCollect = getLastCollect();
                    return new OldRound(mySeqNum, myLastCollect.getNodeId(), myLastCollect.getRndNumber());
                } else {
                    // Be slient - we didn't see the collect, leader hasn't taken account of our value because it hasn't seen our last
                    //
                    _logger.info("Missed collect, going silent: " + mySeqNum + " [ " + myBegin.getRndNumber() + " ]");
                }
            }
            case Operations.SUCCESS : {
                Success mySuccess = (Success) aMessage;

                _logger.info("Learnt value: " + mySuccess.getSeqNum());

                getStorage().put(mySeqNum, mySuccess.getValue());
                updateLowWatermark(mySuccess.getSeqNum());
                updateHighWatermark(mySuccess.getSeqNum());
                return new Ack(mySuccess.getSeqNum());
            }

            default : throw new RuntimeException("Unexpected message");
        }
    }
}

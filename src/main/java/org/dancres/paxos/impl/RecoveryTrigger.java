package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Need;

import java.net.InetSocketAddress;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * We track each completed proposal expecting them to be in approximately linear order.
 * Gaps can appear because we miss some packets in which case we ultimately need to close them.
 * If we're slightly out of order then that proposal will be settled shortly and we can continue
 * processing however, if the gap remains and the proposal sequence number moves too far from the gap
 * it is likely we need to recover. In essence the maximum number of concurrent requests forms the
 * approx maximum distance from the low watermark that can be tolerated with gaps.
 *
 * Gap is measured between low watermark and the lowest above that mark completed recently.
 */
class RecoveryTrigger {
    private static final long MAX_INFLIGHT = 1;

    /**
     * Low watermark tracks the last successfully committed paxos instance (seqNum and position in log).
     * This is used as a means to filter out repeats of earlier instances and helps identify times where recovery is
     * needed. E.g. Our low watermark is 3 but the next instance we see is 5 indicating that instance 4 has likely
     * occurred without us present and we should take recovery action.
     */
    private AcceptorLearner.Watermark _lowSeqNumWatermark = AcceptorLearner.Watermark.INITIAL;

    private final SortedSet<AcceptorLearner.Watermark> _completed = new TreeSet<AcceptorLearner.Watermark>();

    AcceptorLearner.Watermark getLowWatermark() {
        synchronized(this) {
            return _lowSeqNumWatermark;
        }
    }

    void completed(AcceptorLearner.Watermark aCurrent) {
        synchronized(this) {
            _completed.add(aCurrent);

            // See if we can clear some of the completed now
            //
            while (_completed.size() > 0) {
                if (_completed.first().getSeqNum() == _lowSeqNumWatermark.getSeqNum() + 1) {
                    _lowSeqNumWatermark = _completed.first();
                    _completed.remove(_lowSeqNumWatermark);
                } else
                    break;
            }

            // _logger.debug("AL:Low :" + _lowSeqNumWatermark + ", " + _localAddress);
        }
    }

    Need shouldRecover(long aProposalSeqNum, InetSocketAddress aLocalAddress) {
        synchronized(this) {
            // If we haven't got any completions, we may have a huge gap beyond MAX_INFLIGHT in size - catch this
            //
            if (_completed.size() == 0) {
                if (aProposalSeqNum > _lowSeqNumWatermark.getSeqNum() + MAX_INFLIGHT)
                    return new Need(_lowSeqNumWatermark.getSeqNum(), aProposalSeqNum - 1);
            } else if (_lowSeqNumWatermark.getSeqNum() + MAX_INFLIGHT < _completed.last().getSeqNum()) {
                return new Need(_lowSeqNumWatermark.getSeqNum(), _completed.first().getSeqNum() - 1);
            }

            return null;
        }
    }

    void reset() {
        synchronized(this) {
            _lowSeqNumWatermark = AcceptorLearner.Watermark.INITIAL;
            _completed.clear();
        }
    }

    long install(AcceptorLearner.Watermark aLow) {
        synchronized(this) {
            _lowSeqNumWatermark = aLow;

            return _lowSeqNumWatermark.getSeqNum();
        }
    }
}

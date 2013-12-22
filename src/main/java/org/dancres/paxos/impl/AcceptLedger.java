package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Learned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

class AcceptLedger {
    private static final Logger _logger = LoggerFactory.getLogger(AcceptLedger.class);

    private Set<Transport.Packet> _ledger = new HashSet<>();
    private final long _seqNum;

    AcceptLedger(long aSeqNum) {
        _seqNum = aSeqNum;
    }

    void add(Transport.Packet aPacket) {
        synchronized  (this) {
            if (! (aPacket.getMessage() instanceof Accept))
                throw new IllegalArgumentException("Packet is not an accept");

            if (aPacket.getMessage().getSeqNum() != _seqNum)
                throw new IllegalArgumentException("Packet doesn't have correct sequence number " + _seqNum + " vs" +
                    aPacket.getMessage().getSeqNum());

            _ledger.add(aPacket);
        }
    }

    /**
     * Remove any accepts in the ledger not appropriate for the passed begin. We must tally only those accepts
     * that match the round and sequence number of this begin. All others should be flushed.
     *
     * @param aBegin
     */
    void purge(Begin aBegin) {
        synchronized (this) {
            Iterator<Transport.Packet> myAccs = _ledger.iterator();

            while (myAccs.hasNext()) {
                Transport.Packet myAcc = myAccs.next();
                if (((Accept) myAcc.getMessage()).getRndNumber() != aBegin.getRndNumber())
                    myAccs.remove();
            }
        }
    }

    /**
     * Determines whether a sufficient number of accepts can be tallied against the specified begin.
     *
     * @param aBegin
     * @return A Learned to be logged if there are sufficient accepts, <code>null</code> otherwise.
     */
    Learned tally(Begin aBegin, int aMajority) {
        synchronized (this) {
            int myAcceptTally = 0;

            if (_ledger.size() < aMajority)
                return null;

            for (Transport.Packet myAcc : _ledger)
                if (((Accept) myAcc.getMessage()).getRndNumber() == aBegin.getRndNumber())
                    ++myAcceptTally;

            if (myAcceptTally >= aMajority) {
                _logger.debug("Accepted on set " + _ledger + " with majority " + aMajority);

                return new Learned(aBegin.getSeqNum(), aBegin.getRndNumber());
            } else
                return null;
        }
    }
}

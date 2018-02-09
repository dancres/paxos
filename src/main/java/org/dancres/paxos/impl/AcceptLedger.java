package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Learned;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

class AcceptLedger {
    private static final Logger _logger = LoggerFactory.getLogger(AcceptLedger.class);

    class Slip {
        final InetSocketAddress _addr;
        final Accept _acc;

        Slip(Transport.Packet aPacket) {
            _acc = (Accept) aPacket.getMessage();
            _addr = aPacket.getSource();
        }

        public boolean equals(Object anObject) {
            if (anObject instanceof Slip) {
                Slip mySlip = (Slip) anObject;
                return ((mySlip._addr.equals(_addr)) && (mySlip._acc.equals(_acc)));
            }

            return false;
        }
    }

    private final Set<Slip> _ledger = new HashSet<>();
    private final long _seqNum;
    private final String _alId;

    AcceptLedger(String anALId, long aSeqNum) {
        _seqNum = aSeqNum;
        _alId = anALId;
    }

    void add(Transport.Packet aPacket) {
        synchronized  (this) {
            if (! (aPacket.getMessage() instanceof Accept))
                throw new IllegalArgumentException("Packet is not an accept");

            if (aPacket.getMessage().getSeqNum() != _seqNum)
                throw new IllegalArgumentException("Packet doesn't have correct sequence number " + _seqNum + " vs" +
                    aPacket.getMessage().getSeqNum());

            _ledger.add(new Slip(aPacket));
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
            _ledger.removeIf((aSlip) ->
                    aSlip._acc.getRndNumber() != aBegin.getRndNumber());
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

            for (Slip mySlip : _ledger)
                if (mySlip._acc.getRndNumber() == aBegin.getRndNumber())
                    ++myAcceptTally;

            if (myAcceptTally >= aMajority) {
                _logger.debug(_alId + ": Accepted on set " + _ledger + " with majority " + aMajority);

                return new Learned(aBegin.getSeqNum(), aBegin.getRndNumber());
            } else
                return null;
        }
    }
}

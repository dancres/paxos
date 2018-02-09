package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Learned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class AcceptLedger {
    private final Map<Long, Ledger> _acceptLedgers = new ConcurrentHashMap<>();
    private static final Logger _logger = LoggerFactory.getLogger(AcceptLedger.class);
    private final String _name;

    AcceptLedger(String aName) {
        _name = aName;
    }

    void remove(long aSeqNum) {
        _acceptLedgers.remove(aSeqNum);
    }

    void clear() {
        _acceptLedgers.clear();
    }

    /**
     * Utility method to manage the lifecycle of creating an accept ledger.
     *
     * @param anAccept
     * @return the newly or previously created ledger for the specified sequence number.
     */
    void extendLedger(Transport.Packet anAccept) {
        long mySeqNum = anAccept.getMessage().getSeqNum();

        getAcceptLedger(mySeqNum, true).add(anAccept);
    }

    /**
     * Determines whether a sufficient number of accepts can be tallied against the specified begin.
     *
     * @param aBegin
     * @return A Learned packet to be logged if there are sufficient accepts, <code>null</code> otherwise.
     */
    Learned tallyAccepts(Begin aBegin, int aMajority) {
        Ledger myAccepts = getAcceptLedger(aBegin.getSeqNum(), false);

        if (myAccepts != null) {
            Learned myLearned = myAccepts.tally(aBegin, aMajority);

            if (myLearned != null) {
                _logger.trace(toString() + " *** Speculative COMMIT possible ***");

                return myLearned;
            }
        }

        return null;
    }


    /**
     * Remove any accepts in the ledger not appropriate for the passed begin. We must tally only those accepts
     * that match the round and sequence number of this begin. All others should be flushed.
     *
     * @param aBegin
     */
    void purgeAcceptLedger(Begin aBegin) {
        Ledger myAccepts = getAcceptLedger(aBegin.getSeqNum(), false);

        if (myAccepts != null)
            myAccepts.purge(aBegin);
    }

    private Ledger getAcceptLedger(Long aSeqNum, boolean doCreate) {
        Ledger myAccepts = _acceptLedgers.get(aSeqNum);

        if (myAccepts == null && doCreate) {
            Ledger myInitial = new Ledger(aSeqNum);
            Ledger myResult = _acceptLedgers.put(aSeqNum, myInitial);

            myAccepts = ((myResult == null) ? myInitial : myResult);
        }

        return myAccepts;
    }

    private class Ledger {
        private class Slip {
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

            public int hashCode() {
                return _acc.hashCode() ^ _addr.hashCode();
            }
        }

        private final Set<Slip> _ledger = new HashSet<>();
        private final long _seqNum;

        Ledger(long aSeqNum) {
            _seqNum = aSeqNum;
        }

        void add(Transport.Packet aPacket) {
            synchronized (this) {
                if (!(aPacket.getMessage() instanceof Accept))
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
                    _logger.debug(_name + ": Accepted on set " + _ledger + " with majority " + aMajority);

                    return new Learned(aBegin.getSeqNum(), aBegin.getRndNumber());
                } else
                    return null;
            }
        }
    }
}
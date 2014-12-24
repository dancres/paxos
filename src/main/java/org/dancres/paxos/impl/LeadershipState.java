package org.dancres.paxos.impl;

import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class LeadershipState {
    private static final Logger _logger = LoggerFactory.getLogger(LeadershipState.class);

    private static class FakePacket implements Transport.Packet {
        private final PaxosMessage _message;
        private InetSocketAddress _address;

        FakePacket(PaxosMessage aMessage) {
            _message = aMessage;

            try {
                _address = new InetSocketAddress(Utils.getWorkableInterfaceAddress(), 12345);
            } catch (Exception anE) {
                _logger.error("Problems getting a useful address for FakePacket", anE);
                throw new RuntimeException("No localhost address, doomed");
            }
        }

        public InetSocketAddress getSource() {
            return _address;
        }

        public PaxosMessage getMessage() {
            return _message;
        }

        public String toString() {
            return "PK [ " + _address + " ] " + _message;
        }

        public boolean equals(Object anObject) {
            if (anObject instanceof FakePacket) {
                FakePacket myPacket = (FakePacket) anObject;
                return ((myPacket._address.equals(_address)) && (myPacket._message.equals(_message)));
            }

            return false;
        }

        public int hashCode() {
            return _message.hashCode() ^ _address.hashCode();
        }
    }

    private final AtomicReference<Transport.Packet> _lastCollect =
            new AtomicReference<Transport.Packet>(new FakePacket(Collect.INITIAL));
    private AtomicLong _lastLeaderActionTime = new AtomicLong(0);
    private final LeaderUtils _leaderUtils = new LeaderUtils();

    void leaderAction() {
        _lastLeaderActionTime.set(System.currentTimeMillis());
    }

    void resetLeaderAction() {
        _lastLeaderActionTime.set(0);
    }

    void clearLeadership() {
        _lastCollect.set(new FakePacket(Collect.INITIAL));
        resetLeaderAction();
    }

    void setLastCollect(Transport.Packet aCollect) {
        if (! (aCollect.getMessage() instanceof Collect))
            throw new IllegalArgumentException();

        _lastCollect.set(aCollect);
    }

    Transport.Packet getLastCollect() {
        return _lastCollect.get();
    }

    long getLeaderRndNum() {
        return ((Collect) _lastCollect.get().getMessage()).getRndNumber();
    }

    InetSocketAddress getLeaderAddress() {
        return _lastCollect.get().getSource();
    }

    /**
     * @param aCollect
     *            should be tested to see if it supercedes the current COLLECT
     * @return <code>true</code> if it supercedes, <code>false</code> otherwise
     */
    boolean supercedes(Transport.Packet aCollect) {
        Transport.Packet myCurrentLast;

        do {
            myCurrentLast = _lastCollect.get();

            if (! _leaderUtils.supercedes(aCollect, myCurrentLast))
                return false;

        } while (! _lastCollect.compareAndSet(myCurrentLast, aCollect));

        return true;
    }

    /**
     * @return <code>true</code> if the collect is either from the existing
     *         leader, or there is no leader or there's been nothing heard from
     *         the current leader within DEFAULT_LEASE milliseconds else
     *         <code>false</code>
     */
    boolean amAccepting(Transport.Packet aCollect) {
        long myCurrentTime = System.currentTimeMillis();

        if (((Collect) _lastCollect.get().getMessage()).isInitial()) {
            _logger.trace("Current collect is initial - allow leader");

            return true;
        } else {
            if (_leaderUtils.sameLeader(aCollect, _lastCollect.get())) {
                _logger.trace("Current collect is from same leader - allow");

                return true;
            } else
                _logger.trace("Check leader expiry: " + myCurrentTime + ", " + _lastLeaderActionTime.get() + ", " +
                        Leader.LeaseDuration.get() + ", " + (myCurrentTime > _lastLeaderActionTime.get()
                        + Leader.LeaseDuration.get()));

            return (myCurrentTime > _lastLeaderActionTime.get()
                    + Leader.LeaseDuration.get());
        }
    }

    boolean sameLeader(Transport.Packet aCollect) {
        return _leaderUtils.sameLeader(aCollect, _lastCollect.get());
    }

    boolean originates(Transport.Packet aBegin) {
        return _leaderUtils.originates(aBegin, _lastCollect.get());
    }

    boolean precedes(Transport.Packet aBegin) {
        return _leaderUtils.precedes(aBegin, _lastCollect.get());
    }
}

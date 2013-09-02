package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class Common {
    private static final Logger _logger = LoggerFactory.getLogger(Common.class);

    private Transport _transport;
    private final MessageBasedFailureDetector _fd;
    private final AtomicReference<Transport.Packet> _lastCollect =
            new AtomicReference<Transport.Packet>(new FakePacket(Collect.INITIAL));
    private AtomicLong _lastLeaderActionTime = new AtomicLong(0);
    private final Timer _watchdog = new Timer("Paxos timers");
    private final AtomicReference<Constants.FSMStates> _fsmState =
            new AtomicReference<Constants.FSMStates>(Constants.FSMStates.INITIAL);
    private final AtomicReference<AcceptorLearner.Watermark> _lowWatermark =
            new AtomicReference<AcceptorLearner.Watermark>(AcceptorLearner.Watermark.INITIAL);
    private final LeaderUtils _leaderUtils = new LeaderUtils();

    private class FakePacket implements Transport.Packet {
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
    }

    Common(Transport aTransport, MessageBasedFailureDetector anFD) {
        _transport = aTransport;
        _fd = anFD;
    }

    Common(MessageBasedFailureDetector anFD) {
        this(null, anFD);
    }
    
    void setTransport(Transport aTransport) {
    	_transport = aTransport;
    }
    
    Timer getWatchdog() {
        return _watchdog;
    }

    Transport getTransport() {
        return _transport;
    }

    long install(AcceptorLearner.Watermark aWatermark) {
        _lowWatermark.set(aWatermark);

        return aWatermark.getSeqNum();
    }

    AcceptorLearner.Watermark getLowWatermark() {
        return _lowWatermark.get();
    }

    FailureDetector getFD() {
        return _fd;
    }

    MessageBasedFailureDetector getPFD() {
        return _fd;
    }

    void stop() {
        _fd.stop();
        _watchdog.cancel();
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
    
    void leaderAction() {
        _lastLeaderActionTime.set(System.currentTimeMillis());
    }

    void resetLeaderAction() {
        _lastLeaderActionTime.set(0);
    }

    void setState(Constants.FSMStates aState) {
        _fsmState.getAndSet(aState);
    }

    boolean testAndSetState(Constants.FSMStates anExpected, Constants.FSMStates aNewState) {
        return _fsmState.compareAndSet(anExpected, aNewState);
    }

    boolean testState(Constants.FSMStates aState) {
        return _fsmState.get().equals(aState);
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
            _logger.debug("Current collect is initial - allow leader");

            return true;
        } else {
            if (_leaderUtils.sameLeader(aCollect, _lastCollect.get())) {
                _logger.debug("Current collect is from same leader - allow");

                return true;
            } else
                _logger.debug("Check leader expiry: " + myCurrentTime + ", " + _lastLeaderActionTime.get() + ", " +
                        Constants.getLeaderLeaseDuration() + ", " + (myCurrentTime > _lastLeaderActionTime.get()
                        + Constants.getLeaderLeaseDuration()));

            return (myCurrentTime > _lastLeaderActionTime.get()
                    + Constants.getLeaderLeaseDuration());
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

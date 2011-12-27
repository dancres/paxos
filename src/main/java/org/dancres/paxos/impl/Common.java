package org.dancres.paxos.impl;

import org.dancres.paxos.Paxos;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Common {
    private static Logger _logger = LoggerFactory.getLogger(Common.class);

    private Transport _transport;
    private FailureDetector _fd;
    private Collect _lastCollect = Collect.INITIAL;
    private long _lastLeaderActionTime = 0;
    private final List<Paxos.Listener> _listeners = new ArrayList<Paxos.Listener>();
    private final RecoveryTrigger _trigger = new RecoveryTrigger();

    public Common(Transport aTransport, long anUnresponsivenessThreshold) {
        _transport = aTransport;
        _fd = new FailureDetectorImpl(anUnresponsivenessThreshold);
    }
    
    public Common(Transport aTransport, FailureDetector anFD) {
        _transport = aTransport;
        _fd = anFD;
    }

    public Common(long anUnresponsivenessThreshold) {
        _fd = new FailureDetectorImpl(anUnresponsivenessThreshold);    	
    }
    
    void setTransport(Transport aTransport) {
    	_transport = aTransport;
    }
    
    Transport getTransport() {
        return _transport;
    }

    RecoveryTrigger getRecoveryTrigger() {
        return _trigger;
    }

    FailureDetector getFD() {
        return _fd;
    }
    
    void stop() {
        _fd.stop();
        _transport.shutdown();
    }
    
    void resetLeader() {
        _lastCollect = Collect.INITIAL;
        _lastLeaderActionTime = 0;        
    }
    
    void setLastCollect(Collect aCollect) {
        synchronized(this) {
            _lastCollect = aCollect;
        }
    }
    
    public Collect getLastCollect() {
        synchronized(this) {
            return _lastCollect;
        }
    }
    
    void leaderAction() {
        synchronized(this) {
            _lastLeaderActionTime = System.currentTimeMillis();
        }
    }

    /**
     * @param aCollect
     *            should be tested to see if it supercedes the current COLLECT
     * @return <code>true</code> if it supercedes, <code>false</code> otherwise
     */
    boolean supercedes(Collect aCollect) {
        synchronized(this) {
            if (aCollect.supercedes(_lastCollect)) {
                _lastCollect = aCollect;

                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * @return <code>true</code> if the collect is either from the existing
     *         leader, or there is no leader or there's been nothing heard from
     *         the current leader within DEFAULT_LEASE milliseconds else
     *         <code>false</code>
     */
    boolean amAccepting(Collect aCollect) {
        long myCurrentTime = System.currentTimeMillis();

        synchronized(this) {
            if (_lastCollect.isInitial()) {
                _logger.debug("Current collect is initial - allow leader");

                return true;
            } else {
                if (aCollect.sameLeader(getLastCollect())) {
                    _logger.debug("Current collect is from same leader - allow");

                    return true;
                } else
                    _logger.debug("Check leader expiry: " + (myCurrentTime > _lastLeaderActionTime
                            + Constants.getLeaderLeaseDuration()));

                return (myCurrentTime > _lastLeaderActionTime
                        + Constants.getLeaderLeaseDuration());
            }
        }
    }

    void add(Paxos.Listener aListener) {
        synchronized(_listeners) {
            _listeners.add(aListener);
        }
    }

    void remove(Paxos.Listener aListener) {
        synchronized(_listeners) {
            _listeners.remove(aListener);
        }
    }

    void signal(VoteOutcome aStatus) {
        List<Paxos.Listener> myListeners;

        synchronized(_listeners) {
            myListeners = new ArrayList<Paxos.Listener>(_listeners);
        }

        for (Paxos.Listener myTarget : myListeners)
            myTarget.done(aStatus);
    }
}

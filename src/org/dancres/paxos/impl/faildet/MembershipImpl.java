package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.LivenessListener;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.Membership;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A snapshot of the membership at some point in time, updated by the <code>FailureDetectorImpl</code> over time.  Note the snapshot only
 * reduces in size, it cannot grow so as to allow correct behaviour in cases where majorities are required.
 *
 * @author dan
 */
class MembershipImpl implements Membership, LivenessListener {
    /**
     * Tracks the membership that forms the base for each round
     */
    private Set<InetSocketAddress> _initialMemberAddresses = new HashSet<InetSocketAddress>();

    /**
     * Tracks the members that have yet to respond in a round
     */
    private Set<InetSocketAddress> _outstandingMemberAddresses;

    private FailureDetectorImpl _parent;
    private boolean _populated = false;
    private MembershipListener _listener;

    private int _expectedResponses;
    private int _receivedResponses;

    private Logger _logger = LoggerFactory.getLogger(MembershipImpl.class);

    private boolean _disposed = false;

    MembershipImpl(FailureDetectorImpl aParent, MembershipListener aListener) {
        _listener = aListener;
        _parent = aParent;
    }

    public boolean startInteraction() {
        synchronized(this) {
            if (!abort()) {
                _receivedResponses = 0;
                _expectedResponses = _initialMemberAddresses.size();
                _outstandingMemberAddresses = new HashSet(_initialMemberAddresses);
                return true;
            } else {
            	return false;
            }
        }
    }

    public boolean receivedResponse(InetSocketAddress anAddress) {
        synchronized(this) {
            if (_outstandingMemberAddresses.remove(anAddress)) {
                ++_receivedResponses;
                interactionComplete();
                return true;
            } else {
                _logger.warn("Not an expected response: " + anAddress);
                return false;
            }
        }
    }

    public void alive(InetSocketAddress aProcess) {
        // Not interested in new arrivals
    }

    public void dead(InetSocketAddress aProcess) {
        _logger.warn("Death detected: " + aProcess);

        synchronized(this) {
            // Delay messages until we've got a member set
            while (_populated == false) {
                try {
                    wait();
                } catch (InterruptedException anIE) {
                }
            }

            _outstandingMemberAddresses.remove(aProcess);
            _initialMemberAddresses.remove(aProcess);

            // startInteraction will reset this so if we get a dead before then, it should be recorded
            //
            --_expectedResponses;

            if (abort())
                return;

            interactionComplete();
        }
    }

    void populate(Set<InetSocketAddress> anActiveAddresses) {
        _logger.debug("Populating membership");

        synchronized(this) {
            _logger.debug("Populating membership - got lock");

            _initialMemberAddresses.addAll(anActiveAddresses);

            _logger.debug("Populating membership - addresses added");

            _populated = true;

            // Now we have a member set, accept updates
            notifyAll();
        }
    }

    public int getSize() {
        synchronized(this) {
            return _initialMemberAddresses.size();
        }
    }

    public void dispose() {
    	_logger.debug("Membership disposed");
    	
        _parent.remove(this);

        synchronized(this) {
            _disposed = true;
        }
    }

    private boolean interactionComplete() {
        if ((_receivedResponses == _expectedResponses) || (_receivedResponses >= _parent.getMajority())) {
            _listener.allReceived();
            return true;
        }

        return false;
    }

    private boolean abort() {
        if (_initialMemberAddresses.size() < _parent.getMajority()) {
            _listener.abort();
            return true;
        }

        return false;
    }

    protected void finalize() throws Throwable {
        synchronized(this) {
            if (_disposed)
                return;
        }

        System.err.println("Membership was not disposed");
        System.err.flush();
    }
}

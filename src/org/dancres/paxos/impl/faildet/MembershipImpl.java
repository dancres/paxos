package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.Membership;
import java.util.Set;
import java.util.HashSet;

import org.dancres.paxos.impl.util.NodeId;
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
     * @todo Fix up this majority to be more dynamic
     */
    private static final int MAJORITY = 2;

    /**
     * Tracks the membership that forms the base for each round
     */
    private Set _initialMemberAddresses = new HashSet();

    /**
     * Tracks the members that have yet to respond in a round
     */
    private Set _outstandingMemberAddresses;

    private FailureDetectorImpl _parent;
    private boolean _populated = false;
    private MembershipListener _listener;

    private int _expectedResponses;
    private int _receivedResponses;

    private Logger _logger = LoggerFactory.getLogger(MembershipImpl.class);

    static boolean haveMajority(int aSize) {
        return (aSize >= MAJORITY);
    }

    MembershipImpl(FailureDetectorImpl aParent, MembershipListener aListener) {
        _listener = aListener;
        _parent = aParent;
    }

    public void startInteraction() {
        synchronized(this) {
            if (!abort()) {
                _receivedResponses = 0;
                _expectedResponses = _initialMemberAddresses.size();
                _outstandingMemberAddresses = new HashSet(_initialMemberAddresses);
            }
        }
    }

    public void receivedResponse(NodeId anAddress) {
        synchronized(this) {
            if (_outstandingMemberAddresses.remove(anAddress)) {
                ++_receivedResponses;
                interactionComplete();
            } else {
                _logger.warn("Not an expected response: " + anAddress);
            }
        }
    }

    public void alive(NodeId aProcess) {
        // Not interested in new arrivals
    }

    public void dead(NodeId aProcess) {
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
            --_expectedResponses;

            if (abort())
                return;

            interactionComplete();
        }
    }

    void populate(Set anActiveAddresses) {
        _logger.info("Populating membership");

        synchronized(this) {
            _logger.info("Populating membership - got lock");

            _initialMemberAddresses.addAll(anActiveAddresses);

            _logger.info("Populating membership - addresses added");

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
        _parent.remove(this);
    }

    public int getMajority() {
        return MAJORITY;
    }

    private boolean interactionComplete() {
        if (_receivedResponses == _expectedResponses) {
            _listener.allReceived();
            return true;
        }

        return false;
    }

    private boolean abort() {
        if (_initialMemberAddresses.size() < MAJORITY) {
            _listener.abort();
            return true;
        }

        return false;
    }
}

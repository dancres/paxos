package org.dancres.paxos.impl.faildet;

import com.sun.imageio.plugins.jpeg.JPEGImageReader;

import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.HashSet;
import java.net.SocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MembershipImpl implements Membership, LivenessListener {
    /**
     * @todo Fix up this majority to be more dynamic
     */
    private static final int MAJORITY = 2;

    private Set _memberAddresses = new HashSet();

    private FailureDetector _parent;
    private boolean _populated = false;
    private MembershipListener _listener;

    private int _expectedResponses;
    private int _receivedResponses;

    private Logger _logger = LoggerFactory.getLogger(MembershipImpl.class);

    static boolean haveMajority(int aSize) {
        return (aSize >= MAJORITY);
    }

    MembershipImpl(FailureDetector aParent, MembershipListener aListener) {
        _listener = aListener;
        _parent = aParent;
    }

    public void startInteraction() {
        synchronized(this) {
            if (!abort()) {
                _receivedResponses = 0;
                _expectedResponses = _memberAddresses.size();
            }
        }
    }

    public void receivedResponse() {
        synchronized(this) {
            ++_receivedResponses;
            interactionComplete();
        }
    }

    public void alive(SocketAddress aProcess) {
        // Not interested in new arrivals
    }

    public void dead(SocketAddress aProcess) {
        synchronized(this) {
            // Delay messages until we've got a member set
            while (_populated == false) {
                try {
                    wait();
                } catch (InterruptedException anIE) {
                }
            }

            _memberAddresses.remove(aProcess);
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

            _memberAddresses.addAll(anActiveAddresses);

            _logger.info("Populating membership - addresses added");

            _populated = true;

            // Now we have a member set, accept updates
            notifyAll();
        }
    }

    public int getSize() {
        synchronized(this) {
            return _memberAddresses.size();
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
        if (_memberAddresses.size() < MAJORITY) {
            _listener.abort();
            return true;
        }

        return false;
    }
}

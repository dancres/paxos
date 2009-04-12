package org.dancres.paxos.impl.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dancres.paxos.impl.faildet.FailureDetector;

import java.util.Map;
import java.util.TreeMap;
import java.net.InetSocketAddress;

class ProposerState {
    /**
     * The next entry in the ledgers we will try and fill - aka log number
     */
    private long _nextSeqNum = 0;
    private Map _activeRounds = new TreeMap();

    private long _nodeId;

    private Logger _logger = LoggerFactory.getLogger(ProposerState.class);

    private FailureDetector _fd;

    private InetSocketAddress _addr;

    /**
     * @param aDetector to maintain for use by proposers
     * @param anAddr to use for node id generation
     */
    ProposerState(FailureDetector aDetector, InetSocketAddress anAddr) {
        _fd = aDetector;

        byte[] myAddress = anAddr.getAddress().getAddress();
        long myNodeId = 0;

        // Only cope with IPv4 right now
        //
        assert (myAddress.length == 4);

        for (int i = 0; i < 4; i++) {
            myNodeId = myNodeId << 8;
            myNodeId |= (int) myAddress[i] & 0xFF;
        }

        myNodeId = myNodeId << 32;
        myNodeId |= anAddr.getPort();
        _nodeId = myNodeId;

        _addr = anAddr;

        _logger.info("Initialized state with id: " + Long.toHexString(_nodeId));
    }

    private long getNextSeqNum() {
        return _nextSeqNum++;
    }

    FailureDetector getFailureDetector() {
        return _fd;
    }

    /**
     * Create a leader instance for this round
     *
     * @param aChannel is the broadcast channel the leader should use to communicate with acceptor/learners
     * @param aClientChannel is the channel to send the outcome to when leader is done
     * @return
     */
    LeaderImpl newLeader(Transport aTransport, Address aClientAddress) {
        synchronized(this) {
            long mySeqNum = getNextSeqNum();

            LeaderImpl myLeader = new LeaderImpl(mySeqNum, this, aTransport, aClientAddress);
            _activeRounds.put(new Long(mySeqNum), myLeader);

            return myLeader;
        }
    }

    long getNodeId() {
        return _nodeId;
    }

    InetSocketAddress getAddress() {
        return _addr;
    }
    
    void dispose(long aSeqNum) {
        synchronized(this) {
            _activeRounds.remove(new Long(aSeqNum));
        }
    }

    LeaderImpl getLeader(long aSeqNum) {
        synchronized(this) {
            return (LeaderImpl) _activeRounds.get(new Long(aSeqNum));
        }
    }
}

package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constitutes the core implementation of paxos. Requires a <code>Transport</code> to use for membership,
 * failure detection and paxos rounds.
 */
public class Core implements Transport.Dispatcher, Paxos {
    private static Logger _logger = LoggerFactory.getLogger(Core.class);

    private final byte[] _meta;
    private AcceptorLearner _al;
    private LeaderFactory _ld;
    private Heartbeater _hb;
    private final LogStorage _log;
    private final Common _common;
    private final CheckpointHandle _handle;

    /**
     * @param anUnresponsivenessThreshold is the minimum period of time a node must be unresponsive for before being
     * declared dead by the cluster
     *
     * @param aLogger is the storage implementation to use for recording paxos transitions.
     * @param aMeta is the data to be advertised by this core to others. Might be used for server discovery in cases
     * where the server/client do not use the "well known" addresses of the core.
     * @param aListener is the handler for paxos outcomes. There can be many of these but there must be at least one at
     * initialisation time.
     */
    public Core(long anUnresponsivenessThreshold, LogStorage aLogger, byte[] aMeta, CheckpointHandle aHandle,
                Paxos.Listener aListener) {
        _meta = aMeta;
        _log = aLogger;        
        _common = new Common(anUnresponsivenessThreshold);
        _common.add(aListener);
        _handle = aHandle;
    }

    public void close() {
        _hb.halt();

        try {
            _hb.join();
        } catch (InterruptedException anIE) {
        }
        
        _common.stop();

        _ld.shutdown();

        _al.close();
    }

    public void setTransport(Transport aTransport) throws Exception {
        _common.setTransport(aTransport);

        if (_meta == null)
            _hb = new Heartbeater(aTransport, aTransport.getLocalAddress().toString().getBytes());
        else
            _hb = new Heartbeater(aTransport, _meta);

        _al = new AcceptorLearner(_log, _common);
        _al.open(_handle);
        _ld = new LeaderFactory(_common);
        _hb.start();
    }

    public CheckpointHandle newCheckpoint() {
        return _al.newCheckpoint();
    }

    public void bringUpToDate(CheckpointHandle aHandle) throws Exception {
        _al.bringUpToDate(aHandle);
    }

    public FailureDetector getDetector() {
        return _common.getFD();
    }

    public AcceptorLearner getAcceptorLearner() {
        return _al;
    }

    public Common getCommon() {
        return _common;
    }

    public void add(Paxos.Listener aListener) {
    	_common.add(aListener);
    }
    
    public boolean messageReceived(Packet aPacket) {
    	PaxosMessage myMessage = aPacket.getMessage();
    	
        try {
            switch (myMessage.getClassification()) {
                case PaxosMessage.FAILURE_DETECTOR: {
                    _common.getPrivateFD().processMessage(myMessage);

                    return true;
                }

                case PaxosMessage.LEADER:
                case PaxosMessage.RECOVERY: {
                    _al.messageReceived(myMessage);

                    return true;
                }

                case PaxosMessage.ACCEPTOR_LEARNER: {
                    _ld.messageReceived(myMessage);

                    return true;
                }

                default: {
                    _logger.debug("Unrecognised message:" + myMessage);
                    return false;
                }
            }
        } catch (Exception anE) {
            _logger.error("Unexpected exception", anE);
            return false;
        }
    }

    public void submit(Proposal aVal) throws Paxos.InactiveException {
        _ld.newLeader().submit(aVal);
    }
}


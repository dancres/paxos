package org.dancres.paxos.impl;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.Paxos;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constitutes the core implementation of paxos. Requires a <code>Transport</code> to use for membership,
 * failure detection and paxos rounds.
 */
public class Core implements Transport.Dispatcher {
    private static Logger _logger = LoggerFactory.getLogger(Core.class);

    private Transport _tp;
    private Paxos.Listener _listener;
    private byte[] _meta = null;
    private AcceptorLearner _al;
    private Leader _ld;
    private Heartbeater _hb;
    private long _unresponsivenessThreshold;
    private LogStorage _log;
    private Common _common;

    /**
     * @param anUnresponsivenessThreshold is the minimum period of time a node must be unresponsive for before being
     * declared dead by the cluster
     *
     * @param aLogger is the logger implementation to use for recording paxos transitions.
     * @param aMeta is the data to be advertised by this core to others. Might be used for server discovery in cases
     * where the server/client do not use the "well known" addresses of the core.
     * @param aListener is the handler for paxos outcomes. There can be many of these but there must be at least one at
     * initialisation time.
     */
    public Core(long anUnresponsivenessThreshold, LogStorage aLogger, byte[] aMeta,
                Paxos.Listener aListener) {
        _meta = aMeta;
        _listener = aListener;
        _log = aLogger;        
        _unresponsivenessThreshold = anUnresponsivenessThreshold;
    }

    public void stop() {
        _common.stop();
        _hb.halt();

        try {
            _hb.join();
        } catch (InterruptedException anIE) {
        }

        _tp.shutdown();

        _ld.shutdown();

        _al.close();
    }

    public void setTransport(Transport aTransport) throws Exception {
        _tp = aTransport;
        _common = new Common(aTransport, _unresponsivenessThreshold);

        if (_meta == null)
            _hb = new Heartbeater(_tp, _tp.getLocalAddress().toString().getBytes());
        else
            _hb = new Heartbeater(_tp, _meta);

        _al = new AcceptorLearner(_log, _common);
        _al.open();
        _ld = new Leader(_common);
        _al.add(_listener);
        _hb.start();
    }

    public FailureDetector getFailureDetector() {
        return _common.getFD();
    }

    public AcceptorLearner getAcceptorLearner() {
        return _al;
    }

    public Leader getLeader() {
        return _ld;
    }


    public boolean messageReceived(Packet aPacket) {
    	PaxosMessage myMessage = aPacket.getMessage();
    	
        try {
            switch (myMessage.getClassification()) {
                case PaxosMessage.FAILURE_DETECTOR: {
                    _common.getFD().processMessage(myMessage);

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

    public void submit(Proposal aVal) {
        _ld.submit(aVal);
    }
}


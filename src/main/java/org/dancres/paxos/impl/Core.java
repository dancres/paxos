package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Constitutes the core implementation of paxos. Requires a <code>Transport</code> to use for membership,
 * failure detection and paxos rounds. In essence, this is the plumbing that glues the state machines and
 * other bits together.
 */
public class Core implements Transport.Dispatcher, Paxos {
    private static final Logger _logger = LoggerFactory.getLogger(Core.class);

    private final byte[] _meta;
    private AcceptorLearner _al;
    private LeaderFactory _ld;
    private Heartbeater _hb;
    private final LogStorage _log;
    private final Common _common;
    private final CheckpointHandle _handle;

    /**
     * @param aLogger is the storage implementation to use for recording paxos transitions.
     * @param aMeta is the data to be advertised by this core to others. Might be used for server discovery in cases
     * where the server/client do not use the "well known" addresses of the core.
     * @param aListener is the handler for paxos outcomes. There can be many of these but there must be at least one at
     * initialisation time.
     */
    public Core(MessageBasedFailureDetector anFD, LogStorage aLogger, byte[] aMeta, CheckpointHandle aHandle,
                Listener aListener) {
        _meta = aMeta;
        _log = aLogger;        
        _common = new Common(anFD);
        _common.add(aListener);
        _handle = aHandle;
    }

    public void close() {
        _common.getTransport().terminate();
    }

    public void terminate() {
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
            _hb = _common.getPrivateFD().newHeartbeater(aTransport, aTransport.getLocalAddress().toString().getBytes());
        else
            _hb = _common.getPrivateFD().newHeartbeater(aTransport, _meta);

        _al = new AcceptorLearner(_log, _common);
        _al.open(_handle);
        _ld = new LeaderFactory(_common);
        _hb.start();
    }

    public CheckpointHandle newCheckpoint() {
        return _al.newCheckpoint();
    }

    public boolean bringUpToDate(CheckpointHandle aHandle) throws Exception {
        return _al.bringUpToDate(aHandle);
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

    public void add(Listener aListener) {
    	_common.add(aListener);
    }
    
    public boolean messageReceived(Packet aPacket) {
    	PaxosMessage myMessage = aPacket.getMessage();
    	
        try {
            switch (myMessage.getClassification()) {
                case PaxosMessage.FAILURE_DETECTOR: {
                    _common.getPrivateFD().processMessage(aPacket);

                    return true;
                }

                case PaxosMessage.LEADER:
                case PaxosMessage.RECOVERY: {
                    _al.messageReceived(aPacket);

                    return true;
                }

                case PaxosMessage.ACCEPTOR_LEARNER: {
                    _ld.messageReceived(aPacket);

                    return true;
                }

                default: {
                    _logger.debug("Unrecognised message:" + aPacket.getSource() + " " + myMessage);
                    return false;
                }
            }
        } catch (Exception anE) {
            _logger.error("Unexpected exception", anE);
            return false;
        }
    }

    /**
     * @todo Batching could be done here:
     *
     * <ol>
     *     <li>If first in (effected by atomic CAS on a boolean), atomic-queue proposal and then create a leader.</li>
     *     <li>Once leader is created, atomic-take all proposals in queue and reset first-in (CAS)</li>
     *     <li>If not first in, atomic queue proposal</li>
     * </ol>
     *
     * @param aVal
     * @throws org.dancres.paxos.InactiveException
     */
    public void submit(Proposal aVal, Completion aCompletion) throws InactiveException {
        _ld.newLeader().submit(aVal, aCompletion);
    }
}


package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Constitutes the core implementation of paxos. Requires a <code>Transport</code> to use for membership,
 * failure detection and paxos rounds. In essence, this is the plumbing that glues the state machines and
 * other bits together.
 */
public class Core implements Transport.Dispatcher, Paxos {
    private static final Logger _logger = LoggerFactory.getLogger(Core.class);

    private final AcceptorLearner _al;
    private final LeaderFactory _ld;
    private final Common _common;
    private final CheckpointHandle _handle;
    private final AtomicBoolean _initd = new AtomicBoolean(false);
    private final List<MessageProcessor> _msgProcs;

    /**
     * @param aLogger is the storage implementation to use for recording paxos transitions.
     * where the server/client do not use the "well known" addresses of the core.
     * @param aListener is the handler for paxos outcomes. There can be many of these but there must be at least one at
     * initialisation time.
     */
    public Core(LogStorage aLogger, CheckpointHandle aHandle, Listener aListener) {
        this(aLogger, aHandle, aListener, false);
    }

    public Core(LogStorage aLogger, CheckpointHandle aHandle,
                Listener aListener, boolean isDisableLeaderHeartbeats) {
        _common = new Common();
        _al = new AcceptorLearner(aLogger, _common, aListener);
        _ld = new LeaderFactory(_common, isDisableLeaderHeartbeats);
        _handle = aHandle;
        _msgProcs = Arrays.asList(_al, _ld);
    }

    public void close() {
        _common.getTransport().terminate();
    }

    public void terminate() {
        _logger.info(toString() + " terminating");

        _common.stop();

        _ld.shutdown();

        _al.close();
    }

    public void init(Transport aTransport) throws Exception {
        _common.setTransport(aTransport);

        _logger.info(toString() + " initialised");

        _al.open(_handle);

        _initd.set(true);
    }

    public CheckpointHandle newCheckpoint() {
        return _al.newCheckpoint();
    }

    public boolean bringUpToDate(CheckpointHandle aHandle) throws Exception {
        return _al.bringUpToDate(aHandle);
    }

    public FailureDetector getDetector() {
        return _common.getTransport().getFD();
    }

    public AcceptorLearner getAcceptorLearner() {
        return _al;
    }

    Common getCommon() {
        return _common;
    }

    public void add(Listener aListener) {
    	_al.add(aListener);
    }
    
    public boolean messageReceived(Packet aPacket) {
        if (! _initd.get())
            return false;

        boolean didProcess = false;

        try {

            for (MessageProcessor myMP : _msgProcs) {
                if (myMP.accepts(aPacket)) {
                    myMP.processMessage(aPacket);
                    didProcess = true;
                }
            }

            return didProcess;

        } catch (Throwable anE) {
            _logger.error(toString() + " Unexpected exception", anE);
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
    public void submit(Proposal aVal, Completion<VoteOutcome> aCompletion) throws InactiveException {
        _ld.newLeader().submit(aVal, aCompletion);
    }

    public String toString() {
        return "CR [ " + _common.getTransport().getLocalAddress() + " ]";
    }
}


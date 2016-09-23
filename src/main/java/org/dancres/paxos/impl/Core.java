package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
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
        _logger.debug(toString() + " terminating");

        _common.stop();

        _ld.shutdown();

        _al.close();
    }

    public void init(Transport aTransport) throws Exception {
        _common.setTransport(aTransport);

        _logger.debug(toString() + " initialised");

        AcceptorLearner.LedgerPosition myState = _al.open(_handle);
        _ld.resumeAt(myState.getSeqNum(), myState.getRndNum());

        _initd.set(true);
    }

    public CheckpointHandle newCheckpoint() {
        return _al.newCheckpoint();
    }

    public boolean bringUpToDate(CheckpointHandle aHandle) throws Exception {
        return _al.bringUpToDate(aHandle);
    }

    private class MembershipImpl implements Membership {
        private final Assembly _assembly;

        private MembershipImpl(Assembly anAssembly) {
            _assembly = anAssembly;
        }

        public Map<InetSocketAddress, MetaData> getMembers() {
            Map<InetSocketAddress, MetaData> myMembership = new HashMap<>();

            for (Map.Entry<InetSocketAddress, FailureDetector.MetaData> myPair : _assembly.getMembers().entrySet())
                myMembership.put(myPair.getKey(), new MetaDataImpl(myPair.getValue()));

            return myMembership;
        }

        private class MetaDataImpl implements MetaData {
            private final FailureDetector.MetaData _metaData;

            private MetaDataImpl(FailureDetector.MetaData aMeta) {
                _metaData = aMeta;
            }

            public byte[] getData() {
                return _metaData.getData();
            }

            public long getTimestamp() {
                return _metaData.getTimestamp();
            }
        }

        public byte[] dataForNode(InetSocketAddress anAddress) {
            return _assembly.dataForNode(anAddress);
        }

        public boolean updateMembership(Collection<InetSocketAddress> aMembers) throws InactiveException {
            return Core.this.updateMembership(aMembers);
        }
    }

    public Membership getMembership() { return new MembershipImpl(_common.getTransport().getFD().getMembers()); }

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

        for (MessageProcessor myMP : _msgProcs) {
            if (myMP.accepts(aPacket)) {
                try {
                    myMP.processMessage(aPacket);
                    didProcess = true;
                } catch (Throwable anE) {
                    _logger.error(toString() + " Unexpected exception against " + aPacket, anE);
                }
            }
        }

        return didProcess;
    }

    /**
     * TODO: Batching could be done here:
     *
     * <ol>
     *     <li>If first in (effected by atomic CAS on a boolean), atomic-queue proposal and then create a leader.</li>
     *     <li>Once leader is created, atomic-take all proposals in queue and reset first-in (CAS)</li>
     *     <li>If not first in, atomic queue proposal</li>
     * </ol>
     *
     * @param aVal
     * @param aCompletion
     * @throws org.dancres.paxos.InactiveException
     */
    public void submit(Proposal aVal, final Completion<VoteOutcome> aCompletion) throws InactiveException {
        /*
         * First outcome is always the one we report to the submitter even if there are others (available via
         * getOutcomes()). Multiple outcomes occur when we detect a previously proposed value and must drive it
         * to completion. The originally submitted value will need re-submitting. Hence submitter is told
         * OTHER_VALUE whilst AL listeners will see VALUE containing the previously proposed value.
         */
        _ld.submit(aVal, aCompletion);
    }

    boolean updateMembership(Collection<InetSocketAddress> aMembers) throws InactiveException {
        return _ld.updateMembership(aMembers);
    }

    public String toString() {
        return "CR [ " + _common.getTransport().getLocalAddress() + " ]";
    }
}


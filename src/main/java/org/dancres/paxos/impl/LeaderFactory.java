package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>Each paxos instance is driven and represented by an individual instance of <code>Leader</code>.
 * These are created, tracked and driven by this factory class. The factory also looks after handling error outcomes
 * that require an adjustment in round or sequence number and heartbeating.</p>
 *
 * @see Leader
 */
class LeaderFactory implements ProposalAllocator.Listener, MessageProcessor {
    private static final Logger _logger = LoggerFactory.getLogger(LeaderFactory.class);

    private final Common _common;
    private ProposalAllocator _stateFactory;
    private final boolean _disableHeartbeats;
    private final Map<Long, Leader> _activeLeaders = new ConcurrentHashMap<>();

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private AtomicReference<TimerTask> _heartbeatAlarm = new AtomicReference<>();

    LeaderFactory(Common aCommon, boolean isDisableHeartbeats) {
        _common = aCommon;
        _disableHeartbeats = isDisableHeartbeats;
    }

    void init(ProposalAllocator aPropAllocator) {
        _stateFactory = aPropAllocator;

        if (! _disableHeartbeats)
            _stateFactory.add(this);
    }

    /**
     * @throws org.dancres.paxos.InactiveException if the Paxos process is currently out of date or shutting down
     *
     * We stop allowing leaders in this process so as to avoid breaching the constraint where we can be sure we've
     * recorded an outcome at least locally.
     *
     * TODO: Allow concurrent leaders - this would also require modifications to the out-of-date detection used in
     * AcceptorLearner.
     */
    private Leader newLeader() throws InactiveException {
        if ((_common.getNodeState().test(NodeState.State.SHUTDOWN)) ||
                (_common.getNodeState().test(NodeState.State.OUT_OF_DATE)))
            throw new InactiveException();

        killHeartbeats();

        return newLeaderImpl();
    }

    void submit(Proposal aValue, final Completion<VoteOutcome> aCompletion) throws InactiveException {
        newLeader().submit(aValue, new Completion<Leader>() {
            public void complete(Leader aLeader) {
                _stateFactory.conclusion(aLeader, aLeader.getOutcomes().getLast());
                aCompletion.complete(aLeader.getOutcomes().getFirst());
                _activeLeaders.remove(aLeader.getSeqNum());
            }
        });
    }

    private void killHeartbeats() {
        TimerTask myTask = _heartbeatAlarm.getAndSet(null);

        if (myTask != null) {
            myTask.cancel();
            _common.getWatchdog().purge();
        }
    }

    private Leader newLeaderImpl() {
        Leader myLeader = new Leader(_common, _stateFactory.nextInstance(0));
        _activeLeaders.put(myLeader.getSeqNum(), myLeader);

        return myLeader;
    }

    public void inFlight() {
        killHeartbeats();
    }

    /**
     *
     * TODO: Increment round number via heartbeats every so often to avoid jittering collects
     */
    public void allConcluded() {
        if (_stateFactory.amLeader()) {
            // Still leader so heartbeat
            //
            TimerTask myTask =  new TimerTask() {
                public void run() {
                    _logger.trace(this + ": sending heartbeat: " + System.currentTimeMillis());

                    try {
                        submit(new Proposal(AcceptorLearner.HEARTBEAT_KEY, "hearbeat".getBytes()),
                                new Completion<VoteOutcome>() {
                                    public void complete(VoteOutcome anOutcome) {}});
                    } catch (InactiveException anIE) {
                        // Nothing to worry about, just give up
                    }
                }
            };

            if (_heartbeatAlarm.compareAndSet(null, myTask)) {
                _common.getWatchdog().schedule(myTask, calculateLeaderRefresh());
            } else {
                myTask.cancel();
            }
        }
    }

    boolean updateMembership(Collection<InetSocketAddress> aClusterMembers) throws InactiveException {
        final CompletionImpl<VoteOutcome> myResult = new CompletionImpl<>();

        submit(new Proposal(AcceptorLearner.MEMBER_CHANGE_KEY, Codecs.flatten(aClusterMembers)),
                new Completion<VoteOutcome>() {
                    public void complete(VoteOutcome anOutcome) {
                        myResult.complete(anOutcome);
                    }
                }
        );

        VoteOutcome myOutcome = myResult.await();

        return ((myOutcome.getResult() == VoteOutcome.Reason.VALUE) &&
                (myOutcome.getValues().get(AcceptorLearner.MEMBER_CHANGE_KEY) != null));
    }

    private long calculateLeaderRefresh() {
        long myExpiry = Leader.LeaseDuration.get();
        return myExpiry - (myExpiry * 10 / 100);
    }

    public void shutdown() {
        killHeartbeats();

        for (Leader myLeader : _activeLeaders.values())
            myLeader.shutdown();
    }

    public boolean accepts(Transport.Packet aPacket) {
        return aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.LEADER);
    }

    public void processMessage(Transport.Packet aPacket) {
        _logger.trace("Got packet for leaders: " + aPacket.getSource() + "->" + aPacket.getMessage());
        
        for (Leader myLeader : _activeLeaders.values())
            myLeader.processMessage(aPacket);
    }
}

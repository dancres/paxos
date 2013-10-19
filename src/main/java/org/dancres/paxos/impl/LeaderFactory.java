package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * <p>Each paxos instance is driven and represented by an individual instance of <code>Leader</code>.
 * These are created, tracked and driven by this factory class. The factory also looks after handling error outcomes
 * that require an adjustment in round or sequence number and heartbeating.</p>
 *
 * @see Leader
 */
class LeaderFactory implements ProposalAllocator.Listener, MessageProcessor {
    private static final Logger _logger = LoggerFactory.getLogger(LeaderFactory.class);

    public static final Proposal HEARTBEAT = new Proposal("heartbeat",
            "org.dancres.paxos.Heartbeat".getBytes());
    
    private final Common _common;
    private ProposalAllocator _stateFactory;
    private Leader _currentLeader;
    private final boolean _disableHeartbeats;

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private TimerTask _heartbeatAlarm;

    LeaderFactory(Common aCommon, boolean isDisableHeartbeats) {
        _common = aCommon;
        _disableHeartbeats = isDisableHeartbeats;
    }

    /**
     * @throws org.dancres.paxos.InactiveException if the Paxos process is currently out of date or shutting down
     *
     * We stop allowing leaders in this process so as to avoid breaching the constraint where we can be sure we've
     * recorded an outcome at least locally.
     *
     * @todo Allow concurrent leaders - this would also require modifications to the out-of-date detection used in
     * AcceptorLearner.
     *
     * @return a leader for a new sequence
     */
    Leader newLeader() throws InactiveException {
        if ((_common.testState(Constants.FSMStates.SHUTDOWN)) ||
                (_common.testState(Constants.FSMStates.OUT_OF_DATE)))
            throw new InactiveException();

        synchronized(this) {
            killHeartbeats();

            return newLeaderImpl();
        }
    }

    private void killHeartbeats() {
        if (_heartbeatAlarm != null) {
            _heartbeatAlarm.cancel();
            _heartbeatAlarm = null;
            _common.getWatchdog().purge();
        }
    }

    private Leader newLeaderImpl() {
        if (_currentLeader == null) {
            _stateFactory = new ProposalAllocator(_common.getLowWatermark().getSeqNum(), _common.getLeaderRndNum());

            if (! _disableHeartbeats)
                _stateFactory.add(this);

            _currentLeader = new Leader(_common, _stateFactory);

        } else {
            CompletionImpl<Leader> myResult = new CompletionImpl<Leader>();

            _currentLeader.nextLeader(myResult);
            _currentLeader = myResult.await();
        }

        return _currentLeader;
    }

    public void inFlight() {
        killHeartbeats();
    }

    /**
     *
     * @todo Increment round number via heartbeats every so often to avoid jittering collects
     */
    public void allConcluded() {
        if (_stateFactory.amLeader()) {
            // Still leader so heartbeat
            //
            _heartbeatAlarm = new TimerTask() {
                public void run() {
                    _logger.info(this + ": sending heartbeat: " + System.currentTimeMillis());

                    newLeaderImpl().submit(HEARTBEAT, new Completion<VoteOutcome>() {
                        public void complete(VoteOutcome anOutcome) {
                            // Do nothing
                        }
                    });
                }
            };

            _common.getWatchdog().schedule(_heartbeatAlarm, calculateLeaderRefresh());
        }
    }

    private long calculateLeaderRefresh() {
        long myExpiry = Constants.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 10 / 100);
    }

    public void shutdown() {
        synchronized(this) {
            killHeartbeats();

            if (_currentLeader != null)
                _currentLeader.shutdown();
        }
    }

    public boolean accepts(Transport.Packet aPacket) {
        return aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.LEADER);
    }

    public void processMessage(Transport.Packet aPacket) {
        _logger.debug("Got packet for leaders: " + aPacket.getSource() + "->" + aPacket.getMessage());
        
        synchronized(this) {
            _logger.debug("Routing packet to leader " + _currentLeader);

            if (_currentLeader != null)
                _currentLeader.processMessage(aPacket);
        }
    }
}

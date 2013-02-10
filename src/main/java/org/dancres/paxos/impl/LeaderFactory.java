package org.dancres.paxos.impl;

import org.dancres.paxos.Paxos;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
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
public class LeaderFactory {
    private static final Logger _logger = LoggerFactory.getLogger(LeaderFactory.class);

    public static final Proposal HEARTBEAT = new Proposal("heartbeat",
            "org.dancres.paxos.Heartbeat".getBytes());
    
    private final Common _common;
    private Leader _currentLeader;

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private TimerTask _heartbeatAlarm;

    LeaderFactory(Common aCommon) {
        _common = aCommon;

        _common.add(new Paxos.Listener() {
            public void done(VoteOutcome anEvent) {
                if (anEvent.getResult() == VoteOutcome.Reason.OUT_OF_DATE) {
                    synchronized (this) {
                        killHeartbeats();
                    }
                }
            }
        });
    }

    /**
     * @throws org.dancres.paxos.Paxos.InactiveException if the Paxos process is currently out of date or shutting down
     *
     * We stop allowing leaders in this process so as to avoid breaching the constraint where we can be sure we've
     * recorded an outcome at least locally.
     *
     * @return a leader for a new sequence
     */
    Leader newLeader() throws Paxos.InactiveException {
        synchronized (this) {
            while (isActive()) {
                try { 
                    wait();
                } catch (InterruptedException anIE) {}
            }

            killHeartbeats();

            if ((_common.testState(Constants.FSMStates.SHUTDOWN)) ||
                    (_common.testState(Constants.FSMStates.OUT_OF_DATE)))
                throw new Paxos.InactiveException();

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

    private boolean isActive() {
        return ((_currentLeader != null) && (! _currentLeader.isDone()));
    }

    /*
     * We currently infer multi-paxos by lining up AL seq num with our own. We should
     * ideally infer it from the direct result of the previous leader. Similarly for
     * sequence number to use, round number etc.
     *
     * Currently we try and make this work by not constructing a leader until the
     * previous leader has finished one way or another. A better design would be
     * to pass into the next leader it's previous leader and treat that as a state
     * input for the next leader. The leader state machine would wait on that
     * state input stabilising and, when it happens, proceed accordingly
     * (setup for multi-paxos, choose a sequence number etc).
     *
     * Open challenge to solve with this approach is what to do when there is no
     * previous leader. We'd need to construct an initial fake leader that can
     * trigger no multi-paxos, a sequence number from the AL etc.
     *
     * Another challenge is how to signal/trigger the chained leader to proceed.
     * It might be done by delivering a new message of the form (PRIOR_STATE) into
     * the next leader's core message processing loop.
     *
     * Another challenge is heartbeating with this construct. Perhaps we chain
     * a fake leader to receive the PRIOR_STATE and add the next leader to that
     * creating a chain of real->fake->real->fake->real->fake. If a fake detects
     * it is last in the queue it holds the state and uses it to determine if
     * heartbeats should run. When the next real leader is chained onto it,
     * heartbeats would be stopped and the held state is passed over to the next
     * real leader.
     */
    private Leader newLeaderImpl() {
        if (_currentLeader == null)
            _currentLeader = new Leader(_common, this);
        else
            _currentLeader = _currentLeader.nextLeader();

        return _currentLeader;
    }

    /**
     *
     * @todo Increment round number via heartbeats every so often to avoid jittering collects
     *
     * @param aLeader
     */
    void dispose(Leader aLeader) {
        // If there are no leaders and the last one exited cleanly, do heartbeats
        //
        synchronized (this) {
            switch (_currentLeader.getOutcome().getResult()) {
                case VoteOutcome.Reason.OTHER_VALUE :
                case VoteOutcome.Reason.DECISION : {
                    // Still leader so heartbeat
                    //
                    _heartbeatAlarm = new TimerTask() {
                        public void run() {
                            _logger.info(this + ": sending heartbeat: " + System.currentTimeMillis());

                            newLeaderImpl().submit(HEARTBEAT);
                        }
                    };

                    _common.getWatchdog().schedule(_heartbeatAlarm, calculateLeaderRefresh());
                }

                default : {
                    // Not leader, nothing to do
                    //
                    break;
                }
            }
            
            notify();
        }
    }

    private long calculateLeaderRefresh() {
        long myExpiry = Constants.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 10 / 100);
    }

    public void shutdown() {
        synchronized (this) {
            killHeartbeats();

            if (_currentLeader != null)
                _currentLeader.shutdown();
        }
    }

    void messageReceived(Transport.Packet aPacket) {
        _logger.debug("Got packet for leaders: " + aPacket.getSource() + "->" + aPacket.getMessage());
        
        synchronized(this) {
            _logger.debug("Routing packet to leader " + _currentLeader);

            if (_currentLeader != null)
                _currentLeader.messageReceived(aPacket);
        }
    }
}

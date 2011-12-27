package org.dancres.paxos.impl;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LeaderFactory {
    private static final Logger _logger = LoggerFactory.getLogger(LeaderFactory.class);

    public static final Proposal HEARTBEAT = new Proposal("heartbeat",
            "org.dancres.paxos.Heartbeat".getBytes());
    
    private final Timer _watchdog = new Timer("Leader timers");
    private final NavigableMap<Long, Leader> _leaders = new TreeMap<Long, Leader>();
    private final Common _common;

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private TimerTask _heartbeatAlarm;
    
    LeaderFactory(Common aCommon) {
        _common = aCommon;
    }

    /**
     * @return a leader for a new sequence
     */
    Leader newLeader() {
        synchronized (this) {
            if (_heartbeatAlarm != null) {
                _heartbeatAlarm.cancel();
                _heartbeatAlarm = null;
                _watchdog.purge();
            }

            return newLeaderImpl();
        }
    }

    private Leader newLeaderImpl() {
        long mySeqNum = _common.getRecoveryTrigger().getLowWatermark().getSeqNum() + 1;
        long myRndNum = _common.getLastCollect().getRndNumber() + 1;
        Leader.States myState = Leader.States.COLLECT;
        Leader myLeader;

        if (_leaders.size() > 0) {
            Leader myLast = _leaders.lastEntry().getValue();
            VoteOutcome myOutcome = myLast.getOutcome();

            /*
            * Seq & round allocation:
            *
            * If there are active leaders, pick last (i.e. biggest seqNum) and use its seqNum + 1 and its
            * rndNumber
            * elseif last outcome was positive, compare with AL seqNum. If AL wins, use its rndNum + 1 and
            * its seqNum + 1 else use outcome's rndNum and seqNum + 1 (and apply multi-paxos).
            * elseif last outcome was negative and state was other leader proceed with this outcome as per the other
            * outcome case above (but with the proviso that if AL doesn't win, we use outcome's rndNum + 1)
            * else use AL round number + 1 and seqNum + 1
            */
            if (myOutcome != null) {
                /*
                 * Last leader has resolved - account for mySeqNum being our ideal next sequence number not current
                 * (hence myOutcome.getSeqNum() + 1
                 */
                switch(myOutcome.getResult()) {
                    case VoteOutcome.Reason.DECISION :
                    case VoteOutcome.Reason.OTHER_VALUE : {

                        if (mySeqNum <= (myOutcome.getSeqNum() + 1)) {
                            // Apply multi-paxos
                            //
                            myState = Leader.States.BEGIN;
                            mySeqNum = myOutcome.getSeqNum() + 1;
                            myRndNum = myOutcome.getRndNumber();
                        }

                        break;
                    }

                    case VoteOutcome.Reason.OTHER_LEADER : {
                        if (mySeqNum <= (myOutcome.getSeqNum() + 1)) {
                            mySeqNum = myOutcome.getSeqNum() + 1;
                            myRndNum = myOutcome.getRndNumber() + 1;
                        }

                        break;
                    }
                }
            } else {
                // Last leader is still active
                //
                mySeqNum = myLast.getSeqNum() + 1;
                myRndNum = myLast.getRound();
            }
        }

        myLeader = new Leader(_common, _watchdog, this, mySeqNum, myRndNum, myState);

        // If performing equals on sequence numbers and the allocation thereof is correct...
        //
        assert(_leaders.containsKey(new Long(mySeqNum)) == false);

        _leaders.put(new Long(mySeqNum), myLeader);

        return myLeader;
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
            if (_leaders.size() > 1) {
                Long myLast = _leaders.lastKey();
                Iterator<Long> allKeys = _leaders.keySet().iterator();
                
                while (allKeys.hasNext()) {
                    Long k = allKeys.next();
                    if ((! k.equals(myLast)) && (_leaders.get(k).getOutcome() != null))
                        allKeys.remove();
                }                
            }

            if (_leaders.size() == 1) {
                switch (_leaders.lastEntry().getValue().getOutcome().getResult()) {
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

                        _watchdog.schedule(_heartbeatAlarm, calculateLeaderRefresh());
                    }

                    default : {
                        // Not leader, nothing to do
                        //
                        break;
                    }
                }
            }
        }
    }

    private long calculateLeaderRefresh() {
        long myExpiry = Constants.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 20 / 100);
    }

    public void shutdown() {
        synchronized (this) {
            _watchdog.cancel();

            Iterator<Map.Entry<Long, Leader>> all = _leaders.entrySet().iterator();

            while (all.hasNext()) {
                Map.Entry<Long, Leader> myCurrent = all.next();
                myCurrent.getValue().shutdown();
                all.remove();
            }
        }
    }

    void messageReceived(PaxosMessage aMessage) {
        _logger.info("Got packet for leaders: " + aMessage);
        
        synchronized(this) {
            _logger.info("Routing packet to " + _leaders.size() + " leaders");

            for (Leader aLeader : new LinkedList<Leader>(_leaders.values())) {
                aLeader.messageReceived(aMessage);
            }
        }
    }
}

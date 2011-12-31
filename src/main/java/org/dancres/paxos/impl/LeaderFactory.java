package org.dancres.paxos.impl;

import org.dancres.paxos.Paxos;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Each paxos instance is driven and represented by an individual instance of <code>Leader</code>.
 * These are created, tracked and driven by this factory class. The factory also looks after handling error outcomes
 * that require an adjustment in round or sequence number and heartbeating.
 *
 * Leader selection is a client-based element (or at least outside of the library). It is up to the client to decide
 * which leader to go to and it may be re-directed should that leader be aware of another leader. A failing leader
 * provides no guidance in respect of other leadership.
 *
 * Paxos needs only to be used to make changes to state. Read operations need not be passed through Paxos but they
 * do need to be dispatched to a node that is up-to-date. The node that is pretty much guaranteed to be up to date
 * is the existing leader. Other nodes could be used to support read operations so long as a level of staleness is
 * acceptable. If the state is timestamped in some fashion, once can use those timestamps to ensure that updates
 * based on the state are applied to the latest version (by checking its timestamp) or rejected.
 *
 * Rough API:
 *
 * newLeader() can be hidden behind Core.submit or similar and should throw an OutOfDateException whilst the local AL
 * is out of date and until it declares itself up to date. This is necessary because client's cannot see responses
 * because the AL will not produce events until it's back up to date. Note: We may want Common.signal to account for
 * this to.
 *
 * A server using this library needs to handle out of date. It also needs to handle other leader.
 * In all cases it typically passes a message to it's client to request a switch of leader. It can use the meta data
 * per Paxos node to hold the server contact details (ip, port etc). In response to other leader it would pull the
 * relevant meta data and re-direct the client. In the case of out of date, it would use some "well known" policy to
 * determine an alternate leader, pull the meta data and re-direct the client. Note it may need to send "no leader" to
 * a client if it cannot identify a candidate. All other types of error result in dispatching a fail to the client for
 * the request it submitted. Should a request succeed, it is server dependent what action is taken. It could be to
 * execute a command or change a value etc.
 *
 * In the case of out of date the server should also use contact details
 * for some other node to obtain a checkpoint with which it will update the AL. Up to date might, in tandem with out
 * of date allow the library to avoid calling newLeader and handle an exception. Note that the installation of a
 * new checkpoint can immediately generate an up to date followed shortly after by an out of date which will trigger
 * another attempt to recover in the server. This ensures the server's recovery approach can be relatively simple.
 * We may want to allow comparison and serialization of checkpoint handles so they can be used in the process of
 * obtaining a checkpoint to ensure what's being downloaded and installed is more up to date than what we have
 * locally.
 */
public class LeaderFactory {
    private static final Logger _logger = LoggerFactory.getLogger(LeaderFactory.class);

    public static final Proposal HEARTBEAT = new Proposal("heartbeat",
            "org.dancres.paxos.Heartbeat".getBytes());
    
    private final NavigableMap<Long, Leader> _leaders = new TreeMap<Long, Leader>();
    private final Common _common;

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private TimerTask _heartbeatAlarm;

    private void killHeartbeats() {
        if (_heartbeatAlarm != null) {
            _heartbeatAlarm.cancel();
            _heartbeatAlarm = null;
            _common.getWatchdog().purge();
        }
    }

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
     * @return a leader for a new sequence
     */
    Leader newLeader() {
        synchronized (this) {
            killHeartbeats();

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
            *
            * When we're considering past leaders outcomes, we only consider those for which we have some meaningful
            * information. Thus vote timeouts, membership problems etc provide no useful feedback re: accuracy of
            * sequence numbers or rounds whilst decisions, other values and other leaders (which includes feedback
            * about round and sequence) do.
            *
            * AL knowledge always takes priority if it's more recent than what our last informative leader action
            * discloses. If our AL has more recent knowledge, it implies leader activity in another process.
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

        myLeader = new Leader(_common, this, mySeqNum, myRndNum, myState);

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

                        _common.getWatchdog().schedule(_heartbeatAlarm, calculateLeaderRefresh());
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
            Iterator<Map.Entry<Long, Leader>> all = _leaders.entrySet().iterator();

            while (all.hasNext()) {
                Map.Entry<Long, Leader> myCurrent = all.next();
                myCurrent.getValue().shutdown();
                all.remove();
            }
        }
    }

    void messageReceived(PaxosMessage aMessage) {
        _logger.debug("Got packet for leaders: " + aMessage);
        
        synchronized(this) {
            _logger.debug("Routing packet to " + _leaders.size() + " leaders");

            for (Leader aLeader : new LinkedList<Leader>(_leaders.values())) {
                aLeader.messageReceived(aMessage);
            }
        }
    }
}

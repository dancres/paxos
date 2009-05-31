package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.Membership;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArraySet;

import java.util.*;
import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.NodeId;

/**
 * A simple failure detector driven by reception of {@link Heartbeat} messages generated by {@link Heartbeater}.
 * This implementation expects the transport to present all received messages to the detector via {@link processMessage}
 *
 * @todo Ultimately this detector could be further enhanced by using messages generated as part of standard Paxos interactions
 * to determine liveness.  The Heartbeater would then be modified to generate a message only if there had been an absence of
 * other messages sent by a node for a suitable period of time.
 */
public class FailureDetectorImpl implements FailureDetector, Runnable {
    private Map<NodeId, Long> _lastHeartbeats = new HashMap<NodeId, Long>();
    private ExecutorService _executor = Executors.newFixedThreadPool(1);
    private Thread _scanner;
    private CopyOnWriteArraySet _listeners;
    private long _maximumPeriodOfUnresponsiveness;

    private Logger _logger = LoggerFactory.getLogger(FailureDetectorImpl.class);

    /**
     * @param anUnresponsivenessThreshold is the maximum period a node may "dark" before being declared failed.
     */
    public FailureDetectorImpl(long anUnresponsivenessThreshold) {
        _scanner = new Thread(this);
        _scanner.setDaemon(true);
        _scanner.start();
        _listeners = new CopyOnWriteArraySet();
        _maximumPeriodOfUnresponsiveness = anUnresponsivenessThreshold;
    }

    public void add(LivenessListener aListener) {
        _listeners.add(aListener);
    }

    public long getUnresponsivenessThreshold() {
        return _maximumPeriodOfUnresponsiveness;
    }

    public void remove(LivenessListener aListener) {
        _listeners.remove(aListener);
    }

    /**
     * Examine a received {@link PaxosMessage} and update liveness information as appropriate.
     *
     * @todo Modify this to use any packet for liveness and arrange for clients to heartbeat if they don't send
     * any other packet within the heartbeat period.
     */
    public void processMessage(PaxosMessage aMessage, NodeId aNodeId) throws Exception {
        if (aMessage.getType() == Heartbeat.TYPE) {
            Long myLast;

            synchronized (this) {
                myLast = _lastHeartbeats.put(aNodeId,
                        new Long(System.currentTimeMillis()));
            }

            if (myLast == null)
                _executor.submit(new AliveTask(aNodeId, _listeners));
        }
    }

    /**
     * Currently a simple majority test - ultimately we only need one member of the previous majority to be present
     * in this majority for Paxos to work.
     * 
     * @return true if at this point, available membership would allow for a majority
     */
    public boolean couldComplete() {
        synchronized(this) {
            return MembershipImpl.haveMajority(_lastHeartbeats.size());
        }
    }

    public Membership getMembers(MembershipListener aListener) {
        MembershipImpl myMembership = new MembershipImpl(this, aListener);
        _listeners.add(myMembership);

        Set myActives = new HashSet();

        synchronized(this) {
            _logger.info("Snapping failure detector members");

            myActives.addAll(_lastHeartbeats.keySet());

            _logger.info("Snapping failure detector members - done");
        }
        
        myMembership.populate(myActives);
        return myMembership;
    }

    public void run() {
        while(true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                continue;
            }

            synchronized(this) {
                Iterator<NodeId> myProcesses = _lastHeartbeats.keySet().iterator();
                long myMinTime = System.currentTimeMillis() - _maximumPeriodOfUnresponsiveness;

                while (myProcesses.hasNext()) {
                    NodeId myAddress = myProcesses.next();
                    Long myTimeout = _lastHeartbeats.get(myAddress);

                    // No heartbeat since myMinTime means we assume dead
                    //
                    if (myTimeout < myMinTime) {
                        myProcesses.remove();
                        sendDead(myAddress);
                    }
                }
            }
        }
    }

    private void sendDead(NodeId aProcess) {
        Iterator myListeners = _listeners.iterator();
        while (myListeners.hasNext()) {
            LivenessListener myListener = (LivenessListener) myListeners.next();
            myListener.dead(aProcess);
        }
    }
}

package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArraySet;

import java.util.*;

import java.net.SocketAddress;

public class FailureDetector implements Runnable {
    private Map _lastHeartbeats = new HashMap();
    private ExecutorService _executor = Executors.newFixedThreadPool(1);
    private Thread _scanner;
    private CopyOnWriteArraySet _listeners;

    private Logger _logger = LoggerFactory.getLogger(FailureDetector.class);

    public FailureDetector() {
        _scanner = new Thread(this);
        _scanner.setDaemon(true);
        _scanner.start();
        _listeners = new CopyOnWriteArraySet();
    }

    public void add(LivenessListener aListener) {
        _listeners.add(aListener);
    }

    public void remove(LivenessListener aListener) {
        _listeners.remove(aListener);
    }

    /**
     * @todo Modify this to use any packet for liveness and arrange for clients to heartbeat if they don't send
     * any other packet within the heartbeat period.
     */
    public void processMessage(PaxosMessage aMessage, SocketAddress anAddress) throws Exception {
        if (aMessage.getType() == Operations.HEARTBEAT) {
            Long myLast;

            synchronized (this) {
                myLast = (Long) _lastHeartbeats.put(anAddress,
                        new Long(System.currentTimeMillis()));
            }

            if (myLast == null)
                _executor.submit(new AliveTask(anAddress, _listeners));
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
                Iterator myProcesses = _lastHeartbeats.keySet().iterator();
                long myMinTime = System.currentTimeMillis() - 5000;

                while (myProcesses.hasNext()) {
                    SocketAddress myAddress = (SocketAddress) myProcesses.next();
                    Long myTimeout = (Long) _lastHeartbeats.get(myAddress);

                    // No heartbeat since myMinTime means we assume dead
                    if (myTimeout < myMinTime) {
                        myProcesses.remove();
                        sendDead(myAddress);
                    }
                }
            }
        }
    }

    private void sendDead(SocketAddress aProcess) {
        Iterator myListeners = _listeners.iterator();
        while (myListeners.hasNext()) {
            LivenessListener myListener = (LivenessListener) myListeners.next();
            myListener.dead(aProcess);
        }
    }
}

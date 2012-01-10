package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.Membership;
import org.dancres.paxos.impl.MembershipListener;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CopyOnWriteArraySet;

import java.util.*;

/**
 * A simple failure detector driven by reception of {@link Heartbeat} messages generated by {@link Heartbeater}.
 * This implementation expects the transport to present all received messages to the detector via <code>processMessage</code>
 *
 * @todo Ultimately this detector could be further enhanced by using messages generated as part of standard Paxos interactions
 * to determine liveness.  The Heartbeater would then be modified to generate a message only if there had been an absence of
 * other messages sent by a node for a suitable period of time.
 */
public class FailureDetectorImpl implements MessageBasedFailureDetector, Runnable {
    /**
     * @todo Fix up this majority to be more dynamic
     */
    private static final int DEFAULT_MAJORITY = 2;

    private Random _random = new Random();
    private Map<InetSocketAddress, MetaDataImpl> _lastHeartbeats = new HashMap<InetSocketAddress, MetaDataImpl>();
    private ExecutorService _executor = Executors.newFixedThreadPool(1);
    private Thread _scanner;
    private CopyOnWriteArraySet<MembershipImpl> _listeners;
    private long _maximumPeriodOfUnresponsiveness;
    private boolean _stopping;
    private int _majority;

    class MetaDataImpl implements FailureDetector.MetaData {
        public long _timestamp;
        public byte[] _metaData;

        MetaDataImpl(long aTimestamp, byte[] aMeta) {
            _timestamp = aTimestamp;
            _metaData = aMeta;
        }

        public byte[] getData() {
            return _metaData;
        }

        public long getTimestamp() {
            return _timestamp;
        }
    }

    private Logger _logger = LoggerFactory.getLogger(FailureDetectorImpl.class);

    /**
     * @param anUnresponsivenessThreshold is the maximum period a node may "dark" before being declared failed.
     */
    private FailureDetectorImpl(int aMajority, long anUnresponsivenessThreshold) {
        _majority = aMajority;
        _scanner = new Thread(this);
        _scanner.setDaemon(true);
        _scanner.start();
        _listeners = new CopyOnWriteArraySet<MembershipImpl>();
        _maximumPeriodOfUnresponsiveness = anUnresponsivenessThreshold;
    }

    public FailureDetectorImpl(long anUnresponsivenessThreshold) {
        this(DEFAULT_MAJORITY, anUnresponsivenessThreshold);
    }

    public void stop() {
    	synchronized(this) {
    		_stopping = true;
    	}

    	try {
    		_scanner.join();
    	} catch (InterruptedException anIE) {    		
    	}
    	
    	_executor.shutdownNow();
    }
    
    private boolean isStopping() {
    	synchronized(this) {
    		return _stopping;
    	}
    }
    
    public void run() {
        while(! isStopping()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                continue;
            }

            synchronized(this) {
                Iterator<InetSocketAddress> myProcesses = _lastHeartbeats.keySet().iterator();
                long myMinTime = System.currentTimeMillis() - _maximumPeriodOfUnresponsiveness;

                while (myProcesses.hasNext()) {
                    InetSocketAddress myAddress = myProcesses.next();
                    long myTimeout = _lastHeartbeats.get(myAddress)._timestamp;

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

    /**
     * Examine a received {@link PaxosMessage} and update liveness information as appropriate.
     */
    public void processMessage(PaxosMessage aMessage) throws Exception {
        if (aMessage.getType() == Operations.HEARTBEAT) {
            MetaDataImpl myLast;

            final Heartbeat myHeartbeat = (Heartbeat) aMessage;
            final InetSocketAddress myNodeId = myHeartbeat.getNodeId();
            
            synchronized (this) {
                myLast = _lastHeartbeats.get(myHeartbeat.getNodeId());

                if (myLast == null) {
                    _lastHeartbeats.put(myHeartbeat.getNodeId(),
                            new MetaDataImpl(System.currentTimeMillis(), myHeartbeat.getMetaData()));
                } else
                    myLast._timestamp = System.currentTimeMillis();
            }

            if (myLast == null)
                _executor.submit(
                        new Runnable() {
                            public void run() {
                                for (MembershipImpl myListener : _listeners)
                                    myListener.alive(myNodeId);
                            }
                        });
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
            return isMajority(_lastHeartbeats.size());
        }
    }

    private boolean isMajority(int aSize) {
        return (aSize >= _majority);
    }

    public int getMajority() {
        return _majority;
    }

    public Map<InetSocketAddress, MetaData> getMemberMap() {
        Map myActives;

        synchronized (this) {
            myActives = new HashMap<InetSocketAddress, MetaData>(_lastHeartbeats);
        }

        return myActives;
    }

    public Membership getMembers(MembershipListener aListener) {
        MembershipImpl myMembership = new MembershipImpl(aListener);
        _listeners.add(myMembership);

        Set myActives = new HashSet();

        synchronized(this) {
            _logger.debug("Snapping failure detector members");

            myActives.addAll(_lastHeartbeats.keySet());

            _logger.debug("Snapping failure detector members - done");
        }
        
        myMembership.populate(myActives);
        return myMembership;
    }

    public InetSocketAddress getRandomMember(InetSocketAddress aLocalAddress) {
        LinkedList<InetSocketAddress> myMembers = new LinkedList<InetSocketAddress>();

        synchronized(this) {
            myMembers.addAll(_lastHeartbeats.keySet());
        }

        myMembers.remove(aLocalAddress);
        return myMembers.get(_random.nextInt(myMembers.size()));
    }

    private void sendDead(InetSocketAddress aProcess) {
        for (MembershipImpl myListener : _listeners)
            myListener.dead(aProcess);
    }

    /**
     * A snapshot of the membership at some point in time, updated by the <code>FailureDetectorImpl</code> over time.  Note the snapshot only
     * reduces in size, it cannot grow so as to allow correct behaviour in cases where majorities are required.
     *
     * @author dan
     */
    class MembershipImpl implements Membership {
        /**
         * Tracks the membership that forms the base for each round
         */
        private Set<InetSocketAddress> _initialMemberAddresses = new HashSet<InetSocketAddress>();

        /**
         * Tracks the members that have yet to respond in a round
         */
        private Set<InetSocketAddress> _outstandingMemberAddresses;

        private boolean _populated = false;
        private MembershipListener _listener;

        private int _expectedResponses;
        private int _receivedResponses;

        private boolean _disposed = false;

        MembershipImpl(MembershipListener aListener) {
            _listener = aListener;
        }

        public boolean startInteraction() {
            synchronized(this) {
                if (!abort()) {
                    _receivedResponses = 0;
                    _expectedResponses = _initialMemberAddresses.size();
                    _outstandingMemberAddresses = new HashSet(_initialMemberAddresses);
                    return true;
                } else {
                    return false;
                }
            }
        }

        public boolean receivedResponse(InetSocketAddress anAddress) {
            synchronized(this) {
                if (_outstandingMemberAddresses.remove(anAddress)) {
                    ++_receivedResponses;
                    interactionComplete();
                    return true;
                } else {
                    _logger.warn("Not an expected response: " + anAddress);
                    return false;
                }
            }
        }

        public void alive(InetSocketAddress aProcess) {
            // Not interested in new arrivals
        }

        public void dead(InetSocketAddress aProcess) {
            _logger.warn("Death detected: " + aProcess);

            synchronized(this) {
                // Delay messages until we've got a member set
                while (! _populated) {
                    try {
                        wait();
                    } catch (InterruptedException anIE) {
                    }
                }

                _outstandingMemberAddresses.remove(aProcess);
                _initialMemberAddresses.remove(aProcess);

                // startInteraction will reset this so if we get a dead before then, it should be recorded
                //
                --_expectedResponses;

                if (abort())
                    return;

                interactionComplete();
            }
        }

        void populate(Set<InetSocketAddress> anActiveAddresses) {
            _logger.debug("Populating membership");

            synchronized(this) {
                _logger.debug("Populating membership - got lock");

                _initialMemberAddresses.addAll(anActiveAddresses);

                _logger.debug("Populating membership - addresses added");

                _populated = true;

                // Now we have a member set, accept updates
                notifyAll();
            }
        }

        public int getSize() {
            synchronized(this) {
                return _initialMemberAddresses.size();
            }
        }

        public void dispose() {
            _logger.debug("Membership disposed");

            _listeners.remove(this);

            synchronized(this) {
                _disposed = true;
            }
        }

        private boolean interactionComplete() {
            if ((_receivedResponses == _expectedResponses) || (_receivedResponses >= getMajority())) {
                _listener.allReceived();
                return true;
            }

            return false;
        }

        private boolean abort() {
            if (_initialMemberAddresses.size() < getMajority()) {
                _listener.abort();
                return true;
            }

            return false;
        }

        protected void finalize() throws Throwable {
            synchronized(this) {
                if (_disposed)
                    return;
            }

            System.err.println("Membership was not disposed");
            System.err.flush();
        }
    }    
}

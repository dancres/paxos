package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Membership;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.util.AbstractFuture;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple failure detector driven by reception of {@link Heartbeat} messages generated by {@link Heartbeater}.
 * This implementation expects the transport to present all received messages to the detector via <code>processMessage</code>
 *
 * @todo Ultimately this detector could be further enhanced by using messages generated as part of standard Paxos interactions
 * to determine liveness.  The Heartbeater would then be modified to generate a message only if there had been an absence of
 * other messages sent by a node for a suitable period of time.
 */
public class FailureDetectorImpl extends MessageBasedFailureDetector {
    /**
     * @todo Fix up this cluster size to be more dynamic
     */
    private static final int DEFAULT_CLUSTER_SIZE = 3;

    private final Queue<FutureImpl> _futures = new LinkedBlockingQueue<>();
    private final Random _random = new Random();
    private final ConcurrentMap<InetSocketAddress, MetaDataImpl> _lastHeartbeats =
            new ConcurrentHashMap<>();
    private final Timer _tasks = new Timer();
    private final long _maximumPeriodOfUnresponsiveness;
    private final AtomicBoolean _stopping = new AtomicBoolean(false);
    private final int _majority;

    private class MetaDataImpl implements FailureDetector.MetaData {
        final long _timestamp;
        final byte[] _metaData;

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

    /**
     * @param aClusterSize is the number of members in the cluster
     * @param anUnresponsivenessThreshold is the maximum period a node may "dark" before being declared failed.
     */
    public FailureDetectorImpl(int aClusterSize, long anUnresponsivenessThreshold) {
        _majority = calculateMajority(aClusterSize);
        _maximumPeriodOfUnresponsiveness = anUnresponsivenessThreshold;
        _tasks.schedule(new ScanImpl(), 0, (_maximumPeriodOfUnresponsiveness / 5));
    }

    private int calculateMajority(int aClusterSize) {
        if ((aClusterSize % 2) != 1)
            throw new IllegalArgumentException("Cluster size must be an odd number");

        return (aClusterSize / 2) + 1;
    }

    /**
     * Assumes a three-node cluster in which a majority of 2 is sufficient for progress
     *
     * @param anUnresponsivenessThreshold
     */
    public FailureDetectorImpl(long anUnresponsivenessThreshold) {
        this(DEFAULT_CLUSTER_SIZE, anUnresponsivenessThreshold);
    }

    public void stop() {
        _stopping.set(true);
        _tasks.cancel();
    }
    
    public Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData) {
        // We want at least three heartbeats within the unresponsiveness period
        //
        return new HeartbeaterImpl(aTransport, aMetaData, (_maximumPeriodOfUnresponsiveness / 3) - 100);
    }   

    private class ScanImpl extends TimerTask {
        public void run() {
            Iterator<InetSocketAddress> myProcesses = _lastHeartbeats.keySet().iterator();
            long myMinTime = System.currentTimeMillis() - _maximumPeriodOfUnresponsiveness;

            while (myProcesses.hasNext()) {
                InetSocketAddress myAddress = myProcesses.next();
                long myTimeout = _lastHeartbeats.get(myAddress)._timestamp;

                // No heartbeat since myMinTime means we assume dead
                //
                if (myTimeout < myMinTime) {
                    myProcesses.remove();
                }
            }
        }
    }

    public boolean accepts(Packet aPacket) {
        return aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.FAILURE_DETECTOR);
    }

    /**
     * Examine a received {@link PaxosMessage} and update liveness information as appropriate.
     */
    public void processMessage(Packet aPacket) {
        PaxosMessage myMessage = aPacket.getMessage();

        if (myMessage.getType() == Operations.HEARTBEAT) {
            MetaDataImpl myLast;

            final Heartbeat myHeartbeat = (Heartbeat) myMessage;
            final InetSocketAddress myNodeId = aPacket.getSource();

            for (;;) {
                myLast = _lastHeartbeats.get(myNodeId);

                if (myLast == null) {
                    if (_lastHeartbeats.putIfAbsent(myNodeId,
                            new MetaDataImpl(System.currentTimeMillis(), myHeartbeat.getMetaData())) == null)
                        break;
                } else {
                    if (_lastHeartbeats.replace(myNodeId, myLast, new MetaDataImpl(System.currentTimeMillis(),
                            myHeartbeat.getMetaData())))
                    break;
                }
            }

            if ((_futures.size() != 0) && (_lastHeartbeats.size() >= _majority)) {
                while (true) {
                    FutureImpl myFuture = _futures.poll();

                    if (myFuture == null)
                        return;

                    myFuture.set(true);
                }
            }
        }
    }

    private class FutureImpl extends AbstractFuture<Boolean> {
        private final Queue _queue;

        FutureImpl(Queue aQueue) {
            _queue = aQueue;
        }

        protected void done() {
            _queue.remove();
        }
    }

    public Future<Boolean> barrier() {
        FutureImpl myFuture = new FutureImpl(_futures);
        _futures.add(myFuture);

        return myFuture;
    }

    /**
     * Currently a simple majority test - ultimately we only need one member of the previous majority to be present
     * in this majority for Paxos to work.
     * 
     * @return true if at this point, available membership would allow for a majority
     */
    public boolean couldComplete() {
        return isMajority(_lastHeartbeats.size());
    }

    private boolean isMajority(int aSize) {
        return (aSize >= _majority);
    }

    public int getMajority() {
        return _majority;
    }

    public Membership getMembers() {
        return new MembershipImpl(new HashMap<InetSocketAddress, MetaData>(_lastHeartbeats));
    }

    public InetSocketAddress getRandomMember(InetSocketAddress aLocalAddress) {
        LinkedList<InetSocketAddress> myMembers = new LinkedList<>(_lastHeartbeats.keySet());

        myMembers.remove(aLocalAddress);
        return myMembers.get(_random.nextInt(myMembers.size()));
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
        private final Map<InetSocketAddress, MetaData> _members;

        MembershipImpl(Map<InetSocketAddress, MetaData> anInitialAddresses) {
            _members = anInitialAddresses;
        }

        public Map<InetSocketAddress, MetaData> getMemberMap() {
            return _members;
        }

        public int getSize() {
            return _members.size();
        }

        public boolean couldComplete() {
            return (_members.size() >= _majority);
        }

        public boolean isMajority(Collection<InetSocketAddress> aListOfAddresses) {
            return ((_members.keySet().containsAll(aListOfAddresses)) &&
                    aListOfAddresses.size() >= _majority);
        }
    }
}

package org.dancres.paxos.test.longterm;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.OrderedMemoryNetwork;
import org.dancres.paxos.test.net.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class EnvironmentImpl implements Environment {
    private static final Logger _logger = LoggerFactory.getLogger(Main.class);
    private static final String BASEDIR = "/Volumes/LaCie/paxoslogs/";

    private final boolean _isLive;
    private final long _maxCycles;
    private final long _ckptCycle;
    private final Random _baseRng;
    private final OrderedMemoryNetwork _factory;
    private final OrderedMemoryNetwork.Factory _nodeFactory;
    private final AtomicLong _opsSinceCkpt = new AtomicLong(0);
    private final AtomicLong _opCount = new AtomicLong(0);
    private final AtomicBoolean _isSettling = new AtomicBoolean(false);
    private final AtomicBoolean _isReady = new AtomicBoolean(false);    
    private final NodeSet _nodeSet = new NodeSet();

    /**
     * @TODO The option for awaiting cluster formation needs to disable Permuter
     * @param aSeed
     * @param aCycles
     * @param doCalibrate
     * @param aCkptCycle
     * @param inMemory
     * @param allowClusterFormation
     * @throws Exception
     */
    EnvironmentImpl(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle,
                    boolean inMemory, boolean allowClusterFormation) throws Exception {
        _ckptCycle = aCkptCycle;
        _isLive = ! doCalibrate;
        _maxCycles = aCycles;
        _baseRng = new Random(aSeed);
        _factory = new OrderedMemoryNetwork(this);

        _nodeFactory = (InetSocketAddress aLocalAddr,
                InetSocketAddress aBroadcastAddr,
                OrderedMemoryNetwork aNetwork,
                MessageBasedFailureDetector anFD,
                Object aContext) -> {

            NodeAdminImpl myNode = new NodeAdminImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD,
                    (NodeAdminImpl.Config) aContext,
                    EnvironmentImpl.this);

            return new OrderedMemoryNetwork.Factory.Constructed(myNode.getTransport(), myNode);
        };

        Deque<NodeAdmin> myNodes = new LinkedList<>();

        for (int i = 0; i < 5; i++) {
            LogStorageFactory myFactory = (! inMemory) ? new HowlLoggerFactory(BASEDIR, i) :
                    new MemoryLoggerFactory();

            OrderedMemoryNetwork.Factory.Constructed myResult =
                    addNodeAdmin(Utils.getTestAddress(), new NodeAdminImpl.Config(i, myFactory));

            myNodes.add((NodeAdmin) myResult.getAdditional());
        }

        _nodeSet.init(myNodes);

        _logger.info("******* ALL NODES NOW LIVE ********");

        if (allowClusterFormation) {
            _logger.info("******* AWAITING CLUSTER FORMATION ********");

            for (NodeAdmin myN : myNodes) {
                _logger.debug("EnsureFD: " + myN.getTransport().getLocalAddress());
                FDUtil.testFD(myN.getTransport().getFD(), 20000, 5);
            }
        }

        _logger.info("******* COMMENCING RUN ********");

        _isReady.set(true);
    }

    private OrderedMemoryNetwork.Factory.Constructed addNodeAdmin(InetSocketAddress anAddress, NodeAdminImpl.Config aConfig) {
        return _factory.newTransport(_nodeFactory, new FailureDetectorImpl(5, 5000, FailureDetectorImpl.OPEN_PIN),
                anAddress, aConfig);
    }

    public void addNodeAdmin(NodeAdmin.Memento aMemento) {
        OrderedMemoryNetwork.Factory.Constructed myResult =
                addNodeAdmin(aMemento.getAddress(), (NodeAdminImpl.Config) aMemento.getContext());
        _nodeSet.install((NodeAdmin) myResult.getAdditional());
    }

    public long getSettleCycles() {
        return 100;
    }

    public OrderedMemoryNetwork getFactory() {
        return _factory;
    }

    public long getMaxCycles() {
        return _maxCycles;
    }

    public boolean isReady() {
        return _isReady.get();
    }

    public boolean isLive() {
        return _isLive;
    }

    public Random getRng() {
        return _baseRng;
    }

    /**
     * TODO: Allow killing of current leader
     */
    public Deque<NodeAdmin> getKillableNodes() {
        return _nodeSet.getKillableNodes();
    }

    public boolean validate() {
        return _nodeSet.validate();
    }

    public NodeAdmin getCurrentLeader() {
        return _nodeSet.getCurrentLeader();
    }

    public boolean isSettling() {
        return _isSettling.get();
    }

    public void settle() {
        _isSettling.set(true);
        _nodeSet.settle();
        stabilise();
    }

    public NodeAdmin.Memento killSpecific(NodeAdmin anAdmin) {
        _logger.info("Killing: " + anAdmin);

        return _nodeSet.terminate(anAdmin);
    }

    public void terminate() {
        _nodeSet.shutdown();
        _factory.stop();
    }

    public boolean makeCurrent(NodeAdmin anAdmin) {
        return _nodeSet.makeCurrent(anAdmin);
    }
    
    public void updateLeader(InetSocketAddress anAddr) {
        _nodeSet.updateLeader(anAddr);
    }

    public long getNextCkptOp() {
        return _opCount.get() - _opsSinceCkpt.get() + _ckptCycle;
    }

    public long getDoneOps() {
        return _opCount.get();
    }

    public void doneOp() {
        _opCount.incrementAndGet();

        long myCount = _opsSinceCkpt.incrementAndGet();

        if (myCount >= _ckptCycle) {
            _logger.info("Issuing checkpoint @ " + _opCount.get());

            _nodeSet.checkpointAll();

            _opsSinceCkpt.compareAndSet(myCount, 0);
        }
    }

    private void stabilise() {
        for (FailureDetector anFD : _nodeSet.getFDs()) {
            _logger.info("Stabilising on FD for: " + anFD);

            try {
                FDUtil.testFD(anFD);
            } catch (Exception anE) {
                _logger.info("Failed to stabilise: ", anE);
                throw new IllegalStateException("No stability");
            }
        }
    }
}


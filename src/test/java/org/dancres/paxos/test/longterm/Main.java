package org.dancres.paxos.test.longterm;

import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.test.net.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Need a statistical failure model with varying probabilities for each thing within a tolerance / order
 * For normalisation against a test duration, one year is 525600 minutes.
 * 
 * <ol>
 * <li>Hard disk failure - 6 to 10% </li>
 * <li>Node failure - 1 to 2% up to 15 minutes to recover</li>
 * <li>DC-wide network failure - 0 to 1% up to  5.26 minutes/year</li>
 * <li>Rack-level failure - 0 to 1% downtime up to 8.76 hours/year</li>
 * <li>Link-level failure - 0 to 1% up to 52.56 minutes/year</li>
 * <li>DC-wide packet loss - 1 to 2% with loss of 2 to 10's packets</li>
 * <li>Rack-level packet loss - 1 to 2% with loss of 2 to 10's packets</li>
 * <li>Link-level packet loss - 1 to 2% with loss of 2 to 10's packets</li>
 * </ol>
 *
 * Each of these will also have a typical time to fix. In essence we want an MTBF/MTRR model.
 *
 * @todo Implement failure model (might affect design for OUT_OF_DATE handling).
 */
public class Main {
    private static final Logger _logger = LoggerFactory.getLogger(Main.class);
    private static final String BASEDIR = "/Volumes/LaCie/paxoslogs/";

    static interface Args {
        @Option(defaultValue="100")
        long getCkptCycle();

        @Option(defaultValue="0")
        long getSeed();

        @Option(defaultValue="200")
        int getCycles();

        @Option(defaultValue="1")
        int getIterations();

        @Option
        boolean isCalibrate();

        @Option
        boolean isMemory();
    }

    private static class EnvironmentImpl implements Environment {
        final boolean _isStorage;
        final boolean _isLive;
        final long _maxCycles;
        final long _settleCycles = 100;
        final long _ckptCycle;
        final Random _baseRng;
        private final Deque<NodeAdmin> _nodes = new ConcurrentLinkedDeque<>();

        NodeAdmin _currentLeader;
        final OrderedMemoryNetwork _factory;
        final OrderedMemoryNetwork.Factory _nodeFactory;

        private AtomicLong _opsSinceCkpt = new AtomicLong(0);
        private AtomicLong _opCount = new AtomicLong(0);

        EnvironmentImpl(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle, boolean inMemory) throws Exception {
            _ckptCycle = aCkptCycle;
            _isLive = ! doCalibrate;
            _maxCycles = aCycles;
            _baseRng = new Random(aSeed);
            _factory = new OrderedMemoryNetwork();
            _isStorage = ! inMemory;

            _nodeFactory = new OrderedMemoryNetwork.Factory() {
                public OrderedMemoryNetwork.OrderedMemoryTransport newTransport(InetSocketAddress aLocalAddr,
                                                                                InetSocketAddress aBroadcastAddr,
                                                                                OrderedMemoryNetwork aNetwork,
                                                                                MessageBasedFailureDetector anFD,
                                                                                Object aContext) {
                    NodeAdminImpl myTp = new NodeAdminImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD,
                            (NodeAdminImpl.Config) aContext,
                            EnvironmentImpl.this);

                    _nodes.add(myTp);

                    return myTp.getTransport();
                }
            };

            for (int i = 0; i < 5; i++) {
                addNodeAdmin(Utils.getTestAddress(), new NodeAdminImpl.Config(i, _isLive, _isStorage, BASEDIR));
            }

            _currentLeader = _nodes.getFirst();
        }

        private void addNodeAdmin(InetSocketAddress anAddress, NodeAdminImpl.Config aConfig) {
            _factory.newTransport(_nodeFactory, new FailureDetectorImpl(5, 5000, FailureDetectorImpl.OPEN_PIN),
                    anAddress, aConfig);
        }

        public void addNodeAdmin(NodeAdmin.Memento aMemento) {
            addNodeAdmin(aMemento.getAddress(), (NodeAdminImpl.Config) aMemento.getContext());
        }

        public long getSettleCycles() {
            return _settleCycles;
        }

        public OrderedMemoryNetwork getFactory() {
            return _factory;
        }

        public long getMaxCycles() {
            return _maxCycles;
        }

        public boolean isLive() {
            return _isLive;
        }

        public Random getRng() {
            return _baseRng;
        }

        public boolean validate() {
            // All AL's should be in sync post the stability phase of the test
            //
            Deque<NodeAdmin> myRest = new LinkedList<>(_nodes);
            NodeAdmin myBase = myRest.removeFirst();

            for (NodeAdmin myNA : myRest) {
                if (myNA.getLastSeq() != myBase.getLastSeq())
                    return false;
            }

            return true;
        }

        public NodeAdmin getCurrentLeader() {
            return _currentLeader;
        }

        public long getDropCount() {
            long myTotal = 0;

            for (NodeAdmin myNA: _nodes) {
                myTotal+= myNA.getDropCount();
            }

            return myTotal;
        }

        public long getTxCount() {
            long myTotal = 0;

            for (NodeAdmin myNA: _nodes) {
                myTotal+= myNA.getTxCount();
            }

            return myTotal;
        }

        public long getRxCount() {
            long myTotal = 0;

            for (NodeAdmin myNA: _nodes) {
                myTotal+= myNA.getRxCount();
            }

            return myTotal;
        }

        public void settle() {
            for (NodeAdmin myNA : _nodes)
                myNA.settle();
        }

        /**
         * TODO: Don't avoid killing the leader
         */
        public NodeAdmin.Memento killAtRandom() {
            ArrayList<NodeAdmin> myNodes = new ArrayList<>(_nodes);

            // Avoid killing leader for now
            //
            myNodes.remove(_currentLeader);

            int myIndex = _baseRng.nextInt(myNodes.size());
            NodeAdmin myChoice = myNodes.get(myIndex);

            _logger.info("Killing: " + myChoice);

            _nodes.remove(myChoice);
            return myChoice.terminate();
        }

        public void terminate() {
            for (NodeAdmin myNA : _nodes)
                myNA.terminate();

            _factory.stop();
        }

        public void updateLeader(InetSocketAddress anAddr) {
            for (NodeAdmin myNA : _nodes) {
                Transport myTp = myNA.getTransport();

                if (myTp.getLocalAddress().equals(anAddr)) {
                    _currentLeader = myNA;
                    break;
                }
            }
        }

        public long getDoneOps() {
            return _opCount.get();
        }

        public void doneOp() {
            _opCount.incrementAndGet();

            long myCount = _opsSinceCkpt.incrementAndGet();

            if (myCount >= _ckptCycle) {
                _logger.info("Issuing checkpoint");

                checkpoint();

                _opsSinceCkpt.compareAndSet(myCount, 0);
            }
        }

        private void checkpoint() {
            for (NodeAdmin myNA : _nodes) {
                try {
                    myNA.checkpoint();
                } catch (Throwable aT) {
                    // We can get one of these if the AL is currently OUT_OF_DATE, ignore it
                    //
                    _logger.warn("Exception at checkpoint", aT);
                }
            }

            // Bring any out of date nodes back up
            //
            for (NodeAdmin myNA : _nodes) {
                if (myNA.isOutOfDate())
                    makeCurrent(myNA);
            }
        }

        /**
         * Bring the specified NodeAdmin up to date via another checkpoint if there is one available
         *
         * @param anAdmin
         * @return <code>true</code> if the NodeAdmin was updated
         */
        public boolean makeCurrent(NodeAdmin anAdmin) {
            for (NodeAdmin myNA : _nodes) {
                if ((! myNA.isOutOfDate()) && (myNA.lastCheckpointTime() > anAdmin.lastCheckpointTime())) {

                    try {
                        CheckpointStorage.ReadCheckpoint myCkpt = myNA.getLastCheckpoint();

                        try {
                            if (anAdmin.bringUpToDate(myCkpt)) {
                                _logger.info("AL is now back up to date");

                                return true;
                            }
                        } catch (Exception anE) {
                            _logger.warn("Exception at bring up to date", anE);
                        }
                    } catch (Exception anE) {
                        _logger.warn("Exception reading back checkpoint handle", anE);
                    }
                }
            }

            _logger.info("Not able to update: " + anAdmin);

            return false;
        }
    }

    private final Environment _env;

    private Main(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle,
                 boolean isMemory) throws Exception {
        _env = new EnvironmentImpl(aSeed, aCycles, doCalibrate, aCkptCycle, isMemory);
    }

    long getSettleCycles() {
        return _env.getSettleCycles();
    }

    private void run() throws Exception {
        // We expect at least an 80% success rate post-settle
        //
        long myProgressTarget = (long) (getSettleCycles() * 0.75);
        long mySuccesses = 0;

        ClientDispatcher myClient = new ClientDispatcher();
        Transport myTransport = _env.getFactory().newTransport(null, null, Utils.getTestAddress(), null);
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        cycle(myClient, _env.getMaxCycles());

        if (_env.isLive()) {
            _logger.info("********** Transition to Settling **********");

            _env.settle();

            mySuccesses = cycle(myClient, getSettleCycles());
        }

        myTransport.terminate();

        _env.terminate();

        if (_env.isLive()) {
            _logger.info("Total dropped packets was " + _env.getDropCount());
            _logger.info("Total rx packets was " + _env.getRxCount());
            _logger.info("Total tx packets was " + _env.getTxCount());

            _logger.info("Required success cycles in settle was " + myProgressTarget +
                    " actual was " + mySuccesses);

            if (! (mySuccesses > myProgressTarget))
                throw new Exception("Failed to settle successfully");

            if (! _env.validate())
                throw new IllegalStateException("Paxos is not consistent");
        }
    }

    /**
     * @return number of successful cycles in the run
     */
    private long cycle(ClientDispatcher aClient, long aCycles) {
        long mySuccessCount = 0;
        long myEndCycles = _env.getDoneOps() + aCycles;

        while (_env.getDoneOps() < myEndCycles) {
            /*
             * Perform a paxos vote - need to react to other leader messages but ignore all else - allows us to
             * cope with strategies that switch our leader to test out election and recover from them (assuming we
             * get the right response)
             */
            ByteBuffer myBuffer = ByteBuffer.allocate(8);
            myBuffer.putLong(_env.getDoneOps());
            Proposal myProposal = new Proposal("data", myBuffer.array());

            aClient.send(new Envelope(myProposal),
                    _env.getCurrentLeader().getTransport().getLocalAddress());

            VoteOutcome myEv = aClient.getNext(10000);

            if (myEv.getResult() == VoteOutcome.Reason.OTHER_LEADER) {
                _env.updateLeader(myEv.getLeader());
            } else if (myEv.getResult() == VoteOutcome.Reason.VALUE) {
                mySuccessCount++;
            }

            _env.doneOp();
        }

        return mySuccessCount;
    }

    public static void main(String[] anArgs) throws Exception {
        Args myArgs = CliFactory.parseArguments(Args.class, anArgs);

        for (int myIterations = 0; myIterations < myArgs.getIterations(); myIterations++) {
            _logger.info("Iteration: " + myIterations);

            Main myLT =
                    new Main(myArgs.getSeed() + myIterations, myArgs.getCycles(),
                            myArgs.isCalibrate(), myArgs.getCkptCycle(), myArgs.isMemory());

            long myStart = System.currentTimeMillis();

            myLT.run();

            double myDuration = (System.currentTimeMillis() - myStart) / 1000.0;

            if (myArgs.isCalibrate()) {
                _logger.info("Run for " + myArgs.getCycles() + " cycles took " + myDuration + " seconds");

                double myOpsPerSec = myArgs.getCycles() / myDuration;
                double myOpsHour = myOpsPerSec * 60 * 60;

                _logger.info("Calibration recommendation - ops/sec: " + myOpsPerSec +
                        " iterations in an hour would be: " + myOpsHour);
            } else {
                _logger.info("Run for " + (myArgs.getCycles() + myLT.getSettleCycles()) +
                        " cycles took " + myDuration + " seconds");
            }
        }
    }
}

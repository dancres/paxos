package org.dancres.paxos.test;

import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.storage.MemoryLogStorage;
import org.dancres.paxos.test.net.*;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.MemoryCheckpointStorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
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
public class LongTerm {
    private static final Logger _logger = LoggerFactory.getLogger(LongTerm.class);
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

    interface NodeAdmin {
        Transport getTransport();
        void checkpoint() throws Exception;
        CheckpointStorage.ReadCheckpoint getLastCheckpoint();
        long lastCheckpointTime();
        boolean isOutOfDate();
        void terminate();
        boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt);
        void settle();
        long getDropCount();
        long getTxCount();
        long getRxCount();
        long getLastSeq();
    }

    interface Environment {
        Random getRng();
        void killAtRandom();
        boolean makeCurrent(NodeAdmin anAdmin);
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

        EnvironmentImpl(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle, boolean inMemory) throws Exception {
            _ckptCycle = aCkptCycle;
            _isLive = ! doCalibrate;
            _maxCycles = aCycles;
            _baseRng = new Random(aSeed);
            _factory = new OrderedMemoryNetwork();
            _isStorage = ! inMemory;

            OrderedMemoryNetwork.Factory myFactory = new OrderedMemoryNetwork.Factory() {
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
                _factory.newTransport(myFactory, new FailureDetectorImpl(5, 5000, FailureDetectorImpl.OPEN_PIN),
                        Utils.getTestAddress(), new NodeAdminImpl.Config(i, _isLive, _isStorage));
            }

            _currentLeader = _nodes.getFirst();
        }

        public Random getRng() {
            return _baseRng;
        }

        boolean validate() {
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

        long getDropCount() {
            long myTotal = 0;

            for (NodeAdmin myNA: _nodes) {
                myTotal+= myNA.getDropCount();
            }

            return myTotal;
        }

        long getTxCount() {
            long myTotal = 0;

            for (NodeAdmin myNA: _nodes) {
                myTotal+= myNA.getTxCount();
            }

            return myTotal;
        }

        long getRxCount() {
            long myTotal = 0;

            for (NodeAdmin myNA: _nodes) {
                myTotal+= myNA.getRxCount();
            }

            return myTotal;
        }

        void settle() {
            for (NodeAdmin myNA : _nodes)
                myNA.settle();
        }

        /**
         * TODO: Have this method return necessary bits of state about the Node so it can be recovered if we desire
         * (this probably amounts to it's id which is used to designate the filesystem location of its log files etc.
         * Also need InetAddress).
         */
        public void killAtRandom() {
            ArrayList<NodeAdmin> myNodes = new ArrayList<>(_nodes);

            // Avoid killing leader for now
            //
            myNodes.remove(_currentLeader);

            int myIndex = _baseRng.nextInt(myNodes.size());
            NodeAdmin myChoice = myNodes.get(myIndex);

            _logger.info("Killing: " + myChoice);

            _nodes.remove(myChoice);
            myChoice.terminate();
        }

        void terminate() {
            for (NodeAdmin myNA : _nodes)
                myNA.terminate();
        }

        void updateLeader(InetSocketAddress anAddr) {
            for (NodeAdmin myNA : _nodes) {
                Transport myTp = myNA.getTransport();

                if (myTp.getLocalAddress().equals(anAddr)) {
                    _currentLeader = myNA;
                    break;
                }
            }
        }

        public void checkpoint() {
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

            return false;
        }
    }

    private final EnvironmentImpl _env;

    private static class NodeAdminImpl implements NodeAdmin, Listener {

        private static class Config {
            int _nodeNum;
            boolean _isLive;
            boolean _isStorage;

            private Config(int aNodeNum, boolean isLive, boolean isStorage) {
                _nodeNum = aNodeNum;
                _isLive = isLive;
                _isStorage = isStorage;
            }
        }

        private static final AtomicLong _killCount = new AtomicLong(0);
        private final OrderedMemoryTransportImpl _transport;
        private final ServerDispatcher _dispatcher;
        private final AtomicBoolean _outOfDate = new AtomicBoolean(false);
        private final CheckpointHandling _checkpointer = new CheckpointHandling();
        private final Environment _env;
        private final NetworkDecider _decider;

        /**
         * TODO: Remove the delete of directory done in here - this needs to be done on first time initialisation of
         * LongTerm only. After that we don't do it, at least not in the case where we're simulating a machine that
         * will recover. In the case of a failure, we should (these two cases suggest we should delete at point of
         * failure if we've decided we're not recovering).
         * @param aLocalAddr
         * @param aBroadcastAddr
         * @param aNetwork
         * @param anFD
         * @param aConfig
         * @param anEnv
         */
        NodeAdminImpl(InetSocketAddress aLocalAddr,
                      InetSocketAddress aBroadcastAddr,
                      OrderedMemoryNetwork aNetwork,
                      MessageBasedFailureDetector anFD,
                      Config aConfig,
                      Environment anEnv) {
            _env = anEnv;
            _decider = new NetworkDecider(new Random(_env.getRng().nextLong()));

            if (! aConfig._isLive) {
                _transport = new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD);
            } else {
                _transport = new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD, _decider);
            }

            FileSystem.deleteDirectory(new File(BASEDIR + "node" + Integer.toString(aConfig._nodeNum) + "logs"));

            _dispatcher = (aConfig._isStorage) ?
                    new ServerDispatcher(new HowlLogger(BASEDIR + "node" + Integer.toString(aConfig._nodeNum) + "logs")) :
                    new ServerDispatcher(new MemoryLogStorage());

            _dispatcher.add(this);

            try {
                _transport.routeTo(_dispatcher);
                _dispatcher.init(_transport);
            } catch (Exception anE) {
                throw new RuntimeException("Failed to add a dispatcher", anE);
            }
        }

        /**
         * TODO: This should schedule the recovery of a dead machine based on a certain
         * number of iterations of the protocol so we can fence it to within the bounds
         * of a single snapshot or let it cross, depending on the sophistication/challenge
         * of testing we require.
         */
        class NetworkDecider implements OrderedMemoryTransportImpl.RoutingDecisions {
            private final Random _rng;
            private AtomicBoolean _isSettling = new AtomicBoolean(false);
            private AtomicLong _dropCount = new AtomicLong(0);
            private AtomicLong _packetsTx = new AtomicLong(0);
            private AtomicLong _packetsRx = new AtomicLong(0);

            NetworkDecider(Random aRandom) {
                _rng = aRandom;
            }

            public boolean sendUnreliable(OrderedMemoryNetwork.OrderedMemoryTransport aTransport,
                                          Transport.Packet aPacket) {
                if (aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT))
                    return true;

                _packetsTx.incrementAndGet();

                if (! _isSettling.get()) {
                    if (_rng.nextInt(101) < 2) {
                        _dropCount.incrementAndGet();
                        return false;
                    } else {
                        considerKill();
                    }
                }

                return true;
            }

            public boolean receive(OrderedMemoryNetwork.OrderedMemoryTransport aTransport, Transport.Packet aPacket) {
                // Client isn't written to cope with failure handling
                //
                if (aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT))
                    return true;

                _packetsRx.incrementAndGet();

                if (! _isSettling.get()) {
                    if (_rng.nextInt(101) < 2) {
                        _dropCount.incrementAndGet();
                        return false;
                    } else {
                        considerKill();
                    }
                }

                return true;
            }

            private void considerKill() {
                if ((_killCount.get() != 1) && (_rng.nextInt(101) < 1) && (_killCount.compareAndSet(0, 1))) {
                    _env.killAtRandom();
                }
            }

            void settle() {
                _isSettling.set(true);
            }

            long getDropCount() {
                return _dropCount.get();
            }

            long getRxPacketCount() {
                return _packetsRx.get();
            }

            long getTxPacketCount() {
                return _packetsTx.get();
            }
        }

        public long getDropCount() {
            return _decider.getDropCount();
        }

        public long getTxCount() {
            return _decider.getTxPacketCount();
        }

        public long getRxCount() {
            return _decider.getRxPacketCount();
        }

        public long getLastSeq() {
            return _dispatcher.getAcceptorLearner().getLastSeq();
        }

        /*
         * Create failure state machine at construction (passing in rng).
         *
         * Wedge each of distributed, send and connectTo to hit the state machine.
         * State machine has listener and if it decides to trigger a failure it invokes
         * on that listener so that appropriate implementation can be done.
         *
         * State machine returns type of fail or proceed to the caller. Caller
         * can then determine what it should do (might be stop or continue or ...)
         *
         * State machine not only considers failures to inject but also sweeps it's current
         * list of failures and if any have expired, invokes the listener appropriately
         * to allow appropriate implementation of restore
         *
         */

        public void settle() {
            _decider.settle();
        }

        public void terminate() {
            _transport.terminate();
        }

        public OrderedMemoryNetwork.OrderedMemoryTransport getTransport() {
            return _transport;
        }

        public boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt) {
            boolean myAnswer = _checkpointer.bringUpToDate(aCkpt, _dispatcher);

            if (myAnswer)
                _outOfDate.set(false);

            return myAnswer;
        }

        public void checkpoint() throws Exception {
            _checkpointer.checkpoint(_dispatcher);
        }

        public CheckpointStorage.ReadCheckpoint getLastCheckpoint() {
            return _checkpointer.getLastCheckpoint();
        }

        public long lastCheckpointTime() {
            return _checkpointer.lastCheckpointTime();
        }

        public boolean isOutOfDate() {
            return _outOfDate.get();
        }

        public void transition(StateEvent anEvent) {
            switch (anEvent.getResult()) {
                case OUT_OF_DATE : {
                    // Seek an instant resolution and if it fails, flag it for later recovery
                    //
                    if (! _env.makeCurrent(this))
                        _outOfDate.set(true);

                    break;
                }

                case UP_TO_DATE : {
                    _outOfDate.set(false);

                    break;
                }
            }
        }

        public String toString() {
            return "NodeAdmin: <" + _transport.getLocalAddress() + ">";
        }
    }

    static class CheckpointHandling {
        private CheckpointStorage _ckptStorage = new MemoryCheckpointStorage();
        private AtomicLong _checkpointTime = new AtomicLong(0);

        boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt, ServerDispatcher aDispatcher) {
            try {
                ObjectInputStream myOIS = new ObjectInputStream(aCkpt.getStream());
                CheckpointHandle myHandle = (CheckpointHandle) myOIS.readObject();

                try {
                    return aDispatcher.getCore().bringUpToDate(myHandle);
                } catch (Exception anE) {
                    _logger.warn("Exception at bring up to date", anE);
                }
            } catch (Exception anE) {
                _logger.warn("Exception reading back checkpoint handle", anE);
            }

            return false;
        }

        void checkpoint(ServerDispatcher aDispatcher) throws Exception {
            CheckpointHandle myHandle = aDispatcher.getCore().newCheckpoint();
            CheckpointStorage.WriteCheckpoint myCkpt = _ckptStorage.newCheckpoint();
            ObjectOutputStream myStream = new ObjectOutputStream(myCkpt.getStream());
            myStream.writeObject(myHandle);
            myStream.close();

            myCkpt.saved();
            myHandle.saved();

            _checkpointTime.set(System.currentTimeMillis());

            assert(_ckptStorage.numFiles() == 1);
        }

        public CheckpointStorage.ReadCheckpoint getLastCheckpoint() {
            return _ckptStorage.getLastCheckpoint();
        }

        public long lastCheckpointTime() {
            return _checkpointTime.get();
        }
    }

    private LongTerm(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle,
                     boolean isMemory) throws Exception {
        _env = new EnvironmentImpl(aSeed, aCycles, doCalibrate, aCkptCycle, isMemory);
    }

    long getSettleCycles() {
        return _env._settleCycles;
    }

    private void run() throws Exception {
        // We expect at least an 80% success rate post-settle
        //
        long myProgressTarget = (long) (_env._settleCycles * 0.75);
        long mySuccesses = 0;

        ClientDispatcher myClient = new ClientDispatcher();
        Transport myTransport = _env._factory.newTransport(null, null, Utils.getTestAddress(), null);
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        cycle(myClient, _env._maxCycles, _env._ckptCycle);

        if (_env._isLive) {
            _logger.info("********** Transition to Settling **********");

            _env.settle();

            mySuccesses = cycle(myClient, _env._settleCycles, _env._ckptCycle);
        }

        myTransport.terminate();

        _env.terminate();

        _env._factory.stop();

        if (_env._isLive) {
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
    private long cycle(ClientDispatcher aClient, long aCycles, long aCkptCycle) {
        long opsSinceCkpt = 0;
        long myOpCount = 0;
        long mySuccessCount = 0;

        while (myOpCount < aCycles) {
            /*
             * Perform a paxos vote - need to react to other leader messages but ignore all else - allows us to
             * cope with strategies that switch our leader to test out election and recover from them (assuming we
             * get the right response)
             */
            ByteBuffer myBuffer = ByteBuffer.allocate(8);
            myBuffer.putLong(myOpCount);
            Proposal myProposal = new Proposal("data", myBuffer.array());

            aClient.send(new Envelope(myProposal),
                    _env._currentLeader.getTransport().getLocalAddress());

            VoteOutcome myEv = aClient.getNext(10000);

            ++opsSinceCkpt;

            if (myEv.getResult() == VoteOutcome.Reason.OTHER_LEADER) {
                _env.updateLeader(myEv.getLeader());
            } else if (myEv.getResult() == VoteOutcome.Reason.VALUE) {
                mySuccessCount++;

                if (opsSinceCkpt >= aCkptCycle) {
                    _env.checkpoint();

                    opsSinceCkpt = 0;
                }
            }

            // Round we go again
            //
            myOpCount++;
        }

        return mySuccessCount;
    }

    public static void main(String[] anArgs) throws Exception {
        Args myArgs = CliFactory.parseArguments(Args.class, anArgs);

        for (int myIterations = 0; myIterations < myArgs.getIterations(); myIterations++) {
            _logger.info("Iteration: " + myIterations);

            LongTerm myLT =
                    new LongTerm(myArgs.getSeed() + myIterations, myArgs.getCycles(),
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

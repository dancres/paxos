package org.dancres.paxos.test.longterm;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.storage.MemoryLogStorage;
import org.dancres.paxos.test.net.OrderedMemoryNetwork;
import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.MemoryCheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.Deque;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class NodeAdminImpl implements NodeAdmin, Listener {
    private static final Logger _logger = LoggerFactory.getLogger(NodeAdminImpl.class);

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

            _checkpointTime.set(myHandle.getTimestamp());

            assert(_ckptStorage.numFiles() == 1);
        }

        public CheckpointStorage.ReadCheckpoint getLastCheckpoint() {
            return _ckptStorage.getLastCheckpoint();
        }

        public long lastCheckpointTime() {
            return _checkpointTime.get();
        }
    }


    static class Config {
        private int _nodeNum;
        private boolean _isLive;
        private boolean _isStorage;
        private String _baseDir;
        private final boolean _clean = true;

        Config(int aNodeNum, boolean isLive, boolean isStorage, String aBaseDir) {
            _nodeNum = aNodeNum;
            _isLive = isLive;
            _isStorage = isStorage;
            _baseDir = aBaseDir;
        }

        public String toString() {
            return "Cfg -  NN:" + _nodeNum + ", LV:" + _isLive + ", ST:" + _isStorage + ", BD:" + _baseDir +
                    ", CL:" + _clean;
        }
    }

    private final OrderedMemoryTransportImpl _transport;
    private final ServerDispatcher _dispatcher;
    private final AtomicBoolean _outOfDate = new AtomicBoolean(false);
    private final CheckpointHandling _checkpointer = new CheckpointHandling();
    private final Environment _env;
    private final NetworkDecider _decider;
    private final Config _config;

    /**
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
        _config = aConfig;
        _env = anEnv;
        _decider = new NetworkDecider(_env);

        if (! aConfig._isLive) {
            _transport = new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD);
        } else {
            _transport = new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD, _decider);
        }

        if (aConfig._clean) {
            _logger.info("Cleaning directory");

            FileSystem.deleteDirectory(new File(aConfig._baseDir + "node" + Integer.toString(aConfig._nodeNum) + "logs"));
        }

        _dispatcher = (aConfig._isStorage) ?
                new ServerDispatcher(new HowlLogger(aConfig._baseDir + "node" + Integer.toString(aConfig._nodeNum) + "logs")) :
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
    static class NetworkDecider implements OrderedMemoryTransportImpl.RoutingDecisions {
        static class Grave {
            private AtomicReference<Memento> _dna = new AtomicReference<>(null);
            private AtomicLong _deadCycles = new AtomicLong(0);

            Grave(Memento aDna, long aDeadCycles) {
                _dna.set(aDna);
                _deadCycles.set(aDeadCycles);
            }

            void awaken(Environment anEnv) {
                _logger.info("Awaking from grave: " + _dna.get());

                Memento myDna = _dna.getAndSet(null);

                if (myDna != null)
                    anEnv.addNodeAdmin(myDna);
            }

            boolean reborn(Environment anEnv) {
                if (_deadCycles.decrementAndGet() == 0) {
                    awaken(anEnv);

                    return true;
                }

                return false;
            }
        }

        private final Environment _env;
        private final Random _rng;

        private static final AtomicLong _killCount = new AtomicLong(0);
        private static final AtomicLong _deadCount = new AtomicLong(0);
        private static final Deque<Grave> _graves = new ConcurrentLinkedDeque<>();

        private AtomicLong _dropCount = new AtomicLong(0);
        private AtomicLong _packetsTx = new AtomicLong(0);
        private AtomicLong _packetsRx = new AtomicLong(0);

        NetworkDecider(Environment anEnv) {
            _env = anEnv;
            _rng = new Random(_env.getRng().nextLong());

        }

        public boolean sendUnreliable(OrderedMemoryNetwork.OrderedMemoryTransport aTransport,
                                      Transport.Packet aPacket) {
            _packetsTx.incrementAndGet();

            return decide(aTransport, aPacket);
        }

        public boolean receive(OrderedMemoryNetwork.OrderedMemoryTransport aTransport, Transport.Packet aPacket) {
            _packetsRx.incrementAndGet();

            return decide(aTransport, aPacket);
        }

        private boolean decide(OrderedMemoryNetwork.OrderedMemoryTransport aTransport, Transport.Packet aPacket) {

            // Client isn't written to cope with failure handling
            //
            if (aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.CLIENT))
                return true;

            Iterator<Grave> myGraves = _graves.iterator();

            while (myGraves.hasNext()) {
                Grave myGrave = myGraves.next();

                if (myGrave.reborn(_env)) {
                    _deadCount.decrementAndGet();
                    _graves.remove();
                }
            }

            if (! _env.isSettling()) {
                if (_rng.nextInt(101) < 2) {
                    _dropCount.incrementAndGet();
                    return false;
                } else {
                    // considerKill();
                    considerTempDeath();
                }
            }

            return true;
        }

        private void considerKill() {
            if ((_killCount.get() != 1) && (_rng.nextInt(101) < 1) && (_killCount.compareAndSet(0, 1))) {
                _env.killAtRandom();
            }
        }

        private void considerTempDeath() {
            long myDeadCount = _deadCount.get();

            if ((myDeadCount < 1) && (_rng.nextInt(101) < 1) &&
                    (_deadCount.compareAndSet(myDeadCount, myDeadCount + 1))) {

                int myRebirthPackets;

                while ((myRebirthPackets = _rng.nextInt(100)) == 0);

                Memento myMemento = _env.killAtRandom();

                _logger.info("Grave would be dug for " + myMemento + " with return @ " + myRebirthPackets);

                _graves.add(new Grave(myMemento, myRebirthPackets));
            }
        }

        void settle() {
            Iterator<Grave> myGraves = _graves.iterator();

            while (myGraves.hasNext()) {
                Grave myGrave = myGraves.next();

                myGrave.awaken(_env);
                _graves.remove();
            }

            _killCount.set(0);
            _deadCount.set(0);
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

    public Memento terminate() {
        _transport.terminate();

        return new Memento() {
            private final Config _cf = _config;
            private final InetSocketAddress _ad = _transport.getLocalAddress();

            public Object getContext() {
                return _cf;
            }

            public InetSocketAddress getAddress() {
                return _ad;
            }

            public String toString() {
                return "Memento: " + _cf + ", " + _ad;
            }
        };
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

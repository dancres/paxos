package org.dancres.paxos.test.longterm;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.test.net.OrderedMemoryNetwork;
import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.test.utils.MemoryCheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
        private LogStorageFactory _loggerFactory;

        Config(int aNodeNum, LogStorageFactory aLoggerFactory) {
            _nodeNum = aNodeNum;
            _loggerFactory = aLoggerFactory;
        }

        public String toString() {
            return "Cfg -  NN: " + _nodeNum + ", ST: " + _loggerFactory.toString();
        }
    }

    private final OrderedMemoryTransportImpl _transport;
    private final ServerDispatcher _dispatcher;
    private final AtomicBoolean _outOfDate = new AtomicBoolean(false);
    private final CheckpointHandling _checkpointer = new CheckpointHandling();
    private final Environment _env;
    private final Config _config;
    private final ConcurrentHashMap<String, Random> _rngs = new ConcurrentHashMap<>();

    NodeAdminImpl(InetSocketAddress aLocalAddr,
                  InetSocketAddress aBroadcastAddr,
                  OrderedMemoryNetwork aNetwork,
                  MessageBasedFailureDetector anFD,
                  Config aConfig,
                  Environment anEnv) {
        _config = aConfig;
        _env = anEnv;
        _transport = new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork, anFD, _env);

        _dispatcher = new ServerDispatcher(_config._loggerFactory.getLogger(), this);

        try {
            _dispatcher.init(_transport);
        } catch (Exception anE) {
            throw new RuntimeException("Failed to add a dispatcher", anE);
        }
    }


    public long getLastSeq() {
        return _dispatcher.getAcceptorLearner().getLastSeq();
    }

    public Random getRngByName(String aName) {
        Random myRng = _rngs.get(aName);

        if (myRng == null) {
            myRng = new Random(_env.getRng().nextLong());

            Random myTmpRng = _rngs.putIfAbsent(aName, myRng);

            if (myTmpRng != null)
                myRng = myTmpRng;
        }

        return myRng;
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

    @Override
    public void settle() {
        _transport.settle();
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

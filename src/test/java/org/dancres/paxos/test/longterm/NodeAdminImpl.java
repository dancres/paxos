package org.dancres.paxos.test.longterm;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.test.net.OrderedMemoryNetwork.OrderedMemoryTransport;
import org.dancres.paxos.test.utils.Builder;
import org.dancres.paxos.test.utils.MemoryCheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class NodeAdminImpl implements NodeAdmin, Listener {
    private static final Logger _logger = LoggerFactory.getLogger(NodeAdminImpl.class);

    static class CheckpointHandling {
        private final CheckpointStorage _ckptStorage = new MemoryCheckpointStorage();
        private final AtomicLong _checkpointTime = new AtomicLong(0);

        boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt, Core aCore) {
            try {
                ObjectInputStream myOIS = new ObjectInputStream(aCkpt.getStream());
                CheckpointHandle myHandle = (CheckpointHandle) myOIS.readObject();
                myOIS.close();

                try {
                    Paxos.Checkpoint myCheckpoint = aCore.checkpoint().forRecovery();
                    return myCheckpoint.getConsumer().apply(myHandle);
                } catch (Exception anE) {
                    _logger.warn("Exception at bring up to date", anE);
                }
            } catch (Exception anE) {
                _logger.warn("Exception reading back checkpoint handle", anE);
            }

            return false;
        }

        CheckpointHandle checkpoint(Core aCore) throws Exception {
            Paxos.Checkpoint myCheckpoint = aCore.checkpoint().forSaving();
            CheckpointStorage.WriteCheckpoint myCkpt = _ckptStorage.newCheckpoint();
            ObjectOutputStream myStream = new ObjectOutputStream(myCkpt.getStream());
            myStream.writeObject(myCheckpoint.getHandle());
            myStream.close();

            myCkpt.saved();
            myCheckpoint.getConsumer().apply(myCheckpoint.getHandle());

            _checkpointTime.set(myCheckpoint.getHandle().getTimestamp());

            assert(_ckptStorage.numFiles() == 1);

            return  myCheckpoint.getHandle();
        }

        CheckpointStorage.ReadCheckpoint getLastCheckpoint() {
            return _ckptStorage.getLastCheckpoint();
        }

        long lastCheckpointTime() {
            return _checkpointTime.get();
        }
    }
    
    static class Config {
        private final LogStorageFactory _loggerFactory;
        private CheckpointHandle _handle = CheckpointHandle.NO_CHECKPOINT;

        Config(LogStorageFactory aLoggerFactory) {
            _loggerFactory = aLoggerFactory;
        }

        void setHandle(CheckpointHandle aHandle) {
            _handle = aHandle;
        }

        CheckpointHandle getHandle() {
            return _handle;
        }

        public String toString() {
            return "Cfg -  ST: " + _loggerFactory.toString();
        }
    }

    private final OrderedMemoryTransport _transport;
    private final Core _dispatcher;
    private final AtomicBoolean _outOfDate = new AtomicBoolean(false);
    private final CheckpointHandling _checkpointer = new CheckpointHandling();
    private final Environment _env;
    private final Config _config;

    NodeAdminImpl(OrderedMemoryTransport aTransport,
                  Config aConfig,
                  Environment anEnv) {
        _config = aConfig;
        _env = anEnv;
        _transport = aTransport;

        try {
            _dispatcher = new Builder().newCoreWith(_config._loggerFactory.getLogger(), _config.getHandle(), this, _transport);
        } catch (Exception anE) {
            throw new RuntimeException("Failed to create Core: ", anE);
        }
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
                return "Memento: " + _ad;
            }
        };
    }

    @Override
    public void settle() {
        _transport.settle();
    }

    public OrderedMemoryTransport getTransport() {
        return _transport;
    }

    public boolean bringUpToDate(CheckpointStorage.ReadCheckpoint aCkpt) {
        boolean myAnswer = _checkpointer.bringUpToDate(aCkpt, _dispatcher);

        if (myAnswer)
            _outOfDate.set(false);

        return myAnswer;
    }

    public void checkpoint() throws Exception {
        _config.setHandle(_checkpointer.checkpoint(_dispatcher));
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
                if (! _env.getNodes().makeCurrent(this))
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

package org.dancres.paxos;

import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.storage.MemoryLogStorage;

/**
 * @author dan
 */
public class PaxosFactory {
    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData) throws Exception {
        Core myCore = new Core(new FailureDetectorImpl(5000), new MemoryLogStorage(), aMetaData, aHandle, aListener);
        new TransportImpl().add(myCore);

        return new PaxosWrapper(myCore);
    }

    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData,
                             LogStorage aLogger) throws Exception {
        Core myCore = new Core(new FailureDetectorImpl(5000), aLogger, aMetaData, aHandle, aListener);
        new TransportImpl().add(myCore);

        return new PaxosWrapper(myCore);
    }

    private static class PaxosWrapper implements Paxos {
        private final Core _core;

        PaxosWrapper(Core aCore) {
            _core = aCore;
        }

        public void close() {
            _core.close();
        }

        public CheckpointHandle newCheckpoint() {
            return _core.newCheckpoint();
        }

        public void submit(Proposal aValue) throws InactiveException {
            _core.submit(aValue, new Completion() {
                public void complete(VoteOutcome anOutcome) {
                    // Do nothing for now
                }
            });
        }

        public void add(Listener aListener) {
            _core.add(aListener);
        }

        public boolean bringUpToDate(CheckpointHandle aHandle) throws Exception {
            return _core.bringUpToDate(aHandle);
        }

        public FailureDetector getDetector() {
            return _core.getDetector();
        }
    }
}

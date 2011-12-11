package org.dancres.paxos;

import java.net.InetSocketAddress;

import org.dancres.paxos.impl.CheckpointHandle;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.LogStorage;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.impl.util.MemoryLogStorage;

/**
 * @todo Convert to use HowlLogger
 * 
 * @author dan
 *
 */
public class PaxosFactory {
    /**
     * An application should first load it's last available snapshot which should include an instance of a
     * SnapshotHandle which should be passed to <code>init</code>. If the application has no previous snapshot
     * and no thus no valid <code>SnapshotHandle</code> it should pass SnapshotHandle.NO_SNAPSHOT
     *
     * @param aListener that will receive agreed messages, snapshots etc.
     * @param aHandle is the handle last given to the application as result of a snapshot.
     */
    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData) throws Exception {
    	return new PaxosImpl(aListener, aHandle, aMetaData, new MemoryLogStorage());
    }
    
    private static class PaxosImpl implements Paxos {
    	private Core _core;
    	private TransportImpl _transport;
    	
    	PaxosImpl(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData,
    			LogStorage aLog) throws Exception {
    		_core = new Core(5000, aLog, aMetaData, aListener);
    		_transport = new TransportImpl();
    		_transport.add(_core);
    	}
    	
		public void close() {
			_core.stop();
		}

		public CheckpointHandle newCheckpoint() {
			return _core.getAcceptorLearner().newCheckpoint();
		}

		public void submit(Proposal aValue) {
			_core.submit(aValue);
		}

		public void register(Listener aListener) {
			_core.getAcceptorLearner().add(aListener);
		}

		public void bringUpToDate(CheckpointHandle aHandle) throws Exception {
			_core.getAcceptorLearner().bringUpToDate(aHandle);
		}

		public byte[] getMetaData(InetSocketAddress anAddress) {
			return _core.getFailureDetector().getMetaData(anAddress);
		}
    }
}

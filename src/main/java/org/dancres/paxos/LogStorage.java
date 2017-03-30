package org.dancres.paxos;

/**
 * Standard abstraction for the log required to maintain essential paxos state at recovery.
 *
 * @author dan
 */
public interface LogStorage {
	
	interface RecordListener {
		void onRecord(long anOffset, byte[] aRecord);
	}
	
    byte[] get(long mark) throws Exception;
    long put(byte[] data, boolean sync) throws Exception;
    void mark(long key, boolean force) throws Exception;
    void close() throws Exception;
    void open() throws Exception;
    void replay(RecordListener listener, long mark) throws Exception;
}

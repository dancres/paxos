package org.dancres.paxos.impl;

/**
 * Standard abstraction for the log required to maintain essential paxos state to ensure appropriate recovery.
 *
 * @author dan
 */
public interface LogStorage {
	
	public interface RecordListener {
		public void onRecord(long anOffset, byte[] aRecord);
	}
	
    public byte[] get(long mark) throws Exception;
    public long put(byte[] data, boolean sync) throws Exception;
    public void mark(long key, boolean force) throws Exception;
    public void close() throws Exception;
    public void open() throws Exception;
    public void replay(RecordListener listener, long mark) throws Exception;    
}

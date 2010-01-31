package org.dancres.paxos;

public interface RecordListener {
	public void onRecord(long anOffset, byte[] aRecord);
}

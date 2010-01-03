package org.dancres.paxos;

public interface RecordListener {
	public void onRecord(byte[] aRecord);
}

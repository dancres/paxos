package org.dancres.paxos.impl;

public interface RecordListener {
	public void onRecord(long anOffset, byte[] aRecord);
}

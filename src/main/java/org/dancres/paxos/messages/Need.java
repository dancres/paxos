package org.dancres.paxos.messages;

import java.util.EnumSet;

/**
 * Emitted by AL when it's looking for some paxos instances. Emitted on the initiation of recovery.
 * Actual range of instances required is _minSeq < i <= _maxSeq, where i is a single instance.
 */
public class Need implements PaxosMessage {
	private final long _minSeq;
	private final long _maxSeq;
	
	public Need(long aMin, long aMax) {
		_minSeq = aMin;
		_maxSeq = aMax;
	}
	
	public EnumSet<Classification> getClassifications() {
		return EnumSet.of(Classification.RECOVERY);
	}

	public long getSeqNum() {
		// No meaningful seqnum
		//
		return -1;
	}

	public int getType() {
		return Operations.NEED;
	}
	
	public long getMinSeq() {
		return _minSeq;
	}

	public long getMaxSeq() {
		return _maxSeq;
	}
	
	public String toString() {
        return "Need: " + Long.toHexString(_minSeq) + " -> " + Long.toHexString(_maxSeq);
	}
}

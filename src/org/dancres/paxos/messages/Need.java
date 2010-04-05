package org.dancres.paxos.messages;

/**
 * Emitted by AL when it's looking for some paxos instances. Emitted on the initiation of recovery.
 * Actual range of instances required is _minSeq < i < _maxSeq, where i is a single instance.
 */
public class Need implements PaxosMessage {
	private long _nodeId;	
	private long _minSeq;
	private long _maxSeq;
	
	public Need(long aMin, long aMax, long aNodeId) {
		_minSeq = aMin;
		_maxSeq = aMax;
		_nodeId = aNodeId;
	}
	
	public short getClassification() {
		return RECOVERY;
	}

	public long getNodeId() {
		// TODO Auto-generated method stub
		return _nodeId;
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
        return "Need: " + Long.toHexString(_minSeq) + " -> " + Long.toHexString(_maxSeq) + ", " +
        	Long.toHexString(_nodeId) + " ]";		
	}
}

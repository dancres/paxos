package org.dancres.paxos.messages;

public interface PaxosMessage {
	public static final short ACCEPTOR_LEARNER = 1;
	public static final short CLIENT = 2;
	public static final short LEADER = 3;
	public static final short FAILURE_DETECTOR = 4;
	
    public int getType();
    public long getSeqNum();
    public short getClassification();
}

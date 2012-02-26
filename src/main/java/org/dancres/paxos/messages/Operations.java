package org.dancres.paxos.messages;

public interface Operations {
	public static final int HEARTBEAT = 0;
    public static final int OUTOFDATE = 1;
    public static final int ENVELOPE = 2;
    public static final int COLLECT = 3;
    public static final int LAST = 4;
    public static final int BEGIN = 5;
    public static final int ACCEPT = 6;
    public static final int SUCCESS = 7;
    public static final int OLDROUND = 9;
    public static final int NEED = 10;    
    public static final int EVENT = 11;
}

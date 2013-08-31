package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

import java.util.EnumSet;

/**
 * Message produced by <code>Heartbeater</code> for consumption and processing by <code>FailureDetectorImpl</code>
 *
 * @author dan
 */
public class Heartbeat implements PaxosMessage {
    private final byte[] _metaData;
    
    public Heartbeat(byte[] metaData) {
        _metaData = metaData;
    }
    
    public int getType() {
        return Operations.HEARTBEAT;
    }

    public EnumSet<Classification> getClassifications() {
    	return EnumSet.of(Classification.FAILURE_DETECTOR);
    }
        
    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a heartbeat");
    }

    public byte[] getMetaData() {
        return _metaData;
    }
    
    public String toString() {
        return "Hbeat";
    }
}

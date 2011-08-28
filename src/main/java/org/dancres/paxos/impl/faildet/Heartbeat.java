package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;

/**
 * Message produced by <code>Heartbeater</code> for consumption and processing by <code>FailureDetectorImpl</code>
 *
 * @author dan
 */
public class Heartbeat implements PaxosMessage {
    private InetSocketAddress _addr;
    private byte[] _metaData;
    
    public Heartbeat(InetSocketAddress anAddr, byte[] metaData) {
    	_addr = anAddr;
        _metaData = metaData;
    }
    
    public InetSocketAddress getNodeId() {
    	return _addr;
    }
    
    public int getType() {
        return Operations.HEARTBEAT;
    }

    public short getClassification() {
    	return FAILURE_DETECTOR;
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
    
    public boolean isResponse() {
    	return false;
    }
    
    public int hashCode() {
    	return _addr.hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Heartbeat) {
    		Heartbeat myOther = (Heartbeat) anObject;
    		
    		return (myOther._addr.equals(_addr));
    	}
    	
    	return false;
    }
}

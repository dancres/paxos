package org.dancres.paxos.impl.mina.io;

import org.apache.mina.common.IoFilterAdapter;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.LivenessListener;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.impl.util.AddressImpl;

public class FailureDetectorAdapter extends IoFilterAdapter {
    private FailureDetectorImpl _detector;

    public FailureDetectorAdapter(long anUnresponsivenessThreshold) {
        _detector = new FailureDetectorImpl(anUnresponsivenessThreshold);
    }

    public void messageReceived(NextFilter aNextFilter, IoSession aSession, Object anObject) throws Exception {
    	PaxosMessage myMessage = (PaxosMessage) anObject;
    	
    	if (myMessage.getType() == Heartbeat.TYPE)
    		_detector.processMessage(myMessage, new AddressImpl(aSession.getRemoteAddress()));
    	else
    		super.messageReceived(aNextFilter, aSession, anObject);
    }

    public void add(LivenessListener aListener) {
        _detector.add(aListener);
    }

    public FailureDetectorImpl getDetector() {
        return _detector;
    }
}

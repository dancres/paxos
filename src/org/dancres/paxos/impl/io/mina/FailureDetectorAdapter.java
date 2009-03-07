package org.dancres.paxos.impl.io.mina;

import org.apache.mina.common.IoFilterAdapter;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.dancres.paxos.impl.faildet.LivenessListener;
import org.dancres.paxos.impl.PaxosPeer;

public class FailureDetectorAdapter extends IoFilterAdapter {
    private FailureDetector _detector;

    public FailureDetectorAdapter() {
        _detector = new FailureDetector();
    }

    public void messageReceived(NextFilter aNextFilter, IoSession aSession, Object anObject) throws Exception {
    	PaxosMessage myMessage = (PaxosMessage) anObject;
    	
    	if (myMessage.getType() == Operations.HEARTBEAT)
    		_detector.processMessage(myMessage, aSession.getRemoteAddress());
    	else
    		super.messageReceived(aNextFilter, aSession, anObject);
    }

    public void add(LivenessListener aListener) {
        _detector.add(aListener);
    }

    public FailureDetector getDetector() {
        return _detector;
    }
}

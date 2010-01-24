package org.dancres.paxos.impl.mina.io;

import org.apache.mina.common.IoHandlerAdapter;
import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.Leader;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaxosPacketHandler extends IoHandlerAdapter {
    private Logger _logger = LoggerFactory.getLogger(PaxosPacketHandler.class);

    private Leader _ld;
    private AcceptorLearner _al;
    private FailureDetectorImpl _fd;
    
    void setLeader(Leader aLeader) {
    	_ld = aLeader;
    }
    
    void setAcceptorLearner(AcceptorLearner anAl) {
    	_al = anAl;
    }
    
    void setFailureDetector(FailureDetectorImpl anFd) {
    	_fd = anFd;
    }
    
    public void exceptionCaught(org.apache.mina.common.IoSession aSession,
			java.lang.Throwable aThrowable) throws java.lang.Exception {
		_logger.error("Server exp: s=" + aSession, aThrowable);
	}

	public void messageReceived(org.apache.mina.common.IoSession aSession,
			java.lang.Object anObject) throws java.lang.Exception {

		PaxosMessage myMessage = (PaxosMessage) anObject;

		if (myMessage.getType() != Heartbeat.TYPE)
			_logger.info("serverMsgRx: s=" + aSession + " o=" + anObject);

        switch (myMessage.getClassification()) {
        	case PaxosMessage.FAILURE_DETECTOR: {
        		_fd.processMessage(myMessage, NodeId.from(aSession.getRemoteAddress()));

        		break;
        	}

        	case PaxosMessage.LEADER: {
        		ProposerPacket myPropPkt = (ProposerPacket) myMessage;
        		_al.messageReceived(myPropPkt.getOperation(), NodeId.from(aSession.getRemoteAddress(), 
        				myPropPkt.getPort()));

        		break;
        	}

        	case PaxosMessage.ACCEPTOR_LEARNER :
        	case PaxosMessage.CLIENT : {
        		_ld.messageReceived(myMessage, NodeId.from(aSession.getRemoteAddress()));
        		break;
        	}

        	default: {
        		_logger.error("Unrecognised message:" + myMessage);
        	}
        }
	}

	public void messageSent(org.apache.mina.common.IoSession aSession,
			java.lang.Object anObject) throws java.lang.Exception {

		PaxosMessage myMessage = (PaxosMessage) anObject;

		if (myMessage.getType() != Heartbeat.TYPE)
			_logger.info("serverMsgTx: s=" + aSession + " o=" + anObject);
	}
}

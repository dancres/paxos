package org.dancres.paxos.test.utils;

import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.AcceptorLearnerListener;
import org.dancres.paxos.Event;
import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Leader;
import org.dancres.paxos.LogStorage;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.utils.Node.PacketBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerDispatcher implements TransportImpl.Dispatcher {
    private static Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    private NodeId _clientAddress;	
    private AcceptorLearner _al;
    private Leader _ld;
    private FailureDetectorImpl _fd;
    private Heartbeater _hb;
    private Transport _tp;
	
    private long _unresponsivenessThreshold;
    private LogStorage _log;
    
    public ServerDispatcher(long anUnresponsivenessThreshold) {
    	this(anUnresponsivenessThreshold, new MemoryLogStorage());
    }

    public ServerDispatcher(long anUnresponsivenessThreshold, LogStorage aLogger) {
    	_unresponsivenessThreshold = anUnresponsivenessThreshold;
    	_log = aLogger;
    }
    
	public void messageReceived(PaxosMessage aMessage) {
		try {
			switch (aMessage.getClassification()) {
				case PaxosMessage.FAILURE_DETECTOR : {
					_fd.processMessage(aMessage);

					break;
				}

				// UGLY HACK!!!
				//
				case PaxosMessage.CLIENT : {
					_clientAddress = NodeId.from(aMessage.getNodeId());
					_ld.messageReceived(aMessage);

					break;	
				}

				case PaxosMessage.LEADER:
				case PaxosMessage.RECOVERY : {
					_al.messageReceived(aMessage);

					break;
				}

				case PaxosMessage.ACCEPTOR_LEARNER: {
					_ld.messageReceived(aMessage);

					break;
				}

				default : {
					_logger.error("Unrecognised message:" + aMessage);
				}
			}
		} catch (Exception anE) {
        	_logger.error("Unexpected exception", anE);
        }
    }


	public void setTransport(Transport aTransport) {
		_tp = aTransport;
		
        _hb = new Heartbeater(_tp);
        _fd = new FailureDetectorImpl(_unresponsivenessThreshold);
        _al = new AcceptorLearner(_log, _fd, _tp);
        _ld = new Leader(_fd, _tp, _al);
        _al.add(new PacketBridge());  
        _hb.start();
	}
	
	public FailureDetector getFailureDetector() {
		return _fd;
	}
	
    public void stop() {
    	_fd.stop();
    	_hb.halt();
    	
    	try {
    		_hb.join();
    	} catch (InterruptedException anIE) {    		
    	}
    	
    	_tp.shutdown();
    	_al.close();
    	_ld.shutdown();
    }
    	
    /**
     * @todo Remove this ugly hack once we do handbacks etc
     */
    class PacketBridge implements AcceptorLearnerListener {

        public void done(Event anEvent) {
            // If we're not the originating node for the post, because we're not leader, we won't have an addressed stored up
            //
            if (_clientAddress == null)
                return;

            if (anEvent.getResult() == Event.Reason.DECISION) {
                _tp.send(new Complete(anEvent.getSeqNum()), _clientAddress);
            } else {
                _tp.send(new Fail(anEvent.getSeqNum(), anEvent.getResult()), _clientAddress);
            }
        }
    }	
}

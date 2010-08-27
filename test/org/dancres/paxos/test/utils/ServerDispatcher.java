package org.dancres.paxos.test.utils;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.messages.Complete;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ServerDispatcher implements TransportImpl.Dispatcher {
    private static Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    private AcceptorLearner _al;
    private Leader _ld;
    private FailureDetectorImpl _fd;
    private Heartbeater _hb;
    private Transport _tp;

    private AtomicLong _handbackGenerator = new AtomicLong(0);
	private Map<String, NodeId> _requestMap = new ConcurrentHashMap<String, NodeId>();

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

				case PaxosMessage.CLIENT : {
                    String myHandback = Long.toString(_handbackGenerator.getAndIncrement());
                    _requestMap.put(myHandback, NodeId.from(aMessage.getNodeId()));

                    Post myPost = (Post) aMessage;                            
                    _ld.submit(myPost.getValue(), myHandback.getBytes());

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
	
	public Transport getTransport() {
		return _tp;
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
    	
    class PacketBridge implements AcceptorLearnerListener {

        public void done(Event anEvent) {
            // If we're not the originating node for the post, because we're not leader, we won't have an addressed stored up
            //
            String myHandback = new String(anEvent.getHandback());
            NodeId myAddr = _requestMap.remove(myHandback);

            if (myAddr == null)
                return;

            if (anEvent.getResult() == Event.Reason.DECISION) {
                _tp.send(new Complete(anEvent.getSeqNum()), myAddr);
            } else {
                _tp.send(new Fail(anEvent.getSeqNum(), anEvent.getResult()), myAddr);
            }
        }
    }

	public AcceptorLearner getAcceptorLearner() {
		return _al;
	}

	public Leader getLeader() {
		return _ld;
	}	
}

package org.dancres.paxos.impl.net;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.messages.Complete;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p></p>Handles interactions between server and client. Assumes client is using a <code>ClientDispatcher</code>.
 * This constitutes a default server implementation. It shares the transport used by core for internal messaging
 * related to paxos instances, membership, failure detection etc.</p>
 *
 * <p>Metadata passed to the <code>ServerDispatcher</code> constructors will be advertised via Heartbeats.</p>
 *
 * @see org.dancres.paxos.impl.faildet.Heartbeater
 *
 * @todo Implement client failover across leadership plus client discovery of servers in cluster.
 */
public class ServerDispatcher implements Transport.Dispatcher, Paxos.Listener {
	private static final String DATA_KEY = "data";
	private static final String HANDBACK_KEY = "handback";
	
    private static Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    protected Core _core;
    protected Transport _tp;

    private AtomicLong _handbackGenerator = new AtomicLong(0);
	private Map<String, InetSocketAddress> _requestMap = new ConcurrentHashMap<String, InetSocketAddress>();

    public ServerDispatcher(long anUnresponsivenessThreshold, byte[] aMeta) {
        this(anUnresponsivenessThreshold, new MemoryLogStorage(), aMeta);
    }

    public ServerDispatcher(long anUnresponsivenessThreshold) {
    	this(anUnresponsivenessThreshold, new MemoryLogStorage());
    }

    public ServerDispatcher(long anUnresponsivenessThreshold, LogStorage aLogger) {
        this(anUnresponsivenessThreshold, aLogger, null);
    }

    public ServerDispatcher(long anUnresponsivenessThreshold, LogStorage aLogger, byte[] aMeta) {
        _core = new Core(anUnresponsivenessThreshold, aLogger, aMeta, this);
    }

	public boolean messageReceived(PaxosMessage aMessage) {
		try {
			switch (aMessage.getClassification()) {
				case PaxosMessage.CLIENT : {
                    String myHandback = Long.toString(_handbackGenerator.getAndIncrement());
                    _requestMap.put(myHandback, aMessage.getNodeId());

                    Post myPost = (Post) aMessage;
                    ConsolidatedValue myVal = new ConsolidatedValue();
                    myVal.put(DATA_KEY, myPost.getValue());
                    myVal.put(HANDBACK_KEY, myHandback.getBytes());
                    _core.submit(myVal);

                    return true;
				}

                default : {
                    _logger.debug("Unrecognised message:" + aMessage);
                    return false;
                }
			}
		} catch (Exception anE) {
        	_logger.error("Unexpected exception", anE);
            return false;
        }
    }


	public void setTransport(Transport aTransport) throws Exception {
		_tp = aTransport;
        _tp.add(_core);
	}
	
	public Transport getTransport() {
		return _tp;
	}
	
	public FailureDetector getFailureDetector() {
		return _core.getFailureDetector();
	}
	
    public void stop() {
        _core.stop();
    }

    public void done(Event anEvent) {
        // If we're not the originating node for the post, because we're not leader, we won't have an address stored up
        //
        InetSocketAddress myAddr =
                (anEvent.getValues().get(HANDBACK_KEY) != null) ? 
                		_requestMap.remove(new String(anEvent.getValues().get(HANDBACK_KEY))) :
                        null;

        if (myAddr == null)
            return;

        if (anEvent.getResult() == Event.Reason.DECISION) {
            _tp.send(new Complete(anEvent.getSeqNum()), myAddr);
        } else {
            _tp.send(new Fail(anEvent.getSeqNum(), anEvent.getResult()), myAddr);
        }
    }

    public AcceptorLearner getAcceptorLearner() {
		return _core.getAcceptorLearner();
	}

	public Leader getLeader() {
		return _core.getLeader();
	}	
}

package org.dancres.paxos.test.net;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.Event;
import org.dancres.paxos.storage.MemoryLogStorage;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * <p></p>Handles interactions between server and client. Assumes client is using a <code>ClientDispatcher</code>.
 * This constitutes a default server implementation. It shares the transport used by core for internal messaging
 * related to paxos instances, membership, failure detection etc.</p>
 *
 * <p>Metadata passed to the <code>ServerDispatcher</code> constructors will be advertised via Heartbeats.</p>
 */
public class ServerDispatcher implements Transport.Dispatcher {
	private static final String HANDBACK_KEY = "org.dancres.paxos.handback";
	
    private static Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    protected Core _core;
    protected Transport _tp;
    protected Transport.Dispatcher _dispatcher;

    public ServerDispatcher(MessageBasedFailureDetector anFD, byte[] aMeta) {
        this(anFD, new MemoryLogStorage(), aMeta);
    }

    public ServerDispatcher(MessageBasedFailureDetector anFD) {
    	this(anFD, new MemoryLogStorage());
    }

    public ServerDispatcher(MessageBasedFailureDetector anFD, LogStorage aLogger) {
        this(anFD, aLogger, null);
    }

    public ServerDispatcher(Core aCore, Transport.Dispatcher aDispatcher) {
        _core = aCore;
        _dispatcher = aDispatcher;
    }

    private ServerDispatcher(MessageBasedFailureDetector anFD, LogStorage aLogger, byte[] aMeta) {
        _core = new Core(anFD, aLogger, aMeta, CheckpointHandle.NO_CHECKPOINT, new Listener() {
            public void transition(StateEvent anEvent) {
                // Nothing to do
            }
        });
    }

	public boolean messageReceived(Packet aPacket) {
		PaxosMessage myMessage = aPacket.getMessage();
		
		try {
			switch (myMessage.getClassification()) {
				case PaxosMessage.CLIENT : {
                    final InetSocketAddress mySource = aPacket.getSource();

                    Envelope myEnvelope = (Envelope) myMessage;
                    Proposal myProposal = myEnvelope.getValue();

                    _core.submit(myProposal, new Completion<VoteOutcome>() {
                        public void complete(VoteOutcome anOutcome) {
                            _tp.send(new Event(anOutcome), mySource);
                        }
                    });

                    return true;
				}

                default : {
                    _logger.debug("Unrecognised message:" + myMessage);
                    return false;
                }
			}
		} catch (Exception anE) {
        	_logger.error("Unexpected exception", anE);
            return false;
        }
    }


	public void init(Transport aTransport) throws Exception {
		_tp = aTransport;

        if (_dispatcher == null) {
            _tp.routeTo(_core);
            _core.init(_tp);
        } else {
            _tp.routeTo(_dispatcher);
            _dispatcher.init(_tp);
        }
	}
	
	public Transport getTransport() {
		return _tp;
	}
	
    public void terminate() {
    }

    public void add(Listener aListener) {
    	_core.add(aListener);
    }
    
    public Common getCommon() {
        return _core.getCommon();
    }

    public AcceptorLearner getAcceptorLearner() {
		return _core.getAcceptorLearner();
	}
    
    public Core getCore() {
        return _core;
    }
}

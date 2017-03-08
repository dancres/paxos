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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p></p>Handles interactions between server and client. Assumes client is using a <code>ClientDispatcher</code>.
 * This constitutes a default server implementation. It shares the transport used by core for internal messaging
 * related to paxos instances, membership, failure detection etc.</p>
 *
 * <p>Metadata passed to the <code>ServerDispatcher</code> constructors will be advertised via Heartbeats.</p>
 */
public class ServerDispatcher implements Transport.Dispatcher {
    private static final Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    private final Core _core;
    private final AtomicBoolean _initd = new AtomicBoolean(false);
    private Transport _tp;

    public ServerDispatcher(LogStorage aLogger) {
        this(aLogger, false);
    }

    /**
     * For testing only
     */
    public ServerDispatcher() {
        this(new MemoryLogStorage(), false);
    }

    /**
     * For testing only
     */
    public ServerDispatcher(LogStorage aLogger, boolean isDisableHeartbeats) {
        this(new Core(aLogger, CheckpointHandle.NO_CHECKPOINT, new Listener() {
            public void transition(StateEvent anEvent) {
                // Nothing to do
            }
        }, isDisableHeartbeats));
    }

    private ServerDispatcher(Core aCore) {
        _core = aCore;
    }

	public boolean packetReceived(Packet aPacket) {
        if (! _initd.get())
            return false;

        PaxosMessage myMessage = aPacket.getMessage();
		
		try {
            if (myMessage.getClassifications().contains(PaxosMessage.Classification.CLIENT)) {
                final InetSocketAddress mySource = aPacket.getSource();

                Envelope myEnvelope = (Envelope) myMessage;
                Proposal myProposal = myEnvelope.getValue();

                _core.submit(myProposal, new Completion<VoteOutcome>() {
                    public void complete(VoteOutcome anOutcome) {
                        _tp.send(_tp.getPickler().newPacket(new Event(anOutcome)), mySource);
                    }
                });

                return true;
            } else {

                _logger.trace("Unrecognised message:" + myMessage);
                return false;
            }
        } catch (Throwable anE) {
        	_logger.error("Unexpected exception", anE);
            return false;
        }
    }


	public void init(Transport aTransport) throws Exception {
		_tp = aTransport;
        _tp.routeTo(_core);
        _core.init(_tp);
        _initd.set(true);
	}
	
    public void terminate() {
    }

    public void add(Listener aListener) {
    	_core.add(aListener);
    }
    
    public AcceptorLearner getAcceptorLearner() {
		return _core.getAcceptorLearner();
	}
    
    public Core getCore() {
        return _core;
    }
}

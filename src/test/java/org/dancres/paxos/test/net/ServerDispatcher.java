package org.dancres.paxos.test.net;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.Event;
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
public class ServerDispatcher {
    private static final Logger _logger = LoggerFactory.getLogger(ServerDispatcher.class);

    private Core _core;

    public ServerDispatcher(LogStorage aLogger, CheckpointHandle aHandle, Listener aListener, boolean isDisableHeartbeats) {
        this(new Core(aLogger, aHandle,  aListener, isDisableHeartbeats));
    }

    private ServerDispatcher(Core aCore) {
        _core = aCore;
    }

    class Submitter implements Transport.Filter {
        public Packet filter(Transport aTransport, Packet aPacket) {
            if (_core.isInitd()) {
                PaxosMessage myMessage = aPacket.getMessage();

                try {
                    if (myMessage.getClassifications().contains(PaxosMessage.Classification.CLIENT)) {
                        final InetSocketAddress mySource = aPacket.getSource();

                        Envelope myEnvelope = (Envelope) myMessage;
                        Proposal myProposal = myEnvelope.getValue();

                        _core.submit(myProposal, (VoteOutcome anOutcome) ->
                                _core.getCommon().getTransport().send(
                                        _core.getCommon().getTransport().getPickler().newPacket(new Event(anOutcome)),
                                        mySource));

                        return null;
                    }
                } catch (Throwable anE) {
                    _logger.error("Unexpected exception", anE);
                }
            }

            return aPacket;
        }
    }

	public void init(Transport aTransport) throws Exception {
        aTransport.filterRx(new Submitter());
        _core.init(aTransport);
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

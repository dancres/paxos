package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ProposerImpl {
    private Logger _logger = LoggerFactory.getLogger(ProposerImpl.class);

    private ProposerState _state;
    private Transport _transport;

    /**
     * @param aTransport to send messages over
     * @param aDetector to use for construction of memberships
     * @param anAddress with which to generate an id for this node
     */
    public ProposerImpl(Transport aTransport, FailureDetector aDetector, long aNodeId) {
        _state = new ProposerState(aDetector, aNodeId);
        _transport = aTransport;
    }

    /**
     * @param aMessage to process
     * @param aSenderAddress at which the sender of this message can be found
     */
    public void process(PaxosMessage aMessage, Address aSenderAddress) {
        switch (aMessage.getType()) {
            case Operations.POST : {
                _logger.info("Received post - starting leader");

                // Sender address will be the client
                //
                LeaderImpl myLeader = _state.newLeader(_transport, aSenderAddress);
                myLeader.messageReceived(aMessage, aSenderAddress);
                break;
            }
            
            case Operations.OLDROUND :
            case Operations.LAST :
            case Operations.ACCEPT :
            case Operations.ACK: {
                LeaderImpl myLeader = _state.getLeader(aMessage.getSeqNum());

                if (myLeader != null)
                    myLeader.messageReceived(aMessage, aSenderAddress);
                else {
                    _logger.warn("Leader not present for: " + aMessage);
                }
                
                break;
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }

    public ProposerState getState() {
        return _state;
    }
}

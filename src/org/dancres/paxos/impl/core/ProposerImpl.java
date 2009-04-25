package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        _state.process(aMessage, aSenderAddress, _transport);
    }

    public ProposerState getState() {
        return _state;
    }
}

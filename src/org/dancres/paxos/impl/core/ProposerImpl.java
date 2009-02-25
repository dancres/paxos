package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.messages.PaxosMessage;
import org.dancres.paxos.impl.messages.Operations;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.apache.mina.common.IoSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class ProposerImpl {
    private Logger _logger = LoggerFactory.getLogger(ProposerImpl.class);

    private ProposerState _state;
    private IoSession _session;

    public ProposerImpl(IoSession aSession, FailureDetector aDetector, InetSocketAddress anAddress) {
        _state = new ProposerState(aDetector, anAddress);
        _session = aSession;
    }

    public void process(PaxosMessage aMessage, IoSession aSession) {
        switch (aMessage.getType()) {
            case Operations.POST : {
                _logger.info("Received post - starting leader");

                LeaderImpl myLeader = _state.newLeader(_session);
                myLeader.messageReceived(aMessage);
                break;
            }
            case Operations.LAST :
            case Operations.ACCEPT :
            case Operations.ACK: {
                LeaderImpl myLeader = _state.getLeader(aMessage.getSeqNum());

                if (myLeader != null)
                    myLeader.messageReceived(aMessage);
                else {
                    _logger.warn("Leader not present for: " + aMessage);
                }
                
                break;
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }
}

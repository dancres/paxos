package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class ProposerImpl {
    private Logger _logger = LoggerFactory.getLogger(ProposerImpl.class);

    private ProposerState _state;
    private Channel _channel;

    public ProposerImpl(Channel aChannel, FailureDetector aDetector, InetSocketAddress anAddress) {
        _state = new ProposerState(aDetector, anAddress);
        _channel = aChannel;
    }

    /**
     * @todo channel that is passed in for a POST will be the channel to talk to the client upon, this needs to be made available to the leader
     * in some way so it can inform the client of the result.  But the channel it requires for construction is the broadcast channel for
     * acceptor/learners
     * @param aMessage
     * @param aChannel
     */
    public void process(PaxosMessage aMessage, Channel aChannel) {
        switch (aMessage.getType()) {
            case Operations.POST : {
                _logger.info("Received post - starting leader");

                LeaderImpl myLeader = _state.newLeader(_channel, aChannel);
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

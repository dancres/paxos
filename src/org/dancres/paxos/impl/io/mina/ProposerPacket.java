package org.dancres.paxos.impl.io.mina;

import org.dancres.paxos.impl.core.messages.*;

/**
 * ProposerPacket is passed across transports but is not a valid message for the acceptor state machine.  It is thus an instance of
 * PaxosMessage to keep the comms stack clean and simple but cannot be considered a state machine message.
 * 
 * @author dan
 */
public interface ProposerPacket extends PaxosMessage {
    public int getPort();
    public PaxosMessage getOperation();
}

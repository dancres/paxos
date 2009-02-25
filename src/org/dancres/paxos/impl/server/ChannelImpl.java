package org.dancres.paxos.impl.server;

import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.messages.PaxosMessage;

public class ChannelImpl implements Channel {

    private IoSession _session;

    public ChannelImpl(IoSession aSession) {
        _session = aSession;
    }

    public void write(PaxosMessage aMessage) {
        _session.write(aMessage);
    }
}

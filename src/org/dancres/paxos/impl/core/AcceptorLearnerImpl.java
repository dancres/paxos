package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.*;

public class AcceptorLearnerImpl {
    private AcceptorLearnerState _state;

    public AcceptorLearnerImpl() {
        _state = new AcceptorLearnerState();
    }

    public PaxosMessage process(PaxosMessage aMessage) {
        return _state.process(aMessage);
    }
}

package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.messages.*;

public class AcceptorLearnerImpl {
    private AcceptorLearnerState _state;

    public AcceptorLearnerImpl() {
        _state = new AcceptorLearnerState();
    }

    public PaxosMessage process(PaxosMessage aMessage) {
        switch (aMessage.getType()) {
            case Operations.HEARTBEAT : {
                return null; // Nothing to do
            }
            case Operations.COLLECT : {
                Participant myPart = _state.newParticipant(aMessage.getSeqNum());
                return myPart.process(aMessage);
            }
            case Operations.BEGIN : {
                Participant myPart = _state.getParticipant(aMessage.getSeqNum());
                return myPart.process(aMessage);
            }

            case Operations.SUCCESS : {
                Participant myPart = _state.getParticipant(aMessage.getSeqNum());
                return myPart.process(aMessage);
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }
}

package org.dancres.paxos.impl.core;

import java.util.Map;
import java.util.HashMap;

class AcceptorLearnerState {
    private Map _participants = new HashMap();

    Participant newParticipant(long aSeqNum) {
        synchronized(_participants) {
            Participant myPart = new Participant(aSeqNum, this);
            _participants.put(new Long(aSeqNum), myPart);
            return myPart;
        }
    }

    /**
     * @todo Decide what to do if we don't find a participant
     */
    Participant getParticipant(long aSeqNum) {
        synchronized(_participants) {
            Participant myPart = (Participant) _participants.get(new Long(aSeqNum));

            return myPart;
        }
    }
}

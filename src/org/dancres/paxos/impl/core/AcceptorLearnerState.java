package org.dancres.paxos.impl.core;

import java.util.Map;
import java.util.HashMap;
import org.dancres.paxos.impl.core.messages.Begin;
import org.dancres.paxos.impl.core.messages.Collect;

class AcceptorLearnerState {
    private Map<Long, Participant> _participants = new HashMap<Long, Participant>();
    private Collect _lastCollect = Collect.INITIAL;

    Participant newParticipant(long aSeqNum) {
        synchronized(_participants) {
            Participant myPart = getParticipant(aSeqNum);

            if (myPart == null) {
                myPart = new Participant(aSeqNum, this);
                _participants.put(new Long(aSeqNum), myPart);
            }

            return myPart;
        }
    }

    /**
     * @todo Decide what to do if we don't find a participant
     */
    Participant getParticipant(long aSeqNum) {
        synchronized(_participants) {
            Participant myPart = _participants.get(new Long(aSeqNum));

            return myPart;
        }
    }

    /**
     * @param aCollect should be tested to see if it supercedes the current COLLECT
     * @return the old collect if it's superceded or null
     */
    Collect supercedes(Collect aCollect) {
        synchronized(this) {
            if (aCollect.supercedes(_lastCollect)) {
                Collect myOld = _lastCollect;
                _lastCollect = aCollect;

                return myOld;
            } else {
                return null;
            }
        }
    }

    Collect getLastCollect() {
        synchronized(this) {
            return _lastCollect;
        }
    }

    boolean originates(Begin aBegin) {
        synchronized(this) {
            return aBegin.originates(_lastCollect);
        }
    }

    boolean precedes(Begin aBegin) {
        synchronized(this) {
            return aBegin.precedes(_lastCollect);
        }
    }
}

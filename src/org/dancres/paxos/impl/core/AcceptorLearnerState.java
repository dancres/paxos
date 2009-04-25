package org.dancres.paxos.impl.core;

import java.util.Map;
import java.util.HashMap;
import org.dancres.paxos.impl.core.messages.Begin;
import org.dancres.paxos.impl.core.messages.Collect;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

class AcceptorLearnerState {
    private Map<Long, Participant> _participants = new HashMap<Long, Participant>();
    private Collect _lastCollect = Collect.INITIAL;

    private Participant newParticipant(long aSeqNum) {
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
    private Participant getParticipant(long aSeqNum) {
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

    public PaxosMessage process(PaxosMessage aMessage) {
        switch (aMessage.getType()) {
            case Operations.HEARTBEAT : {
                return null; // Nothing to do
            }
            case Operations.COLLECT : {
                Participant myPart = newParticipant(aMessage.getSeqNum());
                return myPart.process(aMessage);
            }
            case Operations.BEGIN : {
                Participant myPart = newParticipant(aMessage.getSeqNum());
                assert(myPart != null);

                return myPart.process(aMessage);
            }

            case Operations.SUCCESS : {
                Participant myPart = getParticipant(aMessage.getSeqNum());
                assert(myPart != null);

                return myPart.process(aMessage);
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }
}

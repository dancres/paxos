package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;

/**
 * A somewhat out-of-date local AL (up-to-date rnd but old sequence) could seed it's leader (at init) with
 * out of date values that can lead to repeating votes for known values which is wasteful. We avoid that by
 * ensuring that we constrain sequence as well as round numbers on COLLECTs.
 */
class LeaderUtils {
	public boolean supercedes(Transport.Packet aProspective, Transport.Packet aCurrent) {
		Collect myProspective = (Collect) aProspective.getMessage();
		Collect myCurrent = (Collect) aCurrent.getMessage();

		return ((myProspective.getSeqNum() >= myCurrent.getSeqNum()) &&
                (myProspective.getRndNumber() > myCurrent.getRndNumber()));
	}

	public boolean sameLeader(Transport.Packet aProspective, Transport.Packet aCurrent) {
		Collect myProspective = (Collect) aProspective.getMessage();
		Collect myCurrent = (Collect) aCurrent.getMessage();

		return ((myProspective.getSeqNum() >= myCurrent.getSeqNum()) &&
                (myProspective.getRndNumber() >= myCurrent.getRndNumber()) &&
			    (aProspective.getSource().equals(aCurrent.getSource())));
	}

	public boolean originates(Transport.Packet aBegin, Transport.Packet aCollect) {
		Begin myBegin = (Begin) aBegin.getMessage();
		Collect myCollect = (Collect) aCollect.getMessage();

		return ((myBegin.getRndNumber() == myCollect.getRndNumber()) &&
			(aBegin.getSource().equals(aCollect.getSource())));
	}

	public boolean precedes(Transport.Packet aBegin, Transport.Packet aCollect) {
		Begin myBegin = (Begin) aBegin.getMessage();
		Collect myCollect = (Collect) aCollect.getMessage();

		return (myBegin.getRndNumber() < myCollect.getRndNumber());
	}
}
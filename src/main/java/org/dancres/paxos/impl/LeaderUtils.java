package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Begin;

public class LeaderUtils {
	public static boolean supercedes(Transport.Packet aProspective, Transport.Packet aCurrent) {
		Collect myProspective = (Collect) aProspective.getMessage();
		Collect myCurrent = (Collect) aCurrent.getMessage();

		return (myProspective.getRndNumber() > myCurrent.getRndNumber());
	}

	public  static boolean sameLeader(Transport.Packet aProspective, Transport.Packet aCurrent) {
		Collect myProspective = (Collect) aProspective.getMessage();
		Collect myCurrent = (Collect) aCurrent.getMessage();

		return ((myProspective.getRndNumber() >= myCurrent.getRndNumber()) &&
			(myProspective.getNodeId().equals(myCurrent.getNodeId())));
	}

	public static boolean originates(Transport.Packet aBegin, Transport.Packet aCollect) {
		Begin myBegin = (Begin) aBegin.getMessage();
		Collect myCollect = (Collect) aCollect.getMessage();

		return ((myBegin.getRndNumber() == myCollect.getRndNumber()) &&
			(myBegin.getNodeId().equals(myCollect.getNodeId())));
	}

	public static boolean precedes(Transport.Packet aBegin, Transport.Packet aCollect) {
		Begin myBegin = (Begin) aBegin.getMessage();
		Collect myCollect = (Collect) aCollect.getMessage();

		return (myBegin.getRndNumber() < myCollect.getRndNumber());
	}
}
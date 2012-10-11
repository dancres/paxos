package org.dancres.paxos.impl;

import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Begin;

public class LeaderUtils {
	public static boolean supercedes(Collect aProspective, Collect aCurrent) {
		return (aProspective.getRndNumber() > aCurrent.getRndNumber());
	}

	public  static boolean sameLeader(Collect aProspective, Collect aCurrent) {
		return ((aProspective.getRndNumber() >= aCurrent.getRndNumber()) &&
			(aProspective.getNodeId().equals(aCurrent.getNodeId())));
	}

	public static boolean originates(Begin aBegin, Collect aCollect) {
		return ((aBegin.getRndNumber() == aCollect.getRndNumber()) &&
			(aBegin.getNodeId().equals(aCollect.getNodeId())));
	}

	public static boolean precedes(Begin aBegin, Collect aCollect) {
		return (aBegin.getRndNumber() < aCollect.getRndNumber());
	}
}
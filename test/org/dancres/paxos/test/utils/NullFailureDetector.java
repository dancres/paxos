package org.dancres.paxos.test.utils;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.LivenessListener;
import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.NodeId;

public class NullFailureDetector implements FailureDetector {

	public void add(LivenessListener aListener) {
	}

	public boolean amLeader(NodeId aNodeId) {
		return false;
	}

	public NodeId getLeader() {
		return null;
	}

	public Membership getMembers(MembershipListener aListener) {
		return null;
	}

	public long getUnresponsivenessThreshold() {
		return 0;
	}

	public boolean isLive(NodeId aNodeId) {
		return false;
	}
}

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
		return new MembershipImpl(aListener);
	}

	class MembershipImpl implements Membership {
		private MembershipListener _listener;
		private int _responseCount = 2;
		
		MembershipImpl(MembershipListener aListener) {
			_listener = aListener;
		}
		
		public void dispose() {
		}

		public int getMajority() {
			return 2;
		}

		public int getSize() {
			return 2;
		}

		public boolean receivedResponse(NodeId aNodeId) {
			synchronized(this) {
				--_responseCount;
				if (_responseCount == 0)
					_listener.allReceived();
			}
			
			return true;
		}

		public boolean startInteraction() {
			return true;
		}			
	};

	public long getUnresponsivenessThreshold() {
		return 0;
	}

	public boolean isLive(NodeId aNodeId) {
		return false;
	}
}

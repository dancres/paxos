package org.dancres.paxos.test.utils;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.LivenessListener;
import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class NullFailureDetector implements FailureDetector {

	public void add(LivenessListener aListener) {
	}

	public boolean amLeader(InetSocketAddress aInetSocketAddress) {
		return false;
	}

	public InetSocketAddress getLeader() {
		return null;
	}

	public Membership getMembers(MembershipListener aListener) {
		return new MembershipImpl(aListener);
	}

    public Set<InetSocketAddress> getMemberSet() {
        return new HashSet<InetSocketAddress>();
    }

    public byte[] getMetaData(InetSocketAddress aNode) {
        return null;
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

		public boolean receivedResponse(InetSocketAddress aInetSocketAddress) {
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

	public boolean isLive(InetSocketAddress aInetSocketAddress) {
		return false;
	}

	public boolean couldComplete() {
		return true;
	}
	
	public void stop() {		
	}
}

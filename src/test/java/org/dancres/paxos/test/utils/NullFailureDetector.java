package org.dancres.paxos.test.utils;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Membership;
import org.dancres.paxos.impl.MembershipListener;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class NullFailureDetector implements FailureDetector {

	public void add(LivenessListener aListener) {
	}

	public Membership getMembers(MembershipListener aListener) {
		return new MembershipImpl(aListener);
	}

    public Set<InetSocketAddress> getMemberSet() {
        return new HashSet<InetSocketAddress>();
    }

    public InetSocketAddress getRandomMember(InetSocketAddress aLocal) {
        return null;
    }

    public InetSocketAddress getLeader(InetSocketAddress aLocal) {
        return null;
    }

    public byte[] getMetaData(InetSocketAddress aNode) {
        return null;
    }

    public int getMajority() {
        return 2;
    }

    public void processMessage(PaxosMessage aMessage) throws Exception {
    }

    class MembershipImpl implements Membership {
		private MembershipListener _listener;
		private int _responseCount = 2;
		
		MembershipImpl(MembershipListener aListener) {
			_listener = aListener;
		}
		
		public void dispose() {
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

	public boolean isLive(InetSocketAddress aInetSocketAddress) {
		return false;
	}

	public boolean couldComplete() {
		return true;
	}
	
	public void stop() {		
	}
}

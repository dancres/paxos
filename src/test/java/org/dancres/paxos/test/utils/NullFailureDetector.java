package org.dancres.paxos.test.utils;

import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Membership;
import org.dancres.paxos.impl.MembershipListener;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class NullFailureDetector implements FailureDetector {

	public Membership getMembers(MembershipListener aListener) {
		return new MembershipImpl(aListener);
	}

    public Map<InetSocketAddress, MetaData> getMemberMap() {
        return new HashMap<InetSocketAddress, MetaData>();
    }

    public InetSocketAddress getRandomMember(InetSocketAddress aLocal) {
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

	public boolean couldComplete() {
		return true;
	}
	
	public void stop() {		
	}
}

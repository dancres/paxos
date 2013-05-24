package org.dancres.paxos.test.utils;

import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Heartbeater;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class NullFailureDetector implements MessageBasedFailureDetector {

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

	public Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData) {
		return null;
	}

    public void processMessage(Packet aPacket) throws Exception {
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
	}

	public boolean couldComplete() {
		return true;
	}
	
	public void stop() {		
	}
}

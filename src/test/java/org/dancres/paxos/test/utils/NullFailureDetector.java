package org.dancres.paxos.test.utils;

import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Heartbeater;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NullFailureDetector implements MessageBasedFailureDetector {

	public Membership getMembers() {
		return new MembershipImpl();
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
		private int _responseCount = 2;
		
		MembershipImpl() {
		}
		
		public int getSize() {
			return 2;
		}

        public boolean couldComplete() {
            return true;
        }

        public boolean isMajority(Collection<InetSocketAddress> aListOfAddresses) {
            return (aListOfAddresses.size() >= _responseCount);
        }
    }

	public boolean couldComplete() {
		return true;
	}
	
	public void stop() {		
	}
}

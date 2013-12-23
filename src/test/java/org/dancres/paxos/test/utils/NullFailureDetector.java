package org.dancres.paxos.test.utils;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NullFailureDetector extends MessageBasedFailureDetector {

	public Membership getMembers() {
		return new MembershipImpl();
	}

    public Map<InetSocketAddress, MetaData> getMemberMap() {
        return new HashMap<>();
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

    public boolean accepts(Packet aPacket) {
        return aPacket.getMessage().getClassifications().contains(PaxosMessage.Classification.FAILURE_DETECTOR);
    }

    public void processMessage(Packet aPacket) {
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

        public Map<InetSocketAddress, MetaData> getMemberMap() {
            return new HashMap<>();
        }
    }

	public boolean couldComplete() {
		return true;
	}
	
	public void stop() {		
	}
}

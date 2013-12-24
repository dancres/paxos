package org.dancres.paxos.test.utils;

import org.dancres.paxos.Membership;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Heartbeater;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * @deprecated In most cases one can use <code>FailureDetectorImpl</code>, where this is not possible, a private
 * implementation of <code>MessageBasedFailureDetector</code> should be used.
 */
public class NullFailureDetector extends MessageBasedFailureDetector {

	public Membership getMembers() {
		return new MembershipImpl();
	}

    public InetSocketAddress getRandomMember(InetSocketAddress aLocal) {
        return null;
    }

    public int getMajority() {
        return 2;
    }

    public void addListener(StateListener aListener) {
        throw new RuntimeException("Not implemented");
    }

    public Future<Membership> barrier() {
        return null;
    }

    public Future<Membership> barrier(int aRequired) {
        return null;
    }

    public void pin(Collection<InetSocketAddress> aMembers) {
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

	public void stop() {
	}
}

package org.dancres.paxos.test.junit;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.impl.PacketBuffer;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.test.utils.Utils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test PacketBuffer to ensure it performs duplicate elimination.
 */
public class PacketBufferTest {

	@Test public void testCollect() {
		PacketBuffer myBuffer = new PacketBuffer();
		
		Collect myCollect1 = new Collect(1, 1, Utils.getTestAddress());
		Collect myCollect2 = new Collect(2, 1, Utils.getTestAddress());
		
		myBuffer.add(myCollect1);
		myBuffer.add(myCollect2);
		myBuffer.add(myCollect2);
		
		int my1Count = 0;
		int my2Count = 0;
		
		PaxosMessage myMessage;
		
		while ((myMessage = myBuffer.await(new Filter(), 500)) != null) {
			Collect myCollect = (Collect) myMessage;
			
			switch	((int) myCollect.getSeqNum()) {
				case 1 : ++my1Count; break;
				case 2 : ++my2Count; break;
			}
		}
		
		Assert.assertEquals(my1Count, 1);
		Assert.assertEquals(my2Count, 1);		
	}
	
	@Test public void testBegin() {
		PacketBuffer myBuffer = new PacketBuffer();
		
		Begin myBegin1 = new Begin(1, 1, new ConsolidatedValue(), Utils.getTestAddress());
		Begin myBegin2 = new Begin(2, 1, new ConsolidatedValue(), Utils.getTestAddress());
		
		myBuffer.add(myBegin1);
		myBuffer.add(myBegin2);
		myBuffer.add(myBegin2);
		
		int my1Count = 0;
		int my2Count = 0;
		
		PaxosMessage myMessage;
		
		while ((myMessage = myBuffer.await(new Filter(), 500)) != null) {
			Begin myBegin = (Begin) myMessage;
			
			switch	((int) myBegin.getSeqNum()) {
				case 1 : ++my1Count; break;
				case 2 : ++my2Count; break;
			}
		}
		
		Assert.assertEquals(my1Count, 1);
		Assert.assertEquals(my2Count, 1);		
	}

	@Test public void testSuccess() {
		PacketBuffer myBuffer = new PacketBuffer();
		
		Success mySuccess1 = new Success(1, 1, Utils.getTestAddress());
		Success mySuccess2 = new Success(2, 1, Utils.getTestAddress());
		
		myBuffer.add(mySuccess1);
		myBuffer.add(mySuccess2);
		myBuffer.add(mySuccess2);
		
		int my1Count = 0;
		int my2Count = 0;
		
		PaxosMessage myMessage;
		
		while ((myMessage = myBuffer.await(new Filter(), 500)) != null) {
			Success mySuccess = (Success) myMessage;
			
			switch	((int) mySuccess.getSeqNum()) {
				case 1 : ++my1Count; break;
				case 2 : ++my2Count; break;
			}
		}
		
		Assert.assertEquals(my1Count, 1);
		Assert.assertEquals(my2Count, 1);		
	}
	
	class Filter implements PacketBuffer.PaxosFilter {

		public boolean interested(PaxosMessage aMessage) {
			return true;
		}		
	}
}

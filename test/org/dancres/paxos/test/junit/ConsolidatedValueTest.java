package org.dancres.paxos.test.junit;

import org.dancres.paxos.Proposal;
import org.junit.*;

public class ConsolidatedValueTest {
	@Test public void testEquals() {
		Proposal myVal1 = new Proposal("abc", "def".getBytes());		
		Proposal myVal2 = new Proposal("def", "abc".getBytes());
		myVal2.put("abc", "def".getBytes());
		
		Assert.assertTrue(myVal1 == myVal1);
		Assert.assertTrue(myVal2 == myVal2);
		Assert.assertTrue(myVal1 != myVal2);
	}
}

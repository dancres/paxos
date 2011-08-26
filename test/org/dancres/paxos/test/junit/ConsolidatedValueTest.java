package org.dancres.paxos.test.junit;

import org.dancres.paxos.ConsolidatedValue;
import org.junit.*;

public class ConsolidatedValueTest {
	@Test public void testEquals() {
		ConsolidatedValue myVal1 = new ConsolidatedValue("abc", "def".getBytes());		
		ConsolidatedValue myVal2 = new ConsolidatedValue("def", "abc".getBytes());
		myVal2.put("abc", "def".getBytes());
		
		Assert.assertTrue(myVal1 == myVal1);
		Assert.assertTrue(myVal2 == myVal2);
		Assert.assertTrue(myVal1 != myVal2);
	}
}

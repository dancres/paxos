package org.dancres.paxos.test.junit;

import org.dancres.paxos.FailureDetector;
import org.junit.Assert;

public class FDUtil {
    public static void ensureFD(FailureDetector anFD) throws Exception {
        int myChances = 0;

        while (!anFD.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }
    }
}

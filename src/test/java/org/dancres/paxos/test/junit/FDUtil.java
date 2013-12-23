package org.dancres.paxos.test.junit;

import org.dancres.paxos.FailureDetector;
import org.junit.Assert;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class FDUtil {
    public static void ensureFD(FailureDetector anFD) throws Exception {
        for (int i = 0; i < 4; i++) {
            if (anFD.barrier().get(5000, TimeUnit.MILLISECONDS))
                return;
        }

        Assert.assertTrue("Membership not achieved", false);
    }
}

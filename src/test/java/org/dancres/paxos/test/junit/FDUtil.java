package org.dancres.paxos.test.junit;

import org.dancres.paxos.FailureDetector;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FDUtil {
    public static void ensureFD(FailureDetector anFD) throws Exception {
        testFD(anFD);
    }

    public static void testFD(FailureDetector anFD) throws Exception {
        Assert.assertTrue("Membership not achieved", testFD(anFD, 20000));
    }

    public static boolean testFD(FailureDetector anFD, long aTimeout) throws Exception {
        try {
            if (anFD.barrier().get(aTimeout, TimeUnit.MILLISECONDS) != null)
                return true;
        } catch (TimeoutException aTE) {
        }

        return false;
    }
}

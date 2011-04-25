package org.dancres.paxos.test.junit;

import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.codec.Codecs;
import org.junit.Assert;
import org.junit.Test;

public class CollectTest {
    @Test
    public void collect() throws Exception {
        Collect myCollect = Collect.INITIAL;
        Collect myCollect2 = Collect.INITIAL;

        Assert.assertEquals(myCollect2, Collect.INITIAL);

        byte[] myBuffer = Codecs.encode(myCollect);

        myCollect2 = (Collect) Codecs.decode(myBuffer);

        Assert.assertEquals(myCollect2, Collect.INITIAL);
    }
}

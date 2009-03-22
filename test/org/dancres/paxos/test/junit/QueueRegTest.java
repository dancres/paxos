package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.ChannelRegistry;
import org.dancres.paxos.test.utils.PacketQueueImpl;
import org.dancres.paxos.test.utils.QueueChannelImpl;
import org.junit.*;
import org.junit.Assert.*;

public class QueueRegTest {
    @Test public void test() throws Exception {
        AddressGenerator myGen = new AddressGenerator();
        ChannelRegistry myQr = new ChannelRegistry();

        InetSocketAddress myFirstAddr = myGen.allocate();
        InetSocketAddress mySecondAddr = myGen.allocate();

        Channel myFirstQ = new QueueChannelImpl(myFirstAddr, new PacketQueueImpl());
        Channel mySecondQ = new QueueChannelImpl(mySecondAddr, new PacketQueueImpl());


        myQr.register(myFirstAddr, myFirstQ);
        myQr.register(mySecondAddr, mySecondQ);

        Assert.assertEquals(myFirstQ, myQr.getChannel(myFirstAddr));
        Assert.assertEquals(mySecondQ, myQr.getChannel(mySecondAddr));
    }
}

package org.dancres.paxos.test.junit;

import java.net.SocketAddress;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.PacketQueue;
import org.dancres.paxos.test.utils.QueueRegistry;
import org.junit.*;
import org.junit.Assert.*;

public class QueueRegTest {
    @Test public void test() throws Exception {
        AddressGenerator myGen = new AddressGenerator();
        QueueRegistry myQr = new QueueRegistry();

        PacketQueue myFirstQ = new PacketQueue();
        PacketQueue mySecondQ = new PacketQueue();

        SocketAddress myFirstAddr = myGen.allocate();
        SocketAddress mySecondAddr = myGen.allocate();

        myQr.register(myFirstAddr, myFirstQ);
        myQr.register(mySecondAddr, mySecondQ);

        Assert.assertEquals(myFirstQ, myQr.getQueue(myFirstAddr));
        Assert.assertEquals(mySecondQ, myQr.getQueue(mySecondAddr));
    }
}

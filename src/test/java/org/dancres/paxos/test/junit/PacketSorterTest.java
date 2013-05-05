package org.dancres.paxos.test.junit;

import junit.framework.Assert;
import org.dancres.paxos.impl.PacketSorter;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Need;
import org.dancres.paxos.test.net.FakePacket;
import org.junit.Test;

public class PacketSorterTest {
    @Test
    public void checkConsume() {
        PacketSorter mySorter = new PacketSorter();
        Tester myTester = new Tester(false);

        for (long mySeq = 0; mySeq < 5; mySeq++) {
            mySorter.process(new FakePacket(new Collect(mySeq, 1)), mySeq - 1, myTester);
        }

        Assert.assertEquals(5, myTester._consumed);
        Assert.assertEquals(false, myTester._recoveryRequested);
        Assert.assertEquals(0, mySorter.numPackets());
    }

    @Test
    public void checkPositiveRecoverTrigger() {
        PacketSorter mySorter = new PacketSorter();
        Tester myTester = new Tester(true);

        mySorter.process(new FakePacket(new Collect(5, 1)), 0, myTester);

        Assert.assertEquals(0, myTester._consumed);
        Assert.assertEquals(true, myTester._recoveryRequested);
        Assert.assertEquals(1, mySorter.numPackets());
    }

    @Test
    public void checkNegativeRecoverTrigger() {
        PacketSorter mySorter = new PacketSorter();
        Tester myTester = new Tester(false);

        mySorter.process(new FakePacket(new Collect(4, 1)), 0, myTester);
        mySorter.process(new FakePacket(new Collect(5, 1)), 0, myTester);

        Assert.assertEquals(0, myTester._consumed);
        Assert.assertEquals(true, myTester._recoveryRequested);
        Assert.assertEquals(2, mySorter.numPackets());
    }

    class Tester implements PacketSorter.PacketProcessor {
        int _consumed = 0;
        boolean _recoveryRequested = false;

        private boolean _recoveryReturn;

        Tester(boolean aRecoveryReturn) {
            _recoveryReturn = aRecoveryReturn;
        }

        public void consume(Transport.Packet aPacket) {
            _consumed++;
        }

        public boolean recover(Need aNeed) {
            _recoveryRequested = true;
            return _recoveryReturn;
        }
    }
}

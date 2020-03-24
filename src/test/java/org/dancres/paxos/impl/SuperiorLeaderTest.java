package org.dancres.paxos.impl;

import java.nio.ByteBuffer;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.messages.*;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.utils.Builder;
import org.junit.*;

public class SuperiorLeaderTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        Builder myBuilder = new Builder();

        _tport1 = myBuilder.newDefaultStack();

        _tport2 = myBuilder.newRxFilteredTransport(new Dropping());
        myBuilder.newCoreWith(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProposal = new Proposal("data", myBuffer.array());
        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        myClient.send(new Envelope(myProposal), _tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.OTHER_LEADER);
    }

    class Dropping implements Transport.Filter {

        @Override
        public Packet filter(Transport aTransport, Packet aPacket) {
            PaxosMessage myMessage = aPacket.getMessage();

            if (myMessage.getType() == PaxosMessage.Types.COLLECT) {
                Collect myCollect = (Collect) myMessage;

                aTransport.send(
                        aTransport.getPickler().newPacket(new OldRound(myCollect.getSeqNum(),
                                aTransport.getLocalAddress(),
                                myCollect.getRndNumber() + 1)), aPacket.getSource());

                return null;
            } else
                return aPacket;
        }
    }
}

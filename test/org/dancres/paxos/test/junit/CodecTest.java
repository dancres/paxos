package org.dancres.paxos.test.junit;

import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.codec.Codec;
import org.dancres.paxos.impl.codec.Codecs;
import org.dancres.paxos.impl.core.messages.Accept;
import org.dancres.paxos.impl.core.messages.Ack;
import org.dancres.paxos.impl.core.messages.Begin;
import org.dancres.paxos.impl.core.messages.Collect;
import org.dancres.paxos.impl.core.messages.Fail;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.impl.core.messages.Last;
import org.dancres.paxos.impl.core.messages.OldRound;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.io.mina.Post;
import org.dancres.paxos.impl.core.messages.ProposerHeader;
import org.dancres.paxos.impl.core.messages.Success;
import org.dancres.paxos.impl.io.mina.ClientMessage;
import org.junit.*;
import org.junit.Assert.*;

public class CodecTest {

    private byte[] encode(PaxosMessage aMessage) {
        int myType = aMessage.getType();
        Codec myCodec = Codecs.CODECS[myType];

        return myCodec.encode(aMessage).array();
    }

    private byte[] encode(ClientMessage aMessage) {
        int myType = aMessage.getType();
        Codec myCodec = Codecs.CODECS[myType];

        return myCodec.encode(aMessage).array();
    }

    private Object decode(byte[] aBuffer, boolean hasLength) {
        IoBuffer myBuffer = IoBuffer.wrap(aBuffer);
        int myOp;
        
        if (hasLength)
            myOp = myBuffer.getInt(4);
        else
            myOp = myBuffer.getInt(0);

        Codec myCodec = Codecs.CODECS[myOp];

        return myCodec.decode(myBuffer);
    }

    @Test public void accept() throws Exception {
        Accept myAccept = new Accept(1, 2);

        byte[] myBuffer = encode(myAccept);

        Accept myAccept2 = (Accept) decode(myBuffer, true);

        Assert.assertTrue(myAccept.getRndNumber() == myAccept2.getRndNumber());
        Assert.assertTrue(myAccept.getSeqNum() == myAccept2.getSeqNum());
    }

    @Test public void ack() throws Exception {
        Ack myAck = new Ack(1);

        byte[] myBuffer = encode(myAck);

        Ack myAck2 = (Ack) decode(myBuffer, true);

        Assert.assertTrue(myAck.getSeqNum() == myAck2.getSeqNum());
    }

    @Test public void fail() throws Exception {
        Fail myFail = new Fail(1, 2);

        byte[] myBuffer = encode(myFail);

        Fail myFail2 = (Fail) decode(myBuffer, true);

        Assert.assertTrue(myFail.getSeqNum() == myFail2.getSeqNum());
        Assert.assertTrue(myFail.getReason() == myFail2.getReason());
    }

    @Test public void begin() throws Exception {
        byte[] myData = {55};

        Begin myBegin = new Begin(1, 2, 3, myData);

        byte[] myBuffer = encode(myBegin);

        Begin myBegin2 = (Begin) decode(myBuffer, false);

        Assert.assertEquals(myBegin.getSeqNum(), myBegin2.getSeqNum());
        Assert.assertEquals(myBegin.getRndNumber(), myBegin2.getRndNumber());
        Assert.assertEquals(myBegin.getNodeId(), myBegin2.getNodeId());
        Assert.assertEquals(myBegin.getValue().length, myBegin2.getValue().length);
        Assert.assertEquals(myBegin.getValue()[0], myBegin2.getValue()[0]);
    }

    @Test public void collect() throws Exception {
        Collect myCollect = new Collect(1, 2, 3);

        byte[] myBuffer = encode(myCollect);

        Collect myCollect2 = (Collect) decode(myBuffer, false);

        Assert.assertEquals(myCollect.getSeqNum(), myCollect.getSeqNum());
        Assert.assertEquals(myCollect.getRndNumber(), myCollect2.getRndNumber());
        Assert.assertEquals(myCollect.getNodeId(), myCollect2.getNodeId());
    }

    @Test public void heartbeat() throws Exception {
        Heartbeat myHeartbeat = new Heartbeat();

        byte[] myBuffer = encode(myHeartbeat);

        Heartbeat myHeartbeat2 = (Heartbeat) decode(myBuffer, true);
    }

    @Test public void last() throws Exception {
        byte[] myData = {55};

        Last myLast = new Last(0, 1, 2, 3, myData);

        byte[] myBuffer = encode(myLast);

        Last myLast2 = (Last) decode(myBuffer, true);

        Assert.assertEquals(myLast.getSeqNum(), myLast.getSeqNum());
        Assert.assertEquals(myLast.getLowWatermark(), myLast2.getLowWatermark());
        Assert.assertEquals(myLast.getHighWatermark(), myLast2.getHighWatermark());
        Assert.assertEquals(myLast.getRndNumber(), myLast2.getRndNumber());
        Assert.assertEquals(myLast.getValue().length, myLast2.getValue().length);
        Assert.assertEquals(myLast.getValue()[0], myLast2.getValue()[0]);
    }

    @Test public void oldRound() throws Exception {
        OldRound myOldRound = new OldRound(1, 2, 3);

        byte[] myBuffer = encode(myOldRound);

        OldRound myOldRound2 = (OldRound) decode(myBuffer, true);

        Assert.assertEquals(myOldRound.getSeqNum(), myOldRound2.getSeqNum());
        Assert.assertEquals(myOldRound.getNodeId(), myOldRound2.getNodeId());
        Assert.assertEquals(myOldRound.getLastRound(), myOldRound2.getLastRound());
    }

    @Test public void post() throws Exception {
        byte[] myData = {55};

        Post myPost = new Post(myData);

        byte[] myBuffer = encode(myPost);

        Post myPost2 = (Post) decode(myBuffer, true);

        Assert.assertEquals(myPost.getValue().length, myPost2.getValue().length);
        Assert.assertEquals(myPost.getValue()[0], myPost2.getValue()[0]);
    }

    @Test public void success() throws Exception {
        byte[] myData = {55};

        Success mySuccess = new Success(1, myData);

        byte[] myBuffer = encode(mySuccess);

        Success mySuccess2 = (Success) decode(myBuffer, false);

        Assert.assertEquals(mySuccess.getSeqNum(), mySuccess2.getSeqNum());
        Assert.assertEquals(mySuccess.getValue().length, mySuccess2.getValue().length);
        Assert.assertEquals(mySuccess.getValue()[0], mySuccess2.getValue()[0]);
    }

    @Test public void proposerHeader() throws Exception {
        ProposerHeader myHeader = new ProposerHeader(new Collect(1, 2, 3), 1);

        byte[] myBuffer = encode(myHeader);

        ProposerHeader myHeader2 = (ProposerHeader) decode(myBuffer, true);

        Assert.assertEquals(myHeader.getPort(), myHeader2.getPort());
        Assert.assertEquals(myHeader.getOperation().getType(), myHeader2.getOperation().getType());

        Collect myCollect = (Collect) myHeader.getOperation();
        Collect myCollect2 = (Collect) myHeader2.getOperation();

        Assert.assertEquals(myCollect.getSeqNum(), myCollect2.getSeqNum());
        Assert.assertEquals(myCollect.getRndNumber(), myCollect2.getRndNumber());
        Assert.assertEquals(myCollect.getNodeId(), myCollect2.getNodeId());
    }

    private void dump(byte[] aBuffer) {
        for (int i = 0; i < aBuffer.length; i++) {
            System.err.print(Integer.toHexString(aBuffer[i]) + " ");
        }

        System.err.println();
    }
}

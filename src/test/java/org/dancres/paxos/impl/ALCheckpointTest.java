package org.dancres.paxos.impl;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.NullFailureDetector;
import org.dancres.paxos.test.net.Utils;
import org.dancres.paxos.test.net.StandalonePickler;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ALCheckpointTest {
    private static final String DIRECTORY = "howllogs";

    private InetSocketAddress _nodeId = Utils.getTestAddress();
    private InetSocketAddress _broadcastId = Utils.getTestAddress();

    private class TransportImpl implements Transport {
        private Transport.PacketPickler _pickler = new StandalonePickler(_nodeId);

        public void routeTo(Dispatcher aDispatcher) {
        }

        public Transport.PacketPickler getPickler() {
            return _pickler;
        }

        private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

        public void send(Packet aPacket, InetSocketAddress aNodeId) {
            synchronized(_messages) {
                _messages.add(aPacket.getMessage());
            }
        }

        PaxosMessage getNextMsg() {
            synchronized(_messages) {
                return _messages.remove(0);
            }
        }

        public InetSocketAddress getLocalAddress() {
            return _nodeId;
        }

        public InetSocketAddress getBroadcastAddress() {
            return _broadcastId;
        }

        public void terminate() {
        }

        public void connectTo(InetSocketAddress aNodeId, ConnectionHandler aHandler) {
        }
    }

    @Before
    public void init() throws Exception {
        FileSystem.deleteDirectory(new File(DIRECTORY));
    }

    @Test
    public void test() throws Exception {
        HowlLogger myLogger = new HowlLogger(DIRECTORY);
        TransportImpl myTransport = new TransportImpl();

        AcceptorLearner myAl =
                new AcceptorLearner(myLogger, new Common(myTransport, new NullFailureDetector()), new Listener() {
                    public void transition(StateEvent anEvent) {
                    }
                });

        myAl.open(CheckpointHandle.NO_CHECKPOINT);
        CheckpointHandle myHandle = myAl.newCheckpoint();
        myHandle.saved();
        myAl.close();

        myAl = new AcceptorLearner(myLogger, new Common(myTransport, new NullFailureDetector()), new Listener() {
            public void transition(StateEvent anEvent) {
            }
        });

        myAl.open(myHandle);
        myAl.close();

        ByteArrayOutputStream myBAOS = new ByteArrayOutputStream();
        ObjectOutputStream myOOS = new ObjectOutputStream(myBAOS);

        myOOS.writeObject(myHandle);
        myOOS.close();

        ByteArrayInputStream myBAIS = new ByteArrayInputStream(myBAOS.toByteArray());
        ObjectInputStream myOIS = new ObjectInputStream(myBAIS);
        CheckpointHandle myRecoveredHandle = (CheckpointHandle) myOIS.readObject();

        myAl = new AcceptorLearner(myLogger, new Common(myTransport, new NullFailureDetector()), new Listener() {
            public void transition(StateEvent anEvent) {
            }
        });

        myAl.open(myRecoveredHandle);
        myAl.close();
    }

    public static void main(String[] anArgs) throws Exception {
        ALStartupTest myTest = new ALStartupTest();
        myTest.init();
        myTest.test();
    }
}

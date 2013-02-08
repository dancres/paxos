package org.dancres.paxos.test;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.OrderedMemoryNetwork;
import org.dancres.paxos.test.net.OrderedMemoryTransportImpl;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.MemoryCheckpointStorage;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Need a statistical failure model with varying probabilities for each thing within a tolerance / order
 * For normalisation against a test duration, one year is 525600 minutes.
 * 
 * <ol>
 * <li>Hard disk failure - 6 to 10% </li>
 * <li>Node failure - 1 to 2% up to 15 minutes to recover</li>
 * <li>DC-wide network failure - 0 to 1% up to  5.26 minutes/year</li>
 * <li>Rack-level failure - 0 to 1% downtime up to 8.76 hours/year</li>
 * <li>Link-level failure - 0 to 1% up to 52.56 minutes/year</li>
 * <li>DC-wide packet loss - 1 to 2% with loss of 2 to 10's packets</li>
 * <li>Rack-level packet loss - 1 to 2% with loss of 2 to 10's packets</li>
 * <li>Link-level packet loss - 1 to 2% with loss of 2 to 10's packets</li>
 * </ol>
 *
 * Each of these will also have a typical time to fix. In essence we want an MTBF/MTRR model.
 *
 * @todo Implement node recovery in face of reports of OUT_OF_DATE.
 * @todo Implement failure model (might affect design for OUT_OF_DATE handling).
 */
public class LongTerm {
    private static final String BASEDIR = "/Volumes/LaCie/paxoslogs/";

    static interface Args {
        @Option(defaultValue="100")
        long getCkptCycle();

        @Option(defaultValue="0")
        long getSeed();

        @Option(defaultValue="200")
        int getCycles();

        @Option
        boolean isCalibrate();
    }

    private static class Factory implements OrderedMemoryNetwork.Factory {
        private int _nextNodeNum = 0;
        private Environment _environment;

        Factory(Environment anEnv) {
            _environment = anEnv;
        }

        public OrderedMemoryNetwork.OrderedMemoryTransport newTransport(InetSocketAddress aLocalAddr,
                                                                        InetSocketAddress aBroadcastAddr,
                                                                        OrderedMemoryNetwork aNetwork) {
            TestTransport myTp = new TestTransport(aLocalAddr, aBroadcastAddr, aNetwork, _nextNodeNum);

            _environment._nodes.add(myTp);
            _nextNodeNum++;

            return myTp;
        }

        class TestTransport implements OrderedMemoryNetwork.OrderedMemoryTransport, NodeAdmin {
            private OrderedMemoryTransportImpl _transport;
            private ServerDispatcher _dispatcher;
            private CheckpointStorage _ckptStorage;

            TestTransport(InetSocketAddress aLocalAddr,
                      InetSocketAddress aBroadcastAddr,
                      OrderedMemoryNetwork aNetwork,
                      int aNodeNum) {
                _ckptStorage = new MemoryCheckpointStorage();
                _transport = new OrderedMemoryTransportImpl(aLocalAddr, aBroadcastAddr, aNetwork);

                FileSystem.deleteDirectory(new File(BASEDIR + "node" + Integer.toString(aNodeNum) + "logs"));

                _dispatcher =
                        new ServerDispatcher(new FailureDetectorImpl(3, 5000),
                                new HowlLogger(BASEDIR + "node" + Integer.toString(aNodeNum) + "logs"));

                try {
                    _transport.add(_dispatcher);
                } catch (Exception anE) {
                    throw new RuntimeException("Failed to add a dispatcher", anE);
                }
            }

            public void distribute(Packet aPacket) {
                _transport.distribute(aPacket);
            }

            public PacketPickler getPickler() {
                return _transport.getPickler();
            }

            public void add(Dispatcher aDispatcher) throws Exception {
                _transport.add(aDispatcher);
            }

            public InetSocketAddress getLocalAddress() {
                return _transport.getLocalAddress();
            }

            public InetSocketAddress getBroadcastAddress() {
                return _transport.getBroadcastAddress();
            }

            public void send(PaxosMessage aMessage, InetSocketAddress anAddr) {
                _transport.send(aMessage, anAddr);
            }

            public void connectTo(InetSocketAddress anAddr, ConnectionHandler aHandler) {
                _transport.connectTo(anAddr, aHandler);
            }

            public void terminate() {
                _transport.terminate();
            }

            public Transport getTransport() {
                return this;
            }

            public void checkpoint() throws Exception {
                CheckpointHandle myHandle = _dispatcher.getAcceptorLearner().newCheckpoint();
                CheckpointStorage.WriteCheckpoint myCkpt = _ckptStorage.newCheckpoint();
                ObjectOutputStream myStream = new ObjectOutputStream(myCkpt.getStream());
                myStream.writeObject(myHandle);
                myStream.close();

                myCkpt.saved();
                myHandle.saved();

                assert(_ckptStorage.numFiles() == 1);
            }
        }
    }

    interface NodeAdmin {
        Transport getTransport();
        void checkpoint() throws Exception;
        void terminate();
    }

    private class Environment {
        final boolean _calibrate;
        final long _maxCycles;
        final long _ckptCycle;
        final Random _rng;
        final List<NodeAdmin> _nodes = new LinkedList<NodeAdmin>();

        Transport _currentLeader;
        final OrderedMemoryNetwork _factory;
        final Factory _tpFactory;

        long _opCount = 0;

        Environment(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle) throws Exception {
            _ckptCycle = aCkptCycle;
            _calibrate = doCalibrate;
            _maxCycles = aCycles;
            _rng = new Random(aSeed);
            _tpFactory = new Factory(this);
            _factory = new OrderedMemoryNetwork();

            for (int i = 0; i < 5; i++) {
                _factory.newTransport(_tpFactory);
            }

            _currentLeader = _nodes.get(0).getTransport();
        }

        void updateLeader(InetSocketAddress anAddr) {
            for (NodeAdmin myNA : _nodes) {
                Transport myTp = myNA.getTransport();

                if (myTp.getLocalAddress().equals(anAddr)) {
                    _currentLeader = myTp;
                    break;
                }
            }
        }
    }

    private Environment _env;

    private LongTerm(long aSeed, long aCycles, boolean doCalibrate, long aCkptCycle) throws Exception {
        _env = new Environment(aSeed, aCycles, doCalibrate, aCkptCycle);
    }

    public static void main(String[] anArgs) throws Exception {
        Args myArgs = CliFactory.parseArguments(Args.class, anArgs);
        
        long myStart = System.currentTimeMillis();

        new LongTerm(myArgs.getSeed(), myArgs.getCycles(), myArgs.isCalibrate(), myArgs.getCkptCycle()).run();

        double myDuration = (System.currentTimeMillis() - myStart) / 1000.0;

        System.out.println("Run for " + myArgs.getCycles() + " cycles took " + myDuration + " seconds");

        if (myArgs.isCalibrate()) {
            double myOpsPerSec = myDuration / myArgs.getCycles();
            double myOpsHour = myOpsPerSec * 60 * 60;

            System.out.println("Calibration recommendation - ops/sec: " + myOpsPerSec + 
                " iterations in an hour would be: " + myOpsHour);
        }
    }

    private void run() throws Exception {
        ClientDispatcher myClient = new ClientDispatcher();
        Transport myTransport = _env._factory.newTransport(null);
        myTransport.add(myClient);

        long opsSinceCkpt = 0;

        if (! _env._calibrate) {
            // Setup failure model
        }

        while (_env._opCount < _env._maxCycles) {
            /*
             * Perform a paxos vote - need to react to other leader messages but ignore all else - allows us to
             * cope with strategies that switch our leader to test out election and recover from them (assuming we
             * get the right response)
             */
            ByteBuffer myBuffer = ByteBuffer.allocate(8);
            myBuffer.putLong(_env._opCount);
            Proposal myProposal = new Proposal("data", myBuffer.array());

            myClient.send(new Envelope(myProposal),
                    _env._currentLeader.getLocalAddress());

            VoteOutcome myEv = myClient.getNext(10000);

            ++opsSinceCkpt;

            if (myEv.getResult() == VoteOutcome.Reason.OTHER_LEADER) {
                _env.updateLeader(myEv.getLeader());
            } else if (myEv.getResult() == VoteOutcome.Reason.DECISION) {
                if (opsSinceCkpt >= _env._ckptCycle) {
                    for (NodeAdmin myNA : _env._nodes) {
                        try {
                            myNA.checkpoint();
                        } catch (Throwable aT) {
                            System.out.println("Exception at checkpoint");
                            aT.printStackTrace(System.out);
                        }
                    }

                    opsSinceCkpt = 0;
                }
            }

            // Round we go again
            //
            _env._opCount++;
        }

        for (NodeAdmin myNA : _env._nodes)
            myNA.terminate();

        _env._factory.stop();
    }
}

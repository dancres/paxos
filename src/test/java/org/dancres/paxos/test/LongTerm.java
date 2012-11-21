package org.dancres.paxos.test;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.test.utils.FileSystem;
import org.dancres.paxos.test.utils.OrderedMemoryTransportFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * This test is not yet deterministic enough. We need a deterministic packet delivery structure to ensure
 * things are dispatched appropriately. This might best be effected by creating some form of transport with
 * queuing that can be processed at the behest of the thread that makes the leader requests. The transport
 * could potentially share one queue with all other instances of the transport. This transport or an
 * instance thereof would be passed ServerDispatchers.
 *
 * An alternative is to create a separate "network" that runs off a single ordered queue and one thread
 * simulating failure in accordance with the RNG used for all else. Similar failure schedules could be
 * applied at logger level etc. So  we create all the components and have them generate a failure
 * schedule meaningful to themselves (e.g. fail a logfile after the 20th write). Conceptually the single
 * thread and its queue are a network switch with a number of connections each with an address 
 * (transport implementations). The switch delivers packets to the connection with the appropriate address.
 * Obviously, the broadcast address results in an ordered delivery of one packet to all connections.
 * Ordering could be done on port number which is assigned via test.Utils as they are atomic and
 * monotonically increasing.
 */
public class LongTerm {
    private static final long MAX_CYCLES = 20000;
    private static final long CKPT_CYCLES = 10000;
    private static final String BASEDIR = "/Volumes/LaCie/paxoslogs/";

    private class Environment {
        final Random _rng;
        final Map<Long, Strategy> _actions = new TreeMap<Long, Strategy>();
        final List<ServerDispatcher> _servers = new LinkedList<ServerDispatcher>();
        Transport _currentLeader;
        final OrderedMemoryTransportFactory _factory;

        long _opCount = 0;

        Environment(long aSeed) throws Exception {
            _rng = new Random(aSeed);
            _factory = new OrderedMemoryTransportFactory();

            for (int i = 0; i < 5; i++) {
                FileSystem.deleteDirectory(new File(BASEDIR + "node" + Integer.toString(i) + "logs"));

                ServerDispatcher myDisp =
                        new ServerDispatcher(new FailureDetectorImpl(3, 5000),
                                new HowlLogger(BASEDIR + "node" + Integer.toString(i) + "logs"));

                Transport myTp = _factory.newTransport();
                myTp.add(myDisp);

                _servers.add(myDisp);
            }

            _currentLeader = _servers.get(0).getTransport();
        }

        void updateLeader(InetSocketAddress anAddr) {
            for (ServerDispatcher mySD : _servers) {
                Transport myTp = mySD.getTransport();

                if (myTp.getLocalAddress().equals(anAddr)) {
                    _currentLeader = myTp;
                    break;
                }
            }
        }
    }

    private static final Strategy[] _happenings = new Strategy[]{new Fake()};

    private Environment _env;

    private LongTerm(long aSeed) throws Exception {
        _env = new Environment(aSeed);
    }

    public static void main(String[] anArgs) throws Exception {
        LongTerm myTest;

        if (anArgs.length == 0)
            myTest = new LongTerm(0);
        else
            myTest = new LongTerm(Long.parseLong(anArgs[0]));

        myTest.run();
    }

    private void run() throws Exception {
        ClientDispatcher myClient = new ClientDispatcher();
        Transport myTransport = _env._factory.newTransport();
        myTransport.add(myClient);

        long opsSinceCkpt = 0;

        for (long i = 0; i < MAX_CYCLES; i++) {
            // Pick a percent chance of a failure
            //
            int myChancePercent = _env._rng.nextInt(101);

            int myResultPercent = _env._rng.nextInt(101);

            if (myResultPercent <= myChancePercent) {
                if (_env._actions.get(i) == null)
                    _env._actions.put(i, _happenings[_env._rng.nextInt(_happenings.length)]);
            }
        }

        while (_env._opCount < MAX_CYCLES) {

            // If there's an action this round
            //
            Strategy myAction = _env._actions.remove(new Long(_env._opCount));

            if (myAction != null)
                myAction.execute(_env);

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
                if (opsSinceCkpt >= CKPT_CYCLES) {
                    System.err.println("Marking");

                    for (ServerDispatcher mySD : _env._servers) {
                        CheckpointHandle myHandle = mySD.getAcceptorLearner().newCheckpoint();
                        myHandle.saved();
                    }

                    opsSinceCkpt = 0;
                }
            }

            // Round we go again
            //
            _env._opCount++;
        }

        for (ServerDispatcher mySd : _env._servers)
            mySd.stop();

        _env._factory.stop();
    }

    /**
     * network outages, message delays, timeouts, process crashes and recoveries, file corruptions,
     * schedule interleavings, leader switch, etc.
     */
    public interface Strategy {
        public void execute(Environment anEnv);
    }

    private static class Fake implements Strategy {
        public void execute(Environment anEnv) {
            System.err.println("Failure event");
        }
    }
}

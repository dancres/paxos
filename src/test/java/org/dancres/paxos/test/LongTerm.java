package org.dancres.paxos.test;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.CheckpointStorage;
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
import org.dancres.paxos.test.utils.MemoryCheckpointStorage;
import org.dancres.paxos.test.utils.OrderedMemoryTransportFactory;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.Cli;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.Option;

import java.io.File;
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
 */
public class LongTerm {
    private static final long CKPT_CYCLES = 100;
    private static final String BASEDIR = "/Volumes/LaCie/paxoslogs/";

    static interface Args {
        @Option(defaultValue="0")
        long getSeed();

        @Option(defaultValue="200")
        int getCycles();

        @Option
        boolean isCalibrate();
    }

    private class Environment {
        final boolean _calibrate;
        final long _maxCycles;
        final Random _rng;
        final Map<Long, Strategy> _actions = new TreeMap<Long, Strategy>();
        final List<ServerDispatcher> _servers = new LinkedList<ServerDispatcher>();
        final Map<ServerDispatcher, CheckpointStorage> _checkpoints =
                new HashMap<ServerDispatcher, CheckpointStorage>();
        Transport _currentLeader;
        final OrderedMemoryTransportFactory _factory;

        long _opCount = 0;

        Environment(long aSeed, long aCycles, boolean doCalibrate) throws Exception {
            _calibrate = doCalibrate;
            _maxCycles = aCycles;
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
                _checkpoints.put(myDisp, new MemoryCheckpointStorage());
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

    private LongTerm(long aSeed, long aCycles, boolean doCalibrate) throws Exception {
        _env = new Environment(aSeed, aCycles, doCalibrate);
    }

    public static void main(String[] anArgs) throws Exception {
        Args myArgs = CliFactory.parseArguments(Args.class, anArgs);
        
        long myStart = System.currentTimeMillis();

        new LongTerm(myArgs.getSeed(), myArgs.getCycles(), myArgs.isCalibrate()).run();

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
        Transport myTransport = _env._factory.newTransport();
        myTransport.add(myClient);

        long opsSinceCkpt = 0;

        if (! _env._calibrate) {
            for (long i = 0; i < _env._maxCycles; i++) {
                // Pick a percent chance of a failure
                //
                int myChancePercent = _env._rng.nextInt(101);

                int myResultPercent = _env._rng.nextInt(101);

                if (myResultPercent <= myChancePercent) {
                    if (_env._actions.get(i) == null)
                        _env._actions.put(i, _happenings[_env._rng.nextInt(_happenings.length)]);
                }
            }
        }

        while (_env._opCount < _env._maxCycles) {

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
                    for (ServerDispatcher mySD : _env._servers) {
                        CheckpointHandle myHandle = mySD.getAcceptorLearner().newCheckpoint();
                        CheckpointStorage myStorage = _env._checkpoints.get(mySD);
                        CheckpointStorage.WriteCheckpoint myCkpt = myStorage.newCheckpoint();
                        ObjectOutputStream myStream = new ObjectOutputStream(myCkpt.getStream());
                        myStream.writeObject(myHandle);
                        myStream.close();

                        myCkpt.saved();
                        myHandle.saved();

                        assert(myStorage.numFiles() == 1);
                    }

                    opsSinceCkpt = 0;
                }
            }

            // Round we go again
            //
            _env._opCount++;
        }

        for (ServerDispatcher mySd : _env._servers)
            mySd.getTransport().terminate();

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

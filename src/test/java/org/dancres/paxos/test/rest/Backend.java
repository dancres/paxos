package org.dancres.paxos.test.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.dancres.paxos.*;
import static org.dancres.paxos.CheckpointStorage.*;
import org.dancres.paxos.storage.DirectoryCheckpointStorage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.impl.net.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Route;

import static spark.Spark.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @todo Handle all possible outcomes from a vote
 */
public class Backend {
    private static final long CHECKPOINT_EVERY = 5;
    
    private static final Logger _logger = LoggerFactory.getLogger(Backend.class);

    private Paxos _paxos;
    private CheckpointStorage _storage;

    private final AtomicLong _opCounter = new AtomicLong(0);
    private final AtomicBoolean _checkpointActive = new AtomicBoolean(false);
    private final AtomicBoolean _outOfDate = new AtomicBoolean(false);
    private final InetSocketAddress _serverAddr;
    private final int _clusterSize;
    
    private ConcurrentHashMap<String, String> _keyValues = new ConcurrentHashMap<>();
    
    public static void main(String[] anArgs) throws Exception {
        new Backend(Integer.parseInt(anArgs[0]), Integer.parseInt(anArgs[1])).start(anArgs[2]);
    }

    private Backend(int aPort, int aClusterSize) {
        _serverAddr = new InetSocketAddress(Utils.getWorkableInterfaceAddress(), aPort);
        _clusterSize = aClusterSize;
    }
    
    private void start(String aCheckpointDir) throws Exception {
        _storage = new DirectoryCheckpointStorage(new File(aCheckpointDir));
        HowlLogger myTxnLogger = new HowlLogger(aCheckpointDir);
        
        port(_serverAddr.getPort());

        get("/checkpoint", new Checkpoint());

        get("/members", new Members());
        
        get("/values", new Values());
        
        put("/map/:key", new WriteKey());
        
        CheckpointHandle myHandle = CheckpointHandle.NO_CHECKPOINT;
        ReadCheckpoint myCkpt = _storage.getLastCheckpoint();

        if (myCkpt != null) {
            InputStream myStream = myCkpt.getStream();

            myHandle = installCheckpoint(myStream);
        }
        
        _paxos = Paxos.init(_clusterSize, new ListenerImpl(), myHandle,
                Utils.marshall(_serverAddr), myTxnLogger);
    }

    private CheckpointHandle installCheckpoint(InputStream anInputStream) throws IOException, ClassNotFoundException {
        ObjectInputStream myOIS = new ObjectInputStream(anInputStream);

        CheckpointHandle myHandle = (CheckpointHandle) myOIS.readObject();

        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, String> myTempKeyValues = (ConcurrentHashMap<String, String>) myOIS.readObject();
        _keyValues = myTempKeyValues;

        myOIS.close();

        return myHandle;
    }

    private String toHttp(byte[] aMarshalledAddress) throws Exception {
        return "http:/" + Utils.unmarshallInetSocketAddress(aMarshalledAddress).toString();
    }
    
    private InetSocketAddress toInetAddress(byte[] aMarshalledAddress) throws Exception {
        return Utils.unmarshallInetSocketAddress(aMarshalledAddress);
    }
    
    class ListenerImpl implements Listener {
        public void transition(StateEvent anEvent) {
            Proposal myCommittedProp = anEvent.getValues();

            switch (anEvent.getResult()) {
                case VALUE: {
                    _keyValues.put(new String(myCommittedProp.get("KEY")),
                            new String(myCommittedProp.get("VALUE")));

                    long myCount = _opCounter.incrementAndGet();
                    if ((myCount % CHECKPOINT_EVERY) == 0) {
                        if (_checkpointActive.compareAndSet(false, true)) {
                            new Checkpointer().start();
                        }
                    }

                    break;
                }

                case OUT_OF_DATE : {
                    if (_outOfDate.compareAndSet(false, true)) {
                        try {
                            new Recovery().start();
                        } catch (Exception anE) {
                            _logger.error("Couldn't start recovery thread", anE);
                        }
                    }

                    break;
                }
                
                case UP_TO_DATE : {
                    _outOfDate.compareAndSet(true, false);
                    
                    break;
                }

                default: {
                    _logger.error("Unhandled outcome: " + anEvent.getResult());

                    break;
                }
            }
        }
    }

    class Recovery extends Thread {
        public void run() {
            while (true) {
                try {
                    List<String> myURLs = getURLs();

                    _logger.debug("Got " + myURLs.size() + " checkpoint candidates");

                    for (String u : myURLs) {
                        try {
                            URL myURL = new URL(u + "/checkpoint");

                            _logger.debug("Connecting to: " + myURL + " for checkpoint recovery");

                            HttpURLConnection myConn = (HttpURLConnection) myURL.openConnection();

                            if (myConn.getResponseCode() != 200)
                                continue;

                            ObjectMapper myMapper = new ObjectMapper();
                            ByteArrayInputStream myBAIS =
                                    new ByteArrayInputStream(myMapper.readValue(myConn.getInputStream(), byte[].class));

                            myConn.getInputStream().close();

                            CheckpointHandle myHandle = installCheckpoint(myBAIS);

                            Paxos.Checkpoint myCheckpoint = _paxos.checkpoint().forRecovery();

                            if (myCheckpoint.getConsumer().apply(myHandle)) {
                                Backend.this._outOfDate.compareAndSet(true, false);
                                return;
                            }
                        } catch (Exception anE) {
                            _logger.warn("Exception whilst obtaining checkpoint", anE);
                        }
                    }


                    Thread.sleep(30000);
                } catch (Exception anE) {
                    _logger.warn("Exception whilst attempting recovery", anE);
                }
            }
        }

        List<String> getURLs() throws Exception {
            List<String> myURLs = new LinkedList<>();

            for (Membership.MetaData m : _paxos.getMembership().getMembers().values()) {
                if (! toInetAddress(m.getData()).equals(_serverAddr)) {
                    myURLs.add(toHttp(m.getData()));
                }
            }

            return myURLs;
        }
    }
    
    private class Checkpointer extends Thread {
        public void run() {
            try {
                Paxos.Checkpoint myCheckpoint = _paxos.checkpoint().forSaving();

                writeCheckpoint(myCheckpoint.getHandle());
                myCheckpoint.getConsumer().apply(myCheckpoint.getHandle());

            } catch (Exception anE) {
                _logger.error("Serious checkpointing problem", anE);
            } finally {
                // Once checkpointed, signal we're done
                //
                Backend.this._checkpointActive.compareAndSet(true, false);                
            }
        }
    }
    
    private void writeCheckpoint(CheckpointHandle aHandle) throws Exception {
        WriteCheckpoint myCkpt = _storage.newCheckpoint();

        OutputStream myOutput = myCkpt.getStream();
        ObjectOutputStream myOOS = new ObjectOutputStream(myOutput);

        myOOS.writeObject(aHandle);
        myOOS.writeObject(_keyValues);

        myOOS.flush();
        myOOS.close();
        myCkpt.saved();
    }

    class Checkpoint implements Route {
        public Object handle(Request request, Response response) {
            if (_outOfDate.get()) {
                response.status(503);
                return "";
            }

            ReadCheckpoint myCkpt = _storage.getLastCheckpoint();

            if (myCkpt == null) {
                response.status(404);
                return "";
            }

            try {
                InputStream myStream = myCkpt.getStream();
                ObjectInputStream myOIS = new ObjectInputStream(myStream);

                CheckpointHandle myHandle = (CheckpointHandle) myOIS.readObject();

                @SuppressWarnings("unchecked")
                ConcurrentHashMap<String, String> myState = (ConcurrentHashMap<String, String>) myOIS.readObject();

                ByteArrayOutputStream myBAOS = new ByteArrayOutputStream();
                ObjectOutputStream myOOS = new ObjectOutputStream(myBAOS);

                myOOS.writeObject(myHandle);
                myOOS.writeObject(myState);
                myOOS.flush();
                myOOS.close();

                ObjectMapper myMapper = new ObjectMapper();

                response.status(200);
                return myMapper.writeValueAsString(myBAOS.toByteArray());

            } catch (Exception anE) {
                _logger.error("Couldn't recover checkpoint", anE);
            }

            response.status(500);
            return "";
        }
    }

    class Values implements Route {
        public Object handle(Request request, Response response) {
            if (_outOfDate.get()) {
                response.status(503);
                return "";
            }

            ObjectMapper myMapper = new ObjectMapper();

            try {
                return myMapper.writeValueAsString(_keyValues);
            } catch (Exception anE) {
                _logger.error("Couldn't recover values", anE);
                response.status(500);

                return "";
            }
        }
    }

    class WriteKey implements Route {
        public Object handle(Request request, Response response) {
            if (_outOfDate.get()) {
                response.status(503);

                return "";
            }

            final CompletionImpl<VoteOutcome> myResult = new CompletionImpl<>();

            Proposal myProp = new Proposal();
            myProp.put("KEY", request.params(":key").getBytes());
            myProp.put("VALUE", request.body().getBytes());

            try {
                _paxos.submit(myProp, myResult);

                VoteOutcome myOutcome = myResult.await();

                switch (myOutcome.getResult()) {
                    case VoteOutcome.Reason.VALUE: {
                        response.status(200);

                        return request.body();
                    }

                    case VoteOutcome.Reason.OTHER_LEADER : {
                        response.status(301);

                        response.header("Location",
                                toHttp(_paxos.getMembership().dataForNode(myOutcome.getLeader())));

                        return "";
                    }

                    default: {
                        _logger.error("Unhandled outcome: " + myOutcome.getResult());
                        response.status(500);

                        return "";
                    }
                }
            } catch (Exception anE) {
                _logger.error("Couldn't run Paxos", anE);
                response.status(500);

                return "";
            }
        }
    }

    class Members implements Route {
        public Object handle(Request request, Response response) {
            ObjectMapper myMapper = new ObjectMapper();

            Map<InetSocketAddress, Membership.MetaData> myMembers = _paxos.getMembership().getMembers();

            Map<String, String> myMemberData = new HashMap<>();

            try {
                for (Map.Entry<InetSocketAddress, Membership.MetaData> myDetails : myMembers.entrySet()) {
                    myMemberData.put(myDetails.getKey().toString(), toHttp(myDetails.getValue().getData()));
                }

                return myMapper.writeValueAsString(myMemberData);
            } catch (Exception anE) {
                _logger.error("Couldn't recover metadata", anE);
            }

            response.status(500);

            return "";
        }
    }
}

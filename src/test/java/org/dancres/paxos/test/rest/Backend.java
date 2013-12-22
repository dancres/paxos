package org.dancres.paxos.test.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.dancres.paxos.*;
import static org.dancres.paxos.CheckpointStorage.*;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.storage.DirectoryCheckpointStorage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.impl.net.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static spark.Spark.*;
import spark.*;

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
    private HowlLogger _txnLogger;
    private CheckpointStorage _storage;

    private final AtomicLong _opCounter = new AtomicLong(0);
    private final AtomicBoolean _checkpointActive = new AtomicBoolean(false);
    private final AtomicBoolean _outOfDate = new AtomicBoolean(false);
    private final InetSocketAddress _serverAddr;
    private final int _clusterSize;

    private ConcurrentHashMap<String, String> _keyValues = new ConcurrentHashMap<>();


    public static void main(String[] anArgs) throws Exception {
        new Backend(Integer.valueOf(anArgs[0]), Integer.valueOf(anArgs[1])).start(anArgs[2]);
    }

    private Backend(int aPort, int aClusterSize) {
        _serverAddr = new InetSocketAddress(Utils.getWorkableInterfaceAddress(), aPort);
        _clusterSize = aClusterSize;
    }
    
    private void start(String aCheckpointDir) throws Exception {
        _storage = new DirectoryCheckpointStorage(new File(aCheckpointDir));
        _txnLogger = new HowlLogger(aCheckpointDir);
        
        setPort(_serverAddr.getPort());

        get(new Route("/checkpoint") {
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
        });

        get(new Route("/members") {
            public Object handle(Request request, Response response) {
                ObjectMapper myMapper = new ObjectMapper();
                
                Map<InetSocketAddress, FailureDetector.MetaData> myMembers = _paxos.getDetector().getMemberMap();
                
                Map<String, String> myMemberData = new HashMap<>();
                
                try {
                    for (Map.Entry<InetSocketAddress, FailureDetector.MetaData> myDetails : myMembers.entrySet()) {
                        myMemberData.put(myDetails.getKey().toString(), toHttp(myDetails.getValue().getData()));
                    }

                    return myMapper.writeValueAsString(myMemberData);
                } catch (Exception anE) {
                    _logger.error("Couldn't recover metadata", anE);
                }
                
                response.status(500);

                return "";
            }
        });

        get(new Route("/values") {
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
        });
        
        put(new Route("/map/:key") {
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
                                    toHttp(_paxos.getDetector().getMemberMap().get(myOutcome.getLeader()).getData()));

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
        });
        
        CheckpointHandle myHandle = CheckpointHandle.NO_CHECKPOINT;
        ReadCheckpoint myCkpt = _storage.getLastCheckpoint();
        if (myCkpt != null) {
            InputStream myStream = myCkpt.getStream();

            ObjectInputStream myOIS = new ObjectInputStream(myStream);

            myHandle = (CheckpointHandle) myOIS.readObject();
            
            _keyValues = (ConcurrentHashMap<String, String>) myOIS.readObject();
            
            myOIS.close();
        }
        
        _paxos = PaxosFactory.init(_clusterSize, new ListenerImpl(), myHandle,
                Utils.marshall(_serverAddr), _txnLogger);
    }

    String toHttp(byte[] aMarshalledAddress) throws Exception {
        return "http:/" + Utils.unmarshallInetSocketAddress(aMarshalledAddress).toString();
    }
    
    InetSocketAddress toInetAddress(byte[] aMarshalledAddress) throws Exception {
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
                            new Recovery(
                                    toHttp(_paxos.getDetector().getMemberMap().get(anEvent.getLeader()).getData())
                            ).start();
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
        private List<String> _targetURLs = new LinkedList<>();
        
        Recovery(String anURL) throws Exception {
            _targetURLs.add(anURL);
            
            // Stock the list with other members but not ourselves and not the first target
            //
            for (FailureDetector.MetaData m : _paxos.getDetector().getMemberMap().values()) {
                if (! toInetAddress(m.getData()).equals(_serverAddr)) {
                    String myURL = toHttp(m.getData());
                    
                    if (! _targetURLs.contains(myURL))
                        _targetURLs.add(myURL);
                }
            }
        }

        public void run() {
            while (true) {
                _logger.debug("Got " + _targetURLs.size() + " checkpoint candidates");

                for (String u : _targetURLs) {
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

                        ObjectInputStream myOIS = new ObjectInputStream(myBAIS);

                        CheckpointHandle myHandle = (CheckpointHandle) myOIS.readObject();
                        _keyValues = (ConcurrentHashMap<String, String>) myOIS.readObject();

                        writeCheckpoint(myHandle);

                        if (_paxos.bringUpToDate(myHandle)) {
                            Backend.this._outOfDate.compareAndSet(true, false);
                            return;
                        }
                    } catch (Exception anE) {
                        _logger.warn("Exception whilst obtaining checkpoint", anE);
                    }
                }

                try {
                    Thread.sleep(30000);

                    _targetURLs.clear();

                    for (FailureDetector.MetaData m : _paxos.getDetector().getMemberMap().values()) {
                        if (! toInetAddress(m.getData()).equals(_serverAddr)) {
                            _targetURLs.add(toHttp(m.getData()));
                        }
                    }
                } catch (Exception anE) {
                    _logger.warn("Exception whilst recycling", anE);
                }
            }
        }
    }
    
    class Checkpointer extends Thread {
        public void run() {
            try {
                CheckpointHandle myHandle = _paxos.newCheckpoint();

                writeCheckpoint(myHandle);
                myHandle.saved();

            } catch (Exception anE) {
                _logger.error("Serious checkpointing problem", anE);
            } finally {
                // Once checkpointed, signal we're done
                //
                Backend.this._checkpointActive.compareAndSet(true, false);                
            }
        }
    }
    
    void writeCheckpoint(CheckpointHandle aHandle) throws Exception {
        WriteCheckpoint myCkpt = _storage.newCheckpoint();

        OutputStream myOutput = myCkpt.getStream();
        ObjectOutputStream myOOS = new ObjectOutputStream(myOutput);

        myOOS.writeObject(aHandle);
        myOOS.writeObject(_keyValues);

        myOOS.flush();
        myOOS.close();
        myCkpt.saved();
    }
}

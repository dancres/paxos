package org.dancres.paxos.test.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.dancres.paxos.*;
import static org.dancres.paxos.CheckpointStorage.*;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.impl.net.Utils;
import org.dancres.paxos.impl.util.DirectoryCheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static spark.Spark.*;
import spark.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @todo Checkpoint installation after out of date.
 */
public class Backend {
    private static final String HANDBACK_KEY = "org.dancres.paxos.test.backend.handback";
    private static final long CHECKPOINT_EVERY = 5;
    
    private static Logger _logger = LoggerFactory
            .getLogger(Backend.class);

    class Result {
        private VoteOutcome _outcome;
    
        void deliver(VoteOutcome anOutcome) {
            synchronized (this) {
                _outcome = anOutcome;
                notify();
            }
        }
        
        VoteOutcome await() {
            synchronized (this) {
                while (_outcome == null) {
                    try {
                        wait();
                    } catch (InterruptedException anIE) {}
                }

                return _outcome;
            }
        }
    }
    
    private Paxos _paxos;
    private HowlLogger _txnLogger;
    private AtomicLong _handbackSequence = new AtomicLong(0);
    private AtomicLong _opCounter = new AtomicLong(0);
    private AtomicBoolean _checkpointActive = new AtomicBoolean(false);
    private AtomicBoolean _outOfDate = new AtomicBoolean(false);
    private InetSocketAddress _serverAddr;
    private ConcurrentHashMap<String, Result> _requestMap = new ConcurrentHashMap<String, Result>();
    private ConcurrentHashMap<String, String> _keyValues = new ConcurrentHashMap<String, String>();
    private CheckpointStorage _storage;


    public static void main(String[] anArgs) throws Exception {
        new Backend().start(Integer.valueOf(anArgs[0]), anArgs[1]);
    }

    private void start(Integer aPort, String aCheckpointDir) throws Exception {
        _storage = new DirectoryCheckpointStorage(new File(aCheckpointDir));
        _txnLogger = new HowlLogger(aCheckpointDir);
        
        setPort(aPort.intValue());

        get(new Route("/members") {
            public Object handle(Request request, Response response) {
                ObjectMapper myMapper = new ObjectMapper();
                
                Map<InetSocketAddress, FailureDetector.MetaData> myMembers = _paxos.getDetector().getMemberMap();
                Map<String, String> myMemberData = new HashMap<String, String>();
                
                try {
                    for (Map.Entry<InetSocketAddress, FailureDetector.MetaData> myDetails : myMembers.entrySet()) {
                        myMemberData.put(myDetails.getKey().toString(), toHttp(myDetails.getValue().getData()));
                    }

                    return myMapper.writeValueAsString(myMemberData);
                } catch (Exception anE) {
                    _logger.error("Couldn't recover metadata", anE);
                }
                
                response.status(500);

                return null;
            }
        });

        get(new Route("/values") {
            public Object handle(Request request, Response response) {
                ObjectMapper myMapper = new ObjectMapper();

                try {
                    return myMapper.writeValueAsString(_keyValues);
                } catch (Exception anE) {
                    _logger.error("Couldn't recover values", anE);
                    response.status(500);

                    return null;
                }
            }
        });
        
        put(new Route("/map/:key") {
            public Object handle(Request request, Response response) {
                if (_outOfDate.get()) {
                    response.status(503);

                    return null;
                }
                    
                String myHandback = _serverAddr.toString() + ";" + Long.toString(_handbackSequence.getAndIncrement());
                Result myResult = new Result();

                _requestMap.put(myHandback, myResult);

                Proposal myProp = new Proposal();
                myProp.put(HANDBACK_KEY, myHandback.getBytes());
                myProp.put("KEY", request.params(":key").getBytes());
                myProp.put("VALUE", request.body().getBytes());

                try {
                    _paxos.submit(myProp);

                    VoteOutcome myOutcome = myResult.await();

                    switch (myOutcome.getResult()) {
                        case VoteOutcome.Reason.DECISION : {
                            response.status(200);
                            
                            return request.body();
                        }

                        case VoteOutcome.Reason.OTHER_LEADER : {
                            response.status(301);
                            
                            response.header("Location",
                                    toHttp(_paxos.getDetector().getMemberMap().get(myOutcome.getLeader()).getData()));

                            return "";
                        }

                        case VoteOutcome.Reason.OUT_OF_DATE : {
                            response.status(503);

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
        
        _serverAddr = new InetSocketAddress(Utils.getWorkableInterface(), aPort.intValue());

        CheckpointHandle myHandle = CheckpointHandle.NO_CHECKPOINT;
        ReadCheckpoint myCkpt = _storage.getLastCheckpoint();
        if (myCkpt != null) {
            InputStream myStream = myCkpt.getStream();

            ObjectInputStream myOIS = new ObjectInputStream(myStream);

            myHandle = (CheckpointHandle) myOIS.readObject();
            ConcurrentHashMap<String, String> myState = (ConcurrentHashMap<String, String>) myOIS.readObject();
            
            _keyValues = myState;
            
            myOIS.close();
        }
        
        _paxos = PaxosFactory.init(new ListenerImpl(), myHandle, Utils.marshall(_serverAddr), _txnLogger);
    }

    String toHttp(byte[] aMarshalledAddress) throws Exception {
        return "http:/" + Utils.unmarshallInetSocketAddress(aMarshalledAddress).toString();
    }
    
    class ListenerImpl implements Paxos.Listener {
        public void done(VoteOutcome anEvent) {
            // If we're not the originating node for the post, because we're not leader,
            // we won't have an address stored up
            //
            Result myResult =
                    (anEvent.getValues().get(HANDBACK_KEY) != null) ?
                            _requestMap.remove(new String(anEvent.getValues().get(HANDBACK_KEY))) :
                            null;

            Proposal myCommittedProp = anEvent.getValues();

            switch (anEvent.getResult()) {
                case VoteOutcome.Reason.DECISION: {
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

                case VoteOutcome.Reason.OUT_OF_DATE : {
                    _outOfDate.compareAndSet(false, true);
                    
                    break;
                }
                
                case VoteOutcome.Reason.UP_TO_DATE : {
                    _outOfDate.compareAndSet(true, false);
                    
                    break;
                }

                default: {
                    _logger.error("Unhandled outcome: " + anEvent.getResult());

                    break;
                }
            }

            if (myResult != null)
                myResult.deliver(anEvent);
        }
    }
    
    class Checkpointer extends Thread {
        public void run() {
            try {
                WriteCheckpoint myCkpt = _storage.newCheckpoint();

                OutputStream myOutput = myCkpt.getStream();
                ObjectOutputStream myOOS = new ObjectOutputStream(myOutput);
                
                CheckpointHandle myHandle = _paxos.newCheckpoint();
                
                myOOS.writeObject(myHandle);
                myOOS.writeObject(_keyValues);

                myOOS.flush();
                myOOS.close();
                myCkpt.saved();
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
}

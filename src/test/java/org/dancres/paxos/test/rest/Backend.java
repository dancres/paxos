package org.dancres.paxos.test.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.dancres.paxos.Paxos;
import org.dancres.paxos.PaxosFactory;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.CheckpointHandle;
import org.dancres.paxos.impl.HowlLogger;
import org.dancres.paxos.impl.net.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static spark.Spark.*;
import spark.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
    private AtomicLong _handbackGenerator = new AtomicLong(0);
    private AtomicLong _opCounter = new AtomicLong(0);
    private AtomicBoolean _checkpointActive = new AtomicBoolean(false);
    
    private ConcurrentHashMap<String, Result> _requestMap = new ConcurrentHashMap<String, Result>();
    private ConcurrentHashMap<String, String> _keyValues = new ConcurrentHashMap<String, String>();
    private CheckpointStorage _storage;


    public static void main(String[] anArgs) throws Exception {
        new Backend().start(Integer.valueOf(anArgs[0]), anArgs[1]);
    }

    private void start(Integer aPort, String aCheckpointDir) throws Exception {
        _storage = new CheckpointStorage(new File(aCheckpointDir));
        _txnLogger = new HowlLogger(aCheckpointDir);
        
        setPort(aPort.intValue());

        get(new Route("/members") {
            public Object handle(Request request, Response response) {
                ObjectMapper myMapper = new ObjectMapper();
                
                Set<InetSocketAddress> myMembers = _paxos.getMembers();
                Map<String, String> myMemberData = new HashMap<String, String>();
                
                try {
                    for (InetSocketAddress myAddr : myMembers) {
                        myMemberData.put(myAddr.toString(),
                                Utils.unmarshallInetSocketAddress(_paxos.getMetaData(myAddr)).toString());
                    }

                    return myMapper.writeValueAsString(myMemberData);
                } catch (Exception anE) {
                    _logger.error("Couldn't recover metadata", anE);
                }
                
                halt(500);

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
                    halt(500);

                    return null;
                }
            }
        });
        
        put(new Route("/map/:key") {
            public Object handle(Request request, Response response) {
                String myHandback = Long.toString(_handbackGenerator.getAndIncrement());
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
                        case VoteOutcome.Reason.DECISION: {
                            response.status(200);
                            
                            return request.body();
                        }

                        default: {
                            _logger.error("Unhandled outcome: " + myOutcome.getResult());
                            halt(500);

                            return null;
                        }
                    }
                } catch (Exception anE) {
                    _logger.error("Couldn't run Paxos", anE);
                    halt(500);

                    return null;
                }
            }
        });
        
        InetSocketAddress myAddr = new InetSocketAddress(Utils.getWorkableInterface(), aPort.intValue());

        CheckpointHandle myHandle = CheckpointHandle.NO_CHECKPOINT;
        CheckpointStorage.ReadCheckpoint myCkpt = _storage.getLastCheckpoint();
        if (myCkpt != null) {
            InputStream myStream = myCkpt.getStream();

            ObjectInputStream myOIS = new ObjectInputStream(myStream);

            myHandle = (CheckpointHandle) myOIS.readObject();
            ConcurrentHashMap<String, String> myState = (ConcurrentHashMap<String, String>) myOIS.readObject();
            
            _keyValues = myState;
            
            myOIS.close();
        }
        
        _paxos = PaxosFactory.init(new ListenerImpl(), myHandle, Utils.marshall(myAddr), _txnLogger);
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

        /**
         * @todo Strictly speaking we should sync that checkpoint to disk
         */
        public void run() {
            try {
                CheckpointStorage.WriteCheckpoint myCkpt = _storage.newCheckpoint();

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
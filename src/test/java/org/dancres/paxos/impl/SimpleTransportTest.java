package org.dancres.paxos.impl;

import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.utils.Builder;
import org.junit.*;

public class SimpleTransportTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        Builder myBuilder = new Builder();

        _tport1 = myBuilder.newDefaultStack();
        _tport2 = myBuilder.newDefaultStack();
    }

    @After public void stop() throws Exception {
        if (_tport1 != null)
            _tport1.terminate();
        else
            System.err.println("No tport1");

        if (_tport2 != null)
            _tport2.terminate();
        else
            System.err.println("No tport2");
    }

    @Test public void post() throws Exception {
        ClientDispatcher myClient = new ClientDispatcher();
        TransportImpl myTransport = new TransportImpl(null);
        myClient.init(myTransport);

        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);
        
        myTransport.terminate();
    }

    public static void main(String[] anArgs) throws Exception {
        SimpleSuccessTest myTest = new SimpleSuccessTest();
        myTest.init();
        myTest.post();
        myTest.stop();
    }
}

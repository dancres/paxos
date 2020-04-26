package org.dancres.paxos.bus;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class MessagesTest {
    private enum Dictionary {
        MESSAGE_TYPE
    }

    private class Listener implements Messages.Subscriber<Dictionary> {
        private final List<String> _messages = new LinkedList<>();

        @Override
        public void msg(Messages.Message<Dictionary> aMessage) {
            _messages.add((String) aMessage.getMessage());
        }

        List<String> getMessages() {
            return Collections.unmodifiableList(_messages);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void duplicateSubscriber() {
        Messages<Dictionary> myMessages = new MessagesImpl<>();
        Listener mySink = new Listener();
        Listener mySource = new Listener();

        myMessages.subscribe("Same", mySource);
        myMessages.subscribe("Same", mySink);
    }

    @Test
    public void unsubscribedDoesNotReceive() {
        Messages<Dictionary> myMessages = new MessagesImpl<>();
        Listener mySink = new Listener();
        Listener mySource = new Listener();

        Messages.Subscription<Dictionary> mySourceSubscription = myMessages.subscribe("Src", mySource);
        Messages.Subscription<Dictionary> mySinkSubscription = myMessages.subscribe("Sink", mySink);

        mySinkSubscription.unsubscribe();

        mySourceSubscription.send(Dictionary.MESSAGE_TYPE, "Payload");

        List<String> myReceived = mySink.getMessages();
        Assert.assertEquals(0, myReceived.size());
    }

    @Test
    public void senderDoesntReceive() {
        Messages<Dictionary> myMessages = new MessagesImpl<>();
        Listener mySink = new Listener();
        Listener mySource = new Listener();

        Messages.Subscription<Dictionary> mySourceSubscription = myMessages.subscribe("Src", mySource);
        myMessages.subscribe("Sink", mySink);

        mySourceSubscription.send(Dictionary.MESSAGE_TYPE, "Payload");

        List<String> myReceived = mySink.getMessages();

        Assert.assertEquals(1, myReceived.size());
        Assert.assertEquals(myReceived.get(0), "Payload");

        myReceived = mySource.getMessages();

        Assert.assertEquals(0, myReceived.size());
    }

    @Test
    public void testReceive() {
        Messages<Dictionary> myMessages = new MessagesImpl<>();
        Listener mySink = new Listener();
        Listener mySource = new Listener();

        Messages.Subscription<Dictionary> mySourceSubscription = myMessages.subscribe("Src", mySource);
        myMessages.subscribe("Sink", mySink);

        mySourceSubscription.send(Dictionary.MESSAGE_TYPE, "Payload");

        List<String> myReceived = mySink.getMessages();

        Assert.assertEquals(1, myReceived.size());
        Assert.assertEquals(myReceived.get(0), "Payload");
    }

    @Test
    public void testAnonReceive() {
        Messages<Dictionary> myMessages = new MessagesImpl<>();
        Listener mySink = new Listener();
        Listener mySource = new Listener();

        Messages.Subscription<Dictionary> mySourceSubscription = myMessages.anonSubscrbe(mySource);
        myMessages.anonSubscrbe(mySink);

        mySourceSubscription.send(Dictionary.MESSAGE_TYPE, "Payload");

        List<String> myReceived = mySink.getMessages();

        Assert.assertEquals(1, myReceived.size());
        Assert.assertEquals(myReceived.get(0), "Payload");
    }
}

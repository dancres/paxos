package org.dancres.paxos.bus;

public interface Messages<T extends Enum> {
    interface Subscriber<T> {
        void msg(Message<T> aMessage);
    }

    interface Message<T> {
        T getType();
        Object getMessage();
        String getSource();
    }

    interface Subscription<T> {
        void send(T aType, Object aBody);
        void send(T aType);
        void unsubscribe();
    }

    Subscription<T> subscribe(String aName, Subscriber<T> aSubs);
    Subscription<T> anonSubscrbe(Subscriber<T> aSubs);
}

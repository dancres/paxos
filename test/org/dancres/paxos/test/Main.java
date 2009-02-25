package org.dancres.paxos.test;

public class Main {
    public static void main(String anArgs[]) throws Exception {
        if (anArgs.length == 0)
            new Client().main(anArgs);
        else
            new Server().main(anArgs);
    }
}

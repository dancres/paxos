package org.dancres.paxos.test;

import static spark.Spark.*;
import spark.*;

class Sparkup {

    public static void main(String[] args) {

        get(new Route("/hello") {
            @Override
            public Object handle(Request request, Response response) {
                return "Hello World!";
            }
        });

    }
}

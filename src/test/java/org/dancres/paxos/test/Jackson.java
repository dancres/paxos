package org.dancres.paxos.test;


import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public class Jackson {
    public static void main(String[] anArgs) throws Exception {
        ObjectMapper myMapper = new ObjectMapper();

        Map<String, String> myTable = new HashMap<>();
        myTable.put("node1", "aURL");
        myTable.put("node2", "anotherURL");

        ByteArrayOutputStream myBAOS = new ByteArrayOutputStream();

        myMapper.writeValue(myBAOS, myTable);

        ByteArrayInputStream myBAIS = new ByteArrayInputStream(myBAOS.toByteArray());
        
        Map<String, Object> myRead = myMapper.readValue(myBAIS, Map.class);
        
        for (Map.Entry e: myRead.entrySet())
            System.out.println(e.getKey() + " -> " + e.getValue());
    }
}

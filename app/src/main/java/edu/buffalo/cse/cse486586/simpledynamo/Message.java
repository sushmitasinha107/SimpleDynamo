package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sushmitasinha on 5/9/17.
 */

public class Message implements Serializable{

    public String type;
    public int index;
    public boolean sendAhead;
    public boolean skipped = false;
    public ConcurrentHashMap<String, String> map;


    public Message(String t, int i, boolean p, ConcurrentHashMap<String, String> m) {
        type = t;
        index = i;
        sendAhead = p;
        map = m;
    }

}



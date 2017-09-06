package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by sushmitasinha on 5/5/17.
 */

public class ClientTask extends Thread {

    public static class ClientMessage{
         int port;
         String type;
         int index;
         ConcurrentHashMap<String, String> map;


        public ClientMessage( int p,  String t,  int i,
                              ConcurrentHashMap<String, String> m) {
            port = p;
            type = t;
            index = i;
            map = m;
        }

    }
    public static final String TAG = ClientTask.class.getSimpleName();


    private static boolean checkClient(ClientMessage clientMessage) {

         int port = clientMessage.port;  String type = clientMessage.type;  int index= clientMessage.index;
         ConcurrentHashMap<String, String> map = clientMessage.map;

        try {
            boolean skipped = false;
            Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
            s.setSoTimeout(10000);
            ObjectOutputStream o = null;
            ObjectInputStream ois = null;
            boolean propagate = false;

            if ((type.equals("insert")|| type.equals("delete")) && index < 2)
                propagate = true;

            Message m = new Message(type, index, propagate, map);
            m.skipped = skipped;

            o = new ObjectOutputStream(s.getOutputStream());
            o.writeObject(m);

            ois = new ObjectInputStream(s.getInputStream());
            m = (Message) ois.readObject();

            o.close();
            s.close();

            if (type.equals("query")) {
                SimpleDynamoProvider.sMap.putAll(m.map);
                synchronized (SimpleDynamoProvider.sMap) {
                    SimpleDynamoProvider.sMap.notify();
                }
            } else if (type.equals("queryAll")) {
                SimpleDynamoProvider.sMap.putAll(m.map);
                if (SimpleDynamoProvider.sReceived.incrementAndGet() == 5) {
                    SimpleDynamoProvider.sReceived.set(0);
                    synchronized (SimpleDynamoProvider.sReceived) {
                        SimpleDynamoProvider.sReceived.notify();
                    }
                }
            } else if (type.equals("recover")) {

                if (port == SimpleDynamoProvider.sPartitionManager.nextPort(SimpleDynamoProvider.mPort)) {
                    SimpleDynamoProvider.keyStore.get(1).putAll(m.map);
                } else if (port == SimpleDynamoProvider.sPartitionManager.prevPort(SimpleDynamoProvider.mPort)) {
                    SimpleDynamoProvider.keyStore.get(2).putAll(m.map);
                } else {
                    SimpleDynamoProvider.keyStore.get(0).putAll(m.map);
                }

                if (SimpleDynamoProvider.sReceived.incrementAndGet() == 3) {
                    SimpleDynamoProvider.sRecover = false;
                    SimpleDynamoProvider.sReceived.set(0);
                    synchronized (SimpleDynamoProvider.sReceived) {
                        SimpleDynamoProvider.sReceived.notifyAll();
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            Log.e(TAG, "ClassNotFoundException"+TAG);
            return false;
        } catch (IOException e) {
            Log.e(TAG, "IOException"+TAG);
            return false;
        }
        return true;
    }

    public static void client(final ClientMessage clientMessage) {
        int port = clientMessage.port;  String type = clientMessage.type;  int index= clientMessage.index;
        ConcurrentHashMap<String, String> map = clientMessage.map;

        Thread t = new Thread() {
            public void run() {

                int port = clientMessage.port;  String type = clientMessage.type;  int index= clientMessage.index;
                ConcurrentHashMap<String, String> map = clientMessage.map;

                if (type.equals("send")) {

                    ClientMessage cMessage = new ClientMessage(port, "insert", index, map);
                    checkClient(cMessage);

                } else if (!checkClient(new ClientMessage(port, type, index, map))) {

                    if (type.equals("query")|| type.equals("queryAll")) {

                        int newPort = SimpleDynamoProvider.sPartitionManager.prevPort(port);
                        int newIndex = index - 1;

                        ClientMessage cMessage = new ClientMessage(newPort, type, newIndex, map);
                        checkClient(cMessage);

                        try {
                            boolean skipped = false;
                            Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), newPort);
                            s.setSoTimeout(10000);
                            ObjectOutputStream o = null;
                            ObjectInputStream ois = null;
                            boolean propagate = false;

                            if ((type.equals("insert")|| type.equals("delete")) && newIndex < 2)
                                propagate = true;

                            Message m = new Message(type, newIndex, propagate, map);
                            m.skipped = skipped;

                            o = new ObjectOutputStream(s.getOutputStream());
                            o.writeObject(m);

                            ois = new ObjectInputStream(s.getInputStream());
                            m = (Message) ois.readObject();

                            o.close();
                            s.close();

                            if (type.equals("query")) {
                                SimpleDynamoProvider.sMap.putAll(m.map);
                                synchronized (SimpleDynamoProvider.sMap) {
                                    SimpleDynamoProvider.sMap.notify();
                                }
                            } else if (type.equals("queryAll")) {

                                SimpleDynamoProvider.sMap.putAll(m.map);
                                if (SimpleDynamoProvider.sReceived.incrementAndGet() == 5) {
                                    SimpleDynamoProvider.sReceived.set(0);
                                    synchronized (SimpleDynamoProvider.sReceived) {
                                        SimpleDynamoProvider.sReceived.notify();
                                    }
                                }
                            }
                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "ClassNotFoundException"+TAG);

                        } catch (IOException e) {
                            Log.e(TAG, "IOException"+TAG);

                        }

                    }
                    else if (type.equals("insert") || type.equals("delete")) {

                        if (index != 2) {
                            int newPort = SimpleDynamoProvider.sPartitionManager.nextPort(port);
                            int newIndex = index + 1;
                            try {
                                Socket s = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), newPort);
                                s.setSoTimeout(10000);
                                ObjectOutputStream o = null;
                                ObjectInputStream ois = null;
                                boolean propagate = false;

                                if ((type.equals("insert") || type.equals("delete")) && newIndex < 2)
                                    propagate = true;

                                Message m = new Message(type, newIndex, propagate, map);
                                m.skipped = true;

                                o = new ObjectOutputStream(s.getOutputStream());
                                o.writeObject(m);

                                ois = new ObjectInputStream(s.getInputStream());
                                m = (Message) ois.readObject();

                                o.close();
                                s.close();

                            } catch (ClassNotFoundException e) {
                                Log.e(TAG, "ClassNotFoundException"+TAG);
                            } catch (IOException e) {
                                Log.e(TAG, "IOException"+TAG);
                            }
                        }
                    }
                }
            }
        };
        t.start();


        if (type.equals("insert") || type.equals("delete")) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Log.e(TAG, "Failed to join"+TAG);
            }
        }
    }
}

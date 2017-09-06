package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by sushmitasinha on 5/5/17.
 */

public class ServerTask {

    public static final String TAG = ServerTask.class.getSimpleName();

    public static void server(final ServerSocket sSocket) {
        new Thread(new Runnable() {
            public void run() {
                ServerSocket serverSocket = sSocket;

                while(true) {
                    try {
                        final Socket s = serverSocket.accept();
                         new Thread(new Runnable() {
                            public void run() {
                                try {
                                    ObjectOutputStream o = new ObjectOutputStream(s.getOutputStream());
                                    o.flush();

                                    ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
                                    Message m = (Message) ois.readObject();

                                    if (SimpleDynamoProvider.sRecover) {

                                        SimpleDynamoProvider.recoverMeNow();
                                    }

                                    if (m.type.equals("insert")) {

                                        SimpleDynamoProvider.keyStore.get(m.index).putAll(m.map);
                                        if (SimpleDynamoProvider.sWaitSet.size() > 0) {
                                            String key = m.map.keySet().iterator().next();
                                            Object object = SimpleDynamoProvider.sWaitSet.get(key);
                                            if (object != null) {
                                                SimpleDynamoProvider.sWaitSet.remove(key);
                                                synchronized (object) {
                                                    object.notifyAll();
                                                }
                                            }
                                        }

                                        if (m.skipped == true) {
                                            int port = SimpleDynamoProvider.sPartitionManager.prevPort(SimpleDynamoProvider.mPort);
                                            int index = m.index - 1;


                                            ClientTask.ClientMessage clientMessage = new ClientTask.ClientMessage(port, "send", index, m.map);
                                            ClientTask.client(clientMessage);
                                        }

                                        if (m.sendAhead == true) {
                                            int port = SimpleDynamoProvider.sPartitionManager.nextPort(SimpleDynamoProvider.mPort);
                                            int index = m.index + 1;

                                            ClientTask.ClientMessage clientMessage = new ClientTask.ClientMessage(port, m.type, index, m.map);
                                            ClientTask.client(clientMessage);

                                        }

                                        o.writeObject(new Message("success", 0, false, null));
                                    }

                                    else if (m.type.equals("query")) {
                                        String key = m.map.keySet().iterator().next();
                                        String value = SimpleDynamoProvider.keyStore.get(m.index).get(key);
                                        if (value == null) {
                                            Object object = SimpleDynamoProvider.sWaitSet.get(key);
                                            if (object == null) {
                                                object = new Object();
                                                SimpleDynamoProvider.sWaitSet.put(key, object);
                                            }
                                            synchronized (object) {
                                                try {

                                                    object.wait();

                                                } catch (InterruptedException e) {
                                                    Log.e(TAG, Log.getStackTraceString(e));
                                                }
                                            }
                                            value = SimpleDynamoProvider.keyStore.get(m.index).get(key);
                                        }
                                        m.map.put(key, value);
                                        o.writeObject(m);
                                    }
                                    else if (m.type.equals("queryAll")) {
                                        m.map = SimpleDynamoProvider.keyStore.get(m.index);
                                        o.writeObject(m);
                                    }
                                    else if (m.type.equals("delete")) {
                                        String key = m.map.keySet().iterator().next();
                                        SimpleDynamoProvider.keyStore.get(m.index).remove(key);
                                        if (m.sendAhead) {
                                            int port = SimpleDynamoProvider.sPartitionManager.nextPort(SimpleDynamoProvider.mPort);
                                            int index = m.index + 1;
                                            ClientTask.ClientMessage clientMessage = new ClientTask.ClientMessage(port, m.type, index, m.map);
                                            ClientTask.client(clientMessage);
                                        }
                                        Message message = new Message("success", 0, false, null);
                                        o.writeObject(message);
                                    }
                                    else {
                                        // RECOVER
                                        m.map = SimpleDynamoProvider.keyStore.get(m.index);
                                        o.writeObject(m);
                                    }
                                    o.close();
                                } catch (ClassNotFoundException e) {
                                    Log.e(TAG, "ClassNotFoundException"+TAG);
                                } catch (IOException e) {
                                    Log.e(TAG, "IOException"+TAG);
                                    Log.e(TAG, Log.getStackTraceString(e));
                                }
                            }
                        }).start();

                    } catch (IOException e) {
                        Log.e(TAG, "IOException"+TAG);
                        Log.e(TAG, Log.getStackTraceString(e));
                    }
                }
            }
        }).start();
    }

}

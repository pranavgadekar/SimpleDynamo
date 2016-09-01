package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.res.Resources;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by pranav on 5/2/16.
 */

public class ServerThread implements Runnable{
    private static String TAG = ServerThread.class.getSimpleName();
    private static int SERVER_PORT = 10000;
    private static final byte INSERT = 0;
    private static final byte QUERY = 1;
    private static final byte QUERY_ALL = 2;
    private static final byte DELETE = 3;
    private static final byte SUCCESS = 4;
    private static final byte RECOVER = 5;
    private static final byte SENDBACK = 6;

    @Override
    public void run() {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            Log.e(TAG, "Server Socket IOException");
        }

        while (true){
            try {
                final Socket socket = serverSocket.accept();
                new Thread(new Runnable() {
                    @Override
                    public void run()
                    {
                        try {
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            objectOutputStream.flush();

                            ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                            Message message = (Message) objectInputStream.readObject();

                            int recoveryStatus = SimpleDynamoProvider.waitForRecovery();
                            if(recoveryStatus ==1){
                                Log.e(TAG,"Recovery Successful at Server");
                            }else {
                                Log.e(TAG,"Recovery failed at Server");
                            }

                            switch (message.getMessageType()){
                                case INSERT:
                                    SimpleDynamoProvider.allMessages.get(message.index).putAll(message.map);
                                    if (SimpleDynamoProvider.queryMessages.size() > 0) {
                                        String key = message.map.keySet().iterator().next();
                                        Object object = SimpleDynamoProvider.queryMessages.get(key);
                                        if (object != null) {
                                            SimpleDynamoProvider.queryMessages.remove(key);
                                            synchronized (object) {
                                                object.notifyAll();
                                            }
                                        }
                                    }
                                    if (message.isForwardMessage()) {
                                        int port = SimpleDynamoProvider.circularLinkedList.getNextNode(SimpleDynamoProvider.myPort);
                                        int index = message.index + 1;
                                        ClientThread clientThread = new ClientThread(SimpleDynamoProvider.myPort, port, message.messageType, index, message.map);
                                        Thread thread = new Thread(clientThread);
                                        thread.start();
                                    }
                                    if (message.isSkipMessage()) {
                                        int port = SimpleDynamoProvider.circularLinkedList.getPrevNode(SimpleDynamoProvider.myPort);
                                        int index = message.index - 1;
                                        ClientThread clientThread = new ClientThread(SimpleDynamoProvider.myPort, port, SENDBACK, index, message.map);
                                        Thread thread = new Thread(clientThread);
                                        thread.start();
                                    }
                                    objectOutputStream.writeObject(new Message(SUCCESS, 0, false, null));
                                    break;
                                case DELETE:
                                {
                                    String key = message.getMap().keySet().iterator().next();
                                    SimpleDynamoProvider.allMessages.get(message.index).remove(key);
                                    if (message.isForwardMessage()) {
                                        int port = SimpleDynamoProvider.circularLinkedList.getNextNode(SimpleDynamoProvider.myPort);
                                        int index = message.getIndex() + 1;
                                        ClientThread clientThread = new ClientThread(SimpleDynamoProvider.myPort, port, message.getMessageType(), index, message.getMap());
                                        Thread thread = new Thread(clientThread);
                                        thread.start();
                                    }
                                    objectOutputStream.writeObject(new Message(SUCCESS, 0, false, null));
                                }
                                break;
                                case QUERY:
                                {
                                    String key = message.map.keySet().iterator().next();
                                    String value = SimpleDynamoProvider.allMessages.get(message.index).get(key);
                                    if (value == null) {
                                        Object object = SimpleDynamoProvider.queryMessages.get(key);
                                        if (object == null) {
                                            object = new Object();
                                            SimpleDynamoProvider.queryMessages.put(key, object);
                                        }
                                        synchronized (object) {
                                            try {
                                                object.wait();
                                            } catch (InterruptedException e) {
                                                Log.e(TAG, "Key wait interrupted");
                                            }
                                        }
                                        value = SimpleDynamoProvider.allMessages.get(message.index).get(key);
                                    }
                                    message.map.put(key, value);
                                    objectOutputStream.writeObject(message);
                                }
                                    break;

                                case QUERY_ALL:
                                {
                                    message.setMap(SimpleDynamoProvider.allMessages.get(message.getIndex()));
                                    objectOutputStream.writeObject(message);
                                }
                                    break;
                                default:
                                {
                                    message.map = SimpleDynamoProvider.allMessages.get(message.index);
                                    objectOutputStream.writeObject(message);
                                }
                                    break;
                            }
                            objectOutputStream.close();

                        } catch (ClassNotFoundException e) {
                            Log.e(TAG, "Inner Thread ObjectInputStream ClassNotFoundException");
                        } catch (IOException e) {
                            Log.e(TAG, "Inner Thread socket IOException");
                        }
                    }
                }).start();
            } catch (IOException e) {
                Log.e(TAG, "server socket IOException");
            }
        }
    }
}
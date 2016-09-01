package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by pranav on 5/2/16.
 */
public class ClientThread implements Runnable {
    private static final byte INSERT = 0;
    private static final byte QUERY = 1;
    private static final byte QUERY_ALL = 2;
    private static final byte DELETE = 3;
    private static final byte SUCCESS = 4;
    private static final byte RECOVER = 5;
    private static final byte SENDBACK = 6;
    private static String TAG = ClientThread.class.getSimpleName();

    public int originPort;
    public int destinationPort;
    public byte type;
    public int index;
    public ConcurrentHashMap<String, String> map;

    public ClientThread(int originPort, int destinationPort, byte type, int index, ConcurrentHashMap<String, String> map) {
        this.originPort = originPort;
        this.destinationPort = destinationPort;
        this.type = type;
        this.index = index;
        this.map = map;
    }


    @Override
    public void run() {

        if (!clientChild(new Message(destinationPort, type, index, map, false))) {
            if (type == INSERT) {
                if (index != SimpleDynamoProvider.N-1) {
                    int newPort = SimpleDynamoProvider.circularLinkedList.getNextNode(destinationPort);
                    int newIndex = index + 1;
                    clientChild(new Message(newPort, INSERT, newIndex, map, true));
                }
            else if (type == DELETE){
                    if (index != SimpleDynamoProvider.N-1) {
                        int newPort = SimpleDynamoProvider.circularLinkedList.getNextNode(destinationPort);
                        int newIndex = index + 1;
                        clientChild(new Message(newPort, DELETE, newIndex, map, true));
                    }
                }
            } else if (type == QUERY || type == QUERY_ALL) {
                int newPort = SimpleDynamoProvider.circularLinkedList.getPrevNode(destinationPort);
                int newIndex = index - 1;
                clientChild(new Message(newPort, type, newIndex, map, false));
            }
        } else if (type == SENDBACK){
            clientChild(new Message(destinationPort, INSERT, index, map, false));
        }
    }

    public static boolean clientChild(Message message) {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), message.destinationPort);
            socket.setSoTimeout(10000);
            ObjectOutputStream objectOutputStream = null;
            ObjectInputStream objectInputStream = null;
            boolean forwardMessage;

            if (message.messageType == INSERT && message.getIndex() < SimpleDynamoProvider.N-1) {
                forwardMessage = true;
            } else if (message.messageType == DELETE && message.getIndex() < SimpleDynamoProvider.N-1){
                forwardMessage = true;
            } else {
                forwardMessage = false;
            }

            Message m = new Message(message.messageType, message.index, forwardMessage, message.getMap());
            m.setSkipMessage(message.skipMessage);

            try {
                objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                objectOutputStream.writeObject(m);
            } catch (IOException e) {
                socket.close();
                return false;
            }

            try {
                objectInputStream = new ObjectInputStream(socket.getInputStream());;
                m = (Message) objectInputStream.readObject();
            } catch (IOException e) {
                socket.close();
                return false;
            }

            objectOutputStream.close();
            socket.close();

            if (message.getMessageType()==QUERY || message.getMessageType() == QUERY_ALL){
                SimpleDynamoProvider.localMessages.putAll(m.map);
                if (message.getMessageType()==QUERY){
                    synchronized (SimpleDynamoProvider.localMessages) {
                        SimpleDynamoProvider.localMessages.notify();
                    }
                }
                else if (message.getMessageType() == QUERY_ALL){
                    if (SimpleDynamoProvider.notifyFlag.incrementAndGet() == SimpleDynamoProvider.circularLinkedList.getSize()) {
                        SimpleDynamoProvider.notifyFlag.set(0);
                        synchronized (SimpleDynamoProvider.notifyFlag) {
                            SimpleDynamoProvider.notifyFlag.notify();
                        }
                    }
                }
            } else if (message.getMessageType() == RECOVER) {
                if (message.destinationPort == SimpleDynamoProvider.circularLinkedList.getPrevNode(SimpleDynamoProvider.myPort)) {
                    SimpleDynamoProvider.allMessages.get(2).putAll(m.map);
                }
                else if (message.destinationPort == SimpleDynamoProvider.circularLinkedList.getNextNode(SimpleDynamoProvider.myPort)) {
                    SimpleDynamoProvider.allMessages.get(1).putAll(m.map);
                }
                else {
                    SimpleDynamoProvider.allMessages.get(0).putAll(m.map);
                }

                if (SimpleDynamoProvider.notifyFlag.incrementAndGet() == SimpleDynamoProvider.N) {
                    SimpleDynamoProvider.recoverFlag = false;
                    SimpleDynamoProvider.notifyFlag.set(0);
                    synchronized (SimpleDynamoProvider.notifyFlag) {
                        SimpleDynamoProvider.notifyFlag.notifyAll();
                        Log.e(TAG,"Recovery complete notification sent successfully to all");
                    }
                }
            }

        } catch (ClassNotFoundException e) {
            Log.e(TAG, "client ObjectInputStream ClassNotFoundException");
            return false;
        } catch (IOException e) {
            Log.e(TAG, "client socket IOException");
            return false;
        }

        return true;
    }



}

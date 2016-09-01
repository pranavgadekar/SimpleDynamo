package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by pranav on 5/2/16.
 */

public class Message implements Serializable {
    public int originPort;
    public int destinationPort;
    public byte messageType;
    public int index;
    public boolean forwardMessage = false;
    public boolean skipMessage = false;
    public ConcurrentHashMap<String, String> map;


    public Message(byte messageType, int index, boolean forwardMessage, ConcurrentHashMap<String, String> map) {
        this.messageType = messageType;
        this.index = index;
        this.forwardMessage = forwardMessage;
        this.map = map;
    }

    public Message(int destinationPort, byte messageType, int index, ConcurrentHashMap<String, String> map, boolean skipMessage) {
        this.destinationPort = destinationPort;
        this.messageType = messageType;
        this.index = index;
        this.map = map;
        this.skipMessage = skipMessage;
    }

    public byte getMessageType() {
        return messageType;
    }

    public void setMessageType(byte messageType) {
        this.messageType = messageType;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isForwardMessage() {
        return forwardMessage;
    }

    public void setForwardMessage(boolean forwardMessage) {
        this.forwardMessage = forwardMessage;
    }

    public boolean isSkipMessage() {
        return skipMessage;
    }

    public void setSkipMessage(boolean skipMessage) {
        this.skipMessage = skipMessage;
    }

    public ConcurrentHashMap<String, String> getMap() {
        return map;
    }

    public void setMap(ConcurrentHashMap<String, String> map) {
        this.map = map;
    }
}
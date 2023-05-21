package org.abigtomato.learn.model;

public class Event {

    public String user;

    public String path;

    public long timestamp;

    public Event(String user, String path, long timestamp) {
        this.user = user;
        this.path = path;
        this.timestamp = timestamp;
    }
}

package com.credit;

public class Event {

    public enum State {
        STARTED, FINISHED
    }

    public enum Type {
        STANDARD, APPLICATION_LOG
    }

    public String id, host;
    public long timestamp;
    public State state;
    public Type type = Type.STANDARD;

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Event))
            return false;
        return this.id.equals(((Event) obj).id);
    }

    @Override
    public String toString() {
        if (type == Type.STANDARD)
            return String.format("{\"id\":\"%s\", \"state\":\"%s\", \"timestamp\":%d}", id, state, timestamp);
        else
            return String.format("{\"id\":\"%s\", \"state\":\"%s\", \"type\":\"%s\", \"host\":\"%s\", \"timestamp\":%d}", id, state, type, host, timestamp);
    }
}

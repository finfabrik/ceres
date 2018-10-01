package com.blokaly.ceres.hitbtc.event;

public abstract class AbstractEvent {

    private String method;
    public AbstractEvent(String method) { this.method = method; }
    public String getMethod() { return method; }
    public EventType getType() {
        return EventType.get(method);
    }
}

package com.blokaly.ceres.hitbtc.event;

public class ErrorEvent extends AbstractEvent {
    private Error error;
    private long id;

    public long getCode() { return error.code; }
    public String getMessage() { return error.message; }
    public String getDescription() { return error.description; }
    public long getId() { return id; }

    ErrorEvent(){
        super(EventType.ERROR.getType());
    }

    @Override
    public String toString() {
        return "ErrorEvent={" +
                "code:" + error.code +
                ",message:" + error.message +
                ",description:" + error.description + "}";
    }

    private static class Error{
        private final long code;
        private final String message;
        private final String description;

        Error(final long code, final String message, final String description){
            this.code = code;
            this.message = message;
            this.description = description;
        }
    }
}

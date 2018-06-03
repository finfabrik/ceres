package com.blokaly.ceres.bitfinex.event;

import com.google.common.base.MoreObjects;

public class InfoEvent extends AbstractEvent {

    public enum Status {
        WEB_SOCKET_RESTART(20051), PAUSE(20060), RESUME(20061), UNKNOWN(0);
        private final int statusCode;

        Status(int statusCode) {
            this.statusCode = statusCode;
        }
    }

    private String version;
    private String code;
    private String msg;
    private Platform platform;

    public String getVersion() {
        return version;
    }

    public Status getStatus() {
        if (code == null || code.isEmpty()) {
            return Status.UNKNOWN;
        }

        switch (Integer.parseInt(code)) {
            case 20051: return Status.WEB_SOCKET_RESTART;
            case 20060: return Status.PAUSE;
            case 20061: return Status.RESUME;
            default: return Status.UNKNOWN;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("InfoEvent{");
        sb.append("version=").append(version);
        if (code != null) {
            sb.append(", code=").append(code);
            sb.append(", msg=").append(msg);
        }
        if (platform != null) {
            sb.append(", status=").append(platform.status);
        }
        sb.append('}');
        return sb.toString();
    }

    private static class Platform {
        private int status;
    }
}

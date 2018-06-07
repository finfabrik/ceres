package com.blokaly.ceres.bitfinex;

import com.blokaly.ceres.network.WSConnectionListener;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.URI;

public class BitfinexClient extends WebSocketClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(BitfinexClient.class);
    private static final String client = "bitfinex";
    private final JsonCracker cracker;
    private final WSConnectionListener listener;
    private volatile boolean stop = false;

    public BitfinexClient(URI serverURI, JsonCracker cracker, WSConnectionListener listener) {
        super(serverURI);
        this.cracker = cracker;
        this.listener = listener;
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
        LOGGER.info("ws open - status {}:{}", handshake.getHttpStatus(), handshake.getHttpStatusMessage());
        if (listener != null) {
            listener.onConnected(client);
        }
    }

    @Override
    public void onMessage(String message) {
        LOGGER.debug("ws message: {}", message);
        if (!stop) {
            cracker.crack(message);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        LOGGER.info("ws close - reason: {}", reason);
        if (listener != null) {
            listener.onDisconnected(client);
        }
    }

    public void tryReconnect() {
        LOGGER.info("ws reconnecting...");
        if (listener != null) {
            listener.reconnect(client);
        }
    }

    @Override
    public void onError(Exception ex) {
        LOGGER.error("ws error", ex);
    }

    @PreDestroy
    public void stop() {
        stop = true;
        super.close();
    }
}

package com.blokaly.ceres.hitbtc;

import com.blokaly.ceres.network.WSConnectionListener;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.URI;

public class HitbtcClient extends WebSocketClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HitbtcClient.class);
    private static final String client = "hitbtc";
    private final JsonCracker cracker;
    private final WSConnectionListener listener;
    private volatile boolean stop = false;

    public HitbtcClient(URI serverURI, JsonCracker cracker, WSConnectionListener listener){
        super(serverURI);
        this.cracker = cracker;
        this.listener = listener;
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        LOGGER.info("ws open - status {}:{}", handshakedata.getHttpStatus(), handshakedata.getHttpStatusMessage());
        if(listener != null){
            listener.onConnected(client);
            cracker.subscribeOrderBooks();
        }
    }

    @Override
    public void onMessage(String message) {
        LOGGER.debug("ws message: {}", message);
        if(!stop){
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

package com.blokaly.ceres.bitfinex;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockModule extends AbstractModule {

    private static Logger LOGGER = LoggerFactory.getLogger(MockModule.class);

    @Override
    protected void configure() {

    }

    @Provides
    public MessageSender provideMessageSender() {
        return new MessageSender() {
            @Override
            public void send(String message) {

            }
        };
    }

    @Provides
    public Service provideService() {
        return new Service() {
            @Override
            public void start() throws Exception {
                LOGGER.info("Mock service started");
            }

            @Override
            public void stop() {
                LOGGER.info("Mock service stopped");
            }
        };
    }
}

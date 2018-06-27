package com.blokaly.ceres.kafka;

import com.blokaly.ceres.system.CommonConfigs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.typesafe.config.Config;
import org.apache.kafka.clients.producer.Producer;

import javax.annotation.PreDestroy;

@Singleton
public class StringProducer extends TopicStringProducer {
  private final String topic;

  @Inject
  public StringProducer(Producer<String, String> producer, Config config) {
    super(producer);
    topic = config.getString(CommonConfigs.KAFKA_TOPIC);
  }

  @PreDestroy
  public void stop() {
    super.stop();
  }

  public void publish(String key, String text) {
   super.publish(topic, key, text);
  }
}

package com.blokaly.ceres.kafka;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;

@Singleton
public class TopicStringProducer {
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Producer<String, String> producer;
  private volatile boolean closing = false;

  @Inject
  public TopicStringProducer(Producer<String, String> producer) {
    this.producer = producer;
  }

  @PreDestroy
  public void stop() {
    closing = true;
    producer.flush();
    producer.close();
  }

  public void publish(String topic, String key, String text) {
    if (closing) {
      return;
    }

    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, text);
    LOGGER.debug("publishing[{}]: {}->{}", topic, key, text);
    producer.send(record, (metadata, exception) -> {
      if (metadata==null || exception != null) {
        LOGGER.error("Error sending Kafka message", exception);
      }
    });
  }
}

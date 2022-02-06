package com.Kafka.intro.kafka_starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.HashMap;
import java.util.Map;

public class Producer extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("acks", "1");

    KafkaProducer<String,String> producer = KafkaProducer.create(vertx,config);

    for (int i = 0; i < 5; i++) {
      // only topic and message value are specified, round-robin on destination partitions
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", "message_" + i);

      producer.write(record);
    }

//    producer
//      .close()
//      .onSuccess(v -> System.out.println("Producer is now closed"))
//      .onFailure(cause -> System.out.println("Close failed: " + cause));
  }
}

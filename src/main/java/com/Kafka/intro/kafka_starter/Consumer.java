package com.Kafka.intro.kafka_starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Consumer extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", "localhost:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");

    KafkaConsumer<String,String> consumer = KafkaConsumer.create(vertx, config);

    consumer.handler(record -> {
//      System.out.println("Processing key = " + record.key() + " ,value = " + record.value() +
//        " ,partition = " + record.partition() + " ,offset = " + record.offset());
      System.out.println(record.value());
    });

    //subscribe to a topic...
    Set<String> topics = new HashSet<>();
    topics.add("Cricket");
    topics.add("Football");
    consumer
      .subscribe(topics)
      .onSuccess(v ->
        System.out.println("subscribed")
      ).onFailure(cause ->
        System.out.println("Could not subscribe " + cause.getMessage())
      );

//    consumer
//      .close()
//      .onSuccess(v -> System.out.println("Consumer is now closed"))
//      .onFailure(cause -> System.out.println("Close failed: " + cause));
  }
}

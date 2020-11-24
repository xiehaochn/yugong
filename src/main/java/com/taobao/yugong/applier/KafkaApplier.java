package com.taobao.yugong.applier;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class KafkaApplier extends AbstractRecordApplier {
  private Producer<String, String> producer;
  private String topic;

  public KafkaApplier() {}

  public KafkaApplier(
      String bootstrapServer,
      String acks,
      int retries,
      int batchSize,
      int lingerMs,
      int bufferMemory,
      String topic) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ProducerConfig.ACKS_CONFIG, acks);
    props.put(ProducerConfig.RETRIES_CONFIG, retries);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    this.topic = topic;
    producer = new KafkaProducer<>(props);
  }

  public void apply(List<Record> records) throws YuGongException {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    for (Record record : records) {
      try {
        String recordJson = objectMapper.writeValueAsString(record);
        producer.send(
            new ProducerRecord<>(
                topic,
                (record.getSchemaName() + "." + record.getTableName()).toLowerCase(),
                recordJson));
      } catch (JsonProcessingException e) {
        logger.error("record convert to json string failed", e);
        logger.error(("record:") + record.toString());
      }
    }
    producer.flush();
  }
}

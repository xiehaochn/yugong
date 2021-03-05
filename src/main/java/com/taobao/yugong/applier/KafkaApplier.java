package com.taobao.yugong.applier;

import com.ccb.cdc.flink.entity.yugong.YuGongColumnMeta;
import com.ccb.cdc.flink.entity.yugong.YuGongColumnValue;
import com.ccb.cdc.flink.entity.yugong.YuGongRecordEntity;
import com.google.common.collect.Lists;
import com.taobao.yugong.common.audit.RecordDumper;
import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.IncrementOpType;
import com.taobao.yugong.common.model.record.OracleIncrementRecord;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaApplier extends AbstractRecordApplier {
  private Producer<String, YuGongRecordEntity> producer;
  private String topic;
  private int partitionNum;

  private static final Logger logger = LoggerFactory.getLogger(KafkaApplier.class);

  public KafkaApplier(Configuration config) {
    this.topic = config.getString("yugong.applier.kafka.topic", "yugong_default_topic");
    this.partitionNum = config.getInt("yugong.applier.kafka.partitionNum", 0);
    Properties props = new Properties();
    props.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.getString("yugong.applier.kafka.bootstrap.servers", "127.0.0.1:9092"));
    props.put(ProducerConfig.ACKS_CONFIG, config.getString("yugong.applier.kafka.acks", "all"));
    props.put(ProducerConfig.RETRIES_CONFIG, config.getInt("yugong.applier.kafka.retries", 0));
    props.put(
        ProducerConfig.BATCH_SIZE_CONFIG, config.getInt("yugong.applier.kafka.batch.size", 16384));
    props.put(ProducerConfig.LINGER_MS_CONFIG, config.getInt("yugong.applier.kafka.linger.ms", 1));
    props.put(
        ProducerConfig.BUFFER_MEMORY_CONFIG,
        config.getInt("yugong.applier.kafka.buffer.memory", 33554432));
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "com.taobao.yugong.common.utils.YuGongEntSerializer");
    File krb5File = new File(config.getString("yugong.applier.kafka.kerberos.krb5.file", ""));
    File jaasFile = new File(config.getString("yugong.applier.kafka.kerberos.jaas.file", ""));
    if (krb5File.exists() && jaasFile.exists()) {
      System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
      System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      props.put("security.protocol", "SASL_PLAINTEXT");
      props.put("sasl.kerberos.service.name", "kafka");
      props.put("sasl.mechanism", "GSSAPI");
    } else {
      logger.info("kerberos config file not exists.file path:");
    }
    producer = new KafkaProducer<>(props);
  }

  public void apply(List<Record> records) throws YuGongException {
    if (records.size() == 0) {
      return;
    }
    List<YuGongRecordEntity> yuGongRecordEntities = transToYuGongRecEnt(records);
    List<Integer> partitionNumList = getParNumList(records);
    RecordDumper.applierLog("start send.size:" + yuGongRecordEntities.size());
    for (int i = 0; i < yuGongRecordEntities.size(); i++) {
      producer.send(
          new ProducerRecord<>(topic, partitionNumList.get(i), "key", yuGongRecordEntities.get(i)));
    }
    RecordDumper.applierLog("end send");
  }

  private List<Integer> getParNumList(List<Record> records) {
    List<Integer> partitionNumList = Lists.newArrayList();
    for (Record record : records) {
      String fullTableName = (record.getSchemaName() + "." + record.getTableName()).toUpperCase();
      if (partitionNum == 0) {
        partitionNumList.add(0);
      } else {
        partitionNumList.add(Math.abs(fullTableName.hashCode() % partitionNum));
      }
    }
    return partitionNumList;
  }

  private List<YuGongRecordEntity> transToYuGongRecEnt(List<Record> records) {
    List<YuGongRecordEntity> yuGongRecordEntities = Lists.newArrayList();
    for (Record record : records) {
      yuGongRecordEntities.add(transToYuGongEnt(record));
    }
    return yuGongRecordEntities;
  }

  private YuGongRecordEntity transToYuGongEnt(Record record) {
    YuGongRecordEntity yuGongRecordEntity = new YuGongRecordEntity();
    yuGongRecordEntity.setTableName(record.getTableName());
    yuGongRecordEntity.setSchemaName(record.getSchemaName());
    yuGongRecordEntity.setTs(record.getTs());
    yuGongRecordEntity.setPrimaryKeys(transToYuGongColumnValueList(record.getPrimaryKeys()));
    yuGongRecordEntity.setColumns(transToYuGongColumnValueList(record.getColumns()));
    if (record instanceof OracleIncrementRecord) {
      OracleIncrementRecord oracleIncrementRecord = (OracleIncrementRecord) record;
      if (oracleIncrementRecord.getOpType() == IncrementOpType.I) {
        yuGongRecordEntity.setOpType("I");
      } else if (oracleIncrementRecord.getOpType() == IncrementOpType.U) {
        yuGongRecordEntity.setOpType("U");
      } else {
        yuGongRecordEntity.setOpType("D");
      }
      yuGongRecordEntity.setRowId(transToYuGongColumnValue(oracleIncrementRecord.getRowId()));
    }
    return yuGongRecordEntity;
  }

  private List<YuGongColumnValue> transToYuGongColumnValueList(List<ColumnValue> colList) {
    List<YuGongColumnValue> yuGongColumnValueList = new ArrayList<>();
    for (ColumnValue columnValue : colList) {
      yuGongColumnValueList.add(transToYuGongColumnValue(columnValue));
    }
    return yuGongColumnValueList;
  }

  private YuGongColumnValue transToYuGongColumnValue(ColumnValue columnValue) {
    YuGongColumnValue yuGongColumnValue = new YuGongColumnValue();
    yuGongColumnValue.setValue(columnValue.getValue());
    yuGongColumnValue.setCheck(columnValue.isCheck());
    yuGongColumnValue.setColumn(transToYuGongColumnMeta(columnValue.getColumn()));
    return yuGongColumnValue;
  }

  private YuGongColumnMeta transToYuGongColumnMeta(ColumnMeta column) {
    YuGongColumnMeta yuGongColumnMeta = new YuGongColumnMeta();
    yuGongColumnMeta.setName(column.getName());
    yuGongColumnMeta.setType(column.getType());
    return yuGongColumnMeta;
  }
}

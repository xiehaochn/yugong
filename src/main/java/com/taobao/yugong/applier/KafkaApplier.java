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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class KafkaApplier extends AbstractRecordApplier {
  private Producer<String, YuGongRecordEntity> producer;
  private String topic;
  private int partitionNum;
  private HashMap<String, Integer> taPartitionMap = new HashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(KafkaApplier.class);

  public KafkaApplier(
      String bootstrapServer,
      String acks,
      int retries,
      int batchSize,
      int lingerMs,
      int bufferMemory,
      String topic,
      String krb5FilePath,
      String jaasFilePath,
      int partitionNum) {
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
        "com.taobao.yugong.common.utils.YuGongEntSerializer");
    this.topic = topic;
    this.partitionNum = partitionNum;
    File krb5File = new File(krb5FilePath);
    File jaasFile = new File(jaasFilePath);
    if (krb5File.exists() && jaasFile.exists()) {
      System.setProperty("java.security.krb5.conf", krb5File.getAbsolutePath());
      System.setProperty("java.security.auth.login.config", jaasFile.getAbsolutePath());
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      props.put("security.protocol", "SASL_PLAINTEXT");
      props.put("sasl.kerberos.service.name", "kafka");
      props.put("sasl.mechanism", "GSSAPI");
    } else {
      logger.info("kerberos config file not exists.file path:");
      logger.info("krb5 file path:" + krb5FilePath);
      logger.info("jaas file path:" + jaasFilePath);
    }
    producer = new KafkaProducer<>(props);
    initPartitionMap();
  }

  private void initPartitionMap() {
    taPartitionMap.put("TA0DB.TA_PD_DVDN_DTL_TBL_0", 0);
    taPartitionMap.put("TA0DB.TA_PD_DVDN_DTL_TBL_1", 1);
    taPartitionMap.put("TA1DB.TA_PD_DVDN_DTL_TBL_0", 2);
    taPartitionMap.put("TA1DB.TA_PD_DVDN_DTL_TBL_1", 3);
    taPartitionMap.put("TA2DB.TA_PD_DVDN_DTL_TBL_0", 4);
    taPartitionMap.put("TA2DB.TA_PD_DVDN_DTL_TBL_1", 5);
    taPartitionMap.put("TA3DB.TA_PD_DVDN_DTL_TBL_0", 6);
    taPartitionMap.put("TA3DB.TA_PD_DVDN_DTL_TBL_1", 7);
    taPartitionMap.put("TA0DB.DVDN_DTL_CFM_0", 0);
    taPartitionMap.put("TA0DB.DVDN_DTL_CFM_1", 1);
    taPartitionMap.put("TA1DB.DVDN_DTL_CFM_0", 2);
    taPartitionMap.put("TA1DB.DVDN_DTL_CFM_1", 3);
    taPartitionMap.put("TA2DB.DVDN_DTL_CFM_0", 4);
    taPartitionMap.put("TA2DB.DVDN_DTL_CFM_1", 5);
    taPartitionMap.put("TA3DB.DVDN_DTL_CFM_0", 6);
    taPartitionMap.put("TA3DB.DVDN_DTL_CFM_1", 7);
  }

  //  public void apply(List<Record> records) throws YuGongException {
  //    ObjectMapper objectMapper = new ObjectMapper();
  //    objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
  //    ArrayList<String> recordJsonString = new ArrayList<>();
  //    ArrayList<Integer> partitionNumList = new ArrayList<>();
  //    for (Record record : records) {
  //      String fullTableName = (record.getSchemaName() + "." +
  // record.getTableName()).toUpperCase();
  //      try {
  //        String recordJson = objectMapper.writeValueAsString(record);
  //        recordJsonString.add(recordJson);
  //        if (partitionNum == 0) {
  //          partitionNumList.add(0);
  //        } else {
  //          //          partitionNumList.add(Math.abs(fullTableName.hashCode() % partitionNum));
  //          partitionNumList.add(taPartitionMap.get(fullTableName));
  //        }
  //      } catch (JsonProcessingException e) {
  //        logger.error("record convert to json string failed", e);
  //        logger.error(("record:") + record.toString());
  //      }
  //    }
  //    RecordDumper.applierLog("start send.size:" + recordJsonString.size());
  //    for (int i = 0; i < recordJsonString.size(); i++) {
  //      producer.send(
  //          new ProducerRecord<>(topic, partitionNumList.get(i), "key", recordJsonString.get(i)));
  //    }
  //    RecordDumper.applierLog("end send");
  //  }

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
        //          partitionNumList.add(Math.abs(fullTableName.hashCode() % partitionNum));
        partitionNumList.add(taPartitionMap.get(fullTableName));
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

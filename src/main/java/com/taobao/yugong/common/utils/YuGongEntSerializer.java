package com.taobao.yugong.common.utils;

import com.ccb.cdc.flink.entity.yugong.YuGongRecordEntity;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

public class YuGongEntSerializer implements Serializer<YuGongRecordEntity> {

  @Override
  public byte[] serialize(String s, YuGongRecordEntity yuGongRecordEntity) {
    return SerializationUtils.serialize(yuGongRecordEntity);
  }
}

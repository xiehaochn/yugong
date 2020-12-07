package com.taobao.yugong.applier;

import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.exception.YuGongException;

import java.util.List;

public class EmptyApplier extends AbstractRecordApplier {
  @Override
  public void apply(List<Record> records) throws YuGongException {}
}

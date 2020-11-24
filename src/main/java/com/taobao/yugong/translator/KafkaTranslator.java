package com.taobao.yugong.translator;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.db.meta.Table;
import com.taobao.yugong.common.model.record.IncrementOpType;
import com.taobao.yugong.common.model.record.IncrementRecord;
import com.taobao.yugong.common.model.record.Record;

import java.util.List;

public class KafkaTranslator implements DataTranslator {
  private Table table;

  @Override
  public String translatorSchema() {
    return null;
  }

  @Override
  public String translatorTable() {
    return null;
  }

  @Override
  public boolean translator(Record record) {
    return true;
  }

  @Override
  public List<Record> translator(List<Record> records) {
    for (Record record : records) {
      if (record instanceof IncrementRecord
          && ((IncrementRecord) record).getOpType().equals(IncrementOpType.D)) {
        List<ColumnValue> columns = record.getColumns();
        addNullValue(columns);
      }
    }
    return records;
  }

  private void addNullValue(List<ColumnValue> columns) {
    List<ColumnMeta> columnMetas = table.getColumns();
    for (ColumnMeta columnMeta : columnMetas) {
      columns.add(new ColumnValue(columnMeta, "null"));
    }
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }
}

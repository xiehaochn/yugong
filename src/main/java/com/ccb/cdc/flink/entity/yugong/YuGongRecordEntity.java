package com.ccb.cdc.flink.entity.yugong;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class YuGongRecordEntity implements Serializable {
  private String schemaName;
  private String tableName;
  private String ts;
  private List<YuGongColumnValue> primaryKeys = new ArrayList<>();
  private List<YuGongColumnValue> columns = new ArrayList<>();
  private String opType;
  private YuGongColumnValue rowId;
  private String discardType = "NONE";

  public YuGongRecordEntity() {}

  public String getTs() {
    return ts;
  }

  public void setTs(String ts) {
    this.ts = ts;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<YuGongColumnValue> getPrimaryKeys() {
    return primaryKeys;
  }

  public void setPrimaryKeys(List<YuGongColumnValue> primaryKeys) {
    this.primaryKeys = primaryKeys;
  }

  public List<YuGongColumnValue> getColumns() {
    return columns;
  }

  public void setColumns(List<YuGongColumnValue> columns) {
    this.columns = columns;
  }

  public String getOpType() {
    return opType;
  }

  public void setOpType(String opType) {
    this.opType = opType;
  }

  public YuGongColumnValue getRowId() {
    return rowId;
  }

  public void setRowId(YuGongColumnValue rowId) {
    this.rowId = rowId;
  }

  public String getDiscardType() {
    return discardType;
  }

  public void setDiscardType(String discardType) {
    this.discardType = discardType;
  }

  public String getFullTableName() {
    return (schemaName + "." + tableName).toUpperCase();
  }

  @Override
  public String toString() {
    return "YuGongRecordEntity{"
        + "schemaName='"
        + schemaName
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", ts='"
        + ts
        + '\''
        + ", primaryKeys="
        + primaryKeys
        + ", columns="
        + columns
        + ", opType='"
        + opType
        + '\''
        + ", rowId="
        + rowId
        + ", discardType='"
        + discardType
        + '\''
        + '}';
  }
}

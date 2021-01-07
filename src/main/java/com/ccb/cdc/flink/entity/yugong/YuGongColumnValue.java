package com.ccb.cdc.flink.entity.yugong;

import java.io.Serializable;

public class YuGongColumnValue implements Serializable {
  private YuGongColumnMeta column;
  private Object value;
  private boolean check = true; // 是否需要做数据对比

  public YuGongColumnValue() {}

  public YuGongColumnValue(YuGongColumnMeta column, String value, boolean check) {
    this.column = column;
    this.value = value;
    this.check = check;
  }

  public YuGongColumnMeta getColumn() {
    return column;
  }

  public void setColumn(YuGongColumnMeta column) {
    this.column = column;
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public boolean isCheck() {
    return check;
  }

  public void setCheck(boolean check) {
    this.check = check;
  }

  @Override
  public String toString() {
    return "YuGongColumnValue{"
        + "column="
        + column
        + ", value="
        + value
        + ", check="
        + check
        + '}';
  }
}

package com.ccb.cdc.flink.entity.yugong;

import java.io.Serializable;

public class YuGongColumnMeta implements Serializable {
  private String name;
  private int type;

  public YuGongColumnMeta() {}

  public YuGongColumnMeta(String name, int type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public String getNameWithQuotes() {
    return "\"" + name + "\"";
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "YuGongColumnMeta{" + "name='" + name + '\'' + ", type=" + type + '}';
  }
}

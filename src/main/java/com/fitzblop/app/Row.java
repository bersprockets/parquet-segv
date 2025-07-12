package com.fitzblop.app;

import java.io.Serializable;

public class Row implements Serializable {
  private long[] data;
  public Row(long[] data) {
    this.data = data;
  }
  public long[] getData() {
    return data;
  }
}

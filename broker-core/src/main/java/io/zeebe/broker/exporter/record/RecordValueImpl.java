package io.zeebe.broker.exporter.record;

import io.zeebe.exporter.record.RecordValue;

public abstract class RecordValueImpl implements RecordValue {
  private String json;

  protected abstract String writeJson();

  @Override
  public String toJson() {
    if (json == null) {
      json = writeJson();
    }

    return json;
  }
}

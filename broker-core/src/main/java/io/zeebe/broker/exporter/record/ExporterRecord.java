package io.zeebe.broker.exporter.record;

import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.RecordMetadata;
import io.zeebe.exporter.record.RecordValue;

public class ExporterRecord<T extends RecordValue> implements Record<T> {
  private final ExporterRecordMetadata metadata;
  private final T value;

  private String json;

  public ExporterRecord(final ExporterRecordMetadata metadata, T value) {
    this.metadata = metadata;
    this.value = value;
  }

  @Override
  public RecordMetadata getMetadata() {
    return metadata;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public String toJson() {
    if (json == null) {
      json = "";
    }

    return json;
  }
}

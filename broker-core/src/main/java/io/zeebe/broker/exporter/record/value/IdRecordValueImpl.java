package io.zeebe.broker.exporter.record.value;

import io.zeebe.broker.clustering.orchestration.id.IdRecord;
import io.zeebe.broker.exporter.record.RecordValueImpl;
import io.zeebe.exporter.record.value.IdRecordValue;

public class IdRecordValueImpl extends RecordValueImpl implements IdRecordValue {
  private final IdRecord record;

  public IdRecordValueImpl(final IdRecord record) {
    this.record = record;
  }

  @Override
  public int getId() {
    return record.getId();
  }

  @Override
  protected String writeJson() {
    final StringBuilder builder = new StringBuilder();
    record.writeJSON(builder);

    return builder.toString();
  }
}

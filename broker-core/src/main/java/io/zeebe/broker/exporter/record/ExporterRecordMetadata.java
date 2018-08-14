package io.zeebe.broker.exporter.record;

import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.util.buffer.BufferUtil;
import java.time.Instant;

public class ExporterRecordMetadata implements io.zeebe.exporter.record.RecordMetadata {
  private final int partitionId;
  private final LoggedEvent event;
  private final RecordMetadata metadata;

  private Instant timestamp;
  private String rejectionReason;

  public ExporterRecordMetadata(
      final int partitionId, final LoggedEvent event, final RecordMetadata metadata) {
    this.partitionId = partitionId;
    this.event = event;
    this.metadata = metadata;
  }

  @Override
  public long getKey() {
    return event.getKey();
  }

  @Override
  public Intent getIntent() {
    return metadata.getIntent();
  }

  @Override
  public int getPartitionId() {
    return partitionId;
  }

  @Override
  public long getPosition() {
    return event.getPosition();
  }

  @Override
  public RecordType getRecordType() {
    return metadata.getRecordType();
  }

  @Override
  public RejectionType getRejectionType() {
    return metadata.getRejectionType();
  }

  @Override
  public String getRejectionReason() {
    if (rejectionReason == null) {
      rejectionReason = BufferUtil.bufferAsString(metadata.getRejectionReason());
    }

    return rejectionReason;
  }

  @Override
  public long getSourceRecordPosition() {
    return event.getSourceEventPosition();
  }

  @Override
  public Instant getTimestamp() {
    if (timestamp == null) {
      timestamp = Instant.ofEpochMilli(event.getTimestamp());
    }

    return timestamp;
  }

  @Override
  public ValueType getValueType() {
    return metadata.getValueType();
  }

  @Override
  public String toJson() {
    return null;
  }
}

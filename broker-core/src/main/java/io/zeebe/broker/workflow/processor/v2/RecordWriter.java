package io.zeebe.broker.workflow.processor.v2;

import java.util.function.Consumer;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriterImpl;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.Intent;

public class RecordWriter {

  private static final Consumer<RecordMetadata> DO_NOTHING = m -> {};

  private final KeyGenerator keyGenerator; // TODO: must be part of snapshotted state

  private final TypedStreamWriterImpl streamWriter;
  private final ResponseWriter responseWriter;

  private TypedBatchWriter batchWriter;
  private int numStagedRecords;

  public RecordWriter(TypedStreamWriterImpl streamWriter, ResponseWriter responseWriter)
  {
    this.keyGenerator = new KeyGenerator();
    this.streamWriter = streamWriter;
    this.responseWriter = responseWriter;
  }


  public void reset(int producerId, long sourceRecordPosition)
  {
    streamWriter.configureSourceContext(producerId, sourceRecordPosition);
    batchWriter = streamWriter.newBatch();
    numStagedRecords = 0;
    responseWriter.reset();
  }

  public void publishCommand(Intent intent, UnpackedObject value)
  {
    publishRecord(-1, intent, value, RecordType.COMMAND, DO_NOTHING);
  }

  public void publishRejection(TypedRecord<? extends UnpackedObject> command,
      RejectionType type,
      String reason)
  {
    publishRejection(command, type, reason, DO_NOTHING);
  }

  // only required for instance creation => https://github.com/zeebe-io/zeebe/issues/1040
  public void publishRejection(TypedRecord<? extends UnpackedObject> command,
      RejectionType type,
      String reason,
      Consumer<RecordMetadata> metadataWriter)
  {
    final RecordMetadata metadata = command.getMetadata();
    final UnpackedObject value = command.getValue();

    // TODO: not garbage-free
    publishRecord(command.getKey(), metadata.getIntent(), value, RecordType.COMMAND_REJECTION, metadataWriter.andThen(m -> m.rejectionType(type).rejectionReason(reason)));
  }

  public void publishEvent(long key, Intent intent, UnpackedObject value)
  {
    publishRecord(key, intent, value, RecordType.EVENT, DO_NOTHING);
  }

  // only required for instance creation => https://github.com/zeebe-io/zeebe/issues/1040
  public void publishEvent(long key, Intent intent, UnpackedObject value, Consumer<RecordMetadata> metadata)
  {
    publishRecord(key, intent, value, RecordType.EVENT, metadata);
  }

  public void publishEvent(Intent intent, UnpackedObject value)
  {
    publishEvent(keyGenerator.nextKey(), intent, value);
  }

  private void publishRecord(long key, Intent intent, UnpackedObject value, RecordType recordType, Consumer<RecordMetadata> metadata)
  {
    batchWriter.addRecord(recordType, key, intent, value, metadata);
    numStagedRecords++;
  }

  public void sendAccept(TypedRecord<?> record, Intent intent)
  {
    responseWriter.sendAccept(record, intent);
  }

  public void sendReject(TypedRecord<?> record, RejectionType rejectionType, String rejectionReason)
  {
    responseWriter.sendReject(record, rejectionType, rejectionReason);
  }

  public long generateKey()
  {
    return keyGenerator.nextKey();
  }

  /**
   * Performs #writeEvents
   */
  public long flushEvents()
  {
    if (numStagedRecords > 0)
    {
      return batchWriter.write();
    }
    else
    {
      return 0L;
    }
  }

  /**
   * #executeSideeffects
   */
  public boolean flushSideEffects()
  {
    return responseWriter.flush();
  }

}

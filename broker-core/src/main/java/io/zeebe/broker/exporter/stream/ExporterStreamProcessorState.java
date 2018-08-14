package io.zeebe.broker.exporter.stream;

import io.zeebe.broker.exporter.stream.ExporterRecord.ExporterPosition;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.util.LangUtil;
import io.zeebe.util.buffer.BufferUtil;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class ExporterStreamProcessorState extends StateController {
  private final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

  public void setPosition(final String exporterId, final long position) {
    final byte[] key = exporterId.getBytes();
    final DirectBuffer keyBuffer = new UnsafeBuffer(key);

    setPosition(keyBuffer, position);
  }

  public void setPosition(final DirectBuffer exporterId, final long position) {
    final byte[] key = BufferUtil.bufferAsArray(exporterId);
    longBuffer.putLong(0, position);

    try {
      getDb().put(key, longBuffer.array());
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }
  }

  public long getPosition(final String exporterId) {
    return getPosition(BufferUtil.wrapString(exporterId));
  }

  public long getPosition(final DirectBuffer exporterId) {
    final byte[] key = BufferUtil.bufferAsArray(exporterId);
    long position = -1;

    try {
      final int bytesRead = getDb().get(key, longBuffer.array());
      if (bytesRead == Long.BYTES) {
        position = longBuffer.getLong(0);
      }
    } catch (RocksDBException e) {
      LangUtil.rethrowUnchecked(e);
    }

    return position;
  }

  public ExporterRecord newExporterRecord() {
    final ExporterRecord record = new ExporterRecord();

    try (final RocksIterator iterator = getDb().newIterator()) {
      for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
        final DirectBuffer wrapper = new UnsafeBuffer(iterator.value());
        final ExporterPosition position = record.getPositionsProperty().add();

        position.getIdProperty().setValue(BufferUtil.wrapArray(iterator.key()));
        position.getPositionProperty().setValue(wrapper.getLong(0));
      }
    }

    return record;
  }
}

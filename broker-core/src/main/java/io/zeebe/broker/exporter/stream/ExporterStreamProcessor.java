/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.exporter.stream;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.exporter.context.ExporterContext;
import io.zeebe.broker.exporter.repo.ExporterDescriptor;
import io.zeebe.broker.logstreams.processor.NoopSnapshotSupport;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.logstreams.log.LogStreamRecordWriter;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.logstreams.processor.EventProcessor;
import io.zeebe.logstreams.processor.StreamProcessor;
import io.zeebe.logstreams.processor.StreamProcessorContext;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.logstreams.state.StateController;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.protocol.intent.ExporterIntent;
import io.zeebe.util.sched.ActorControl;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExporterStreamProcessor implements StreamProcessor {
  private static final Logger LOG = Loggers.EXPORTER_LOGGER;
  private static final SnapshotSupport NONE = new NoopSnapshotSupport();

  private ExporterContainer currentExporter;
  private final List<ExporterContainer> containers;
  private final int partitionId;

  private final ExporterStreamProcessorState state = new ExporterStreamProcessorState();
  private final Processor eventProcessor = new Processor();

  private ActorControl actorControl;

  public ExporterStreamProcessor(
      final int partitionId, final List<ExporterDescriptor> descriptors) {
    this.partitionId = partitionId;

    this.containers = new ArrayList<>(descriptors.size());
    for (final ExporterDescriptor descriptor : descriptors) {
      this.containers.add(new ExporterContainer(descriptor));
    }
  }

  @Override
  public SnapshotSupport getStateResource() {
    return NONE;
  }

  @Override
  public StateController getStateController() {
    return state;
  }

  @Override
  public EventProcessor onEvent(LoggedEvent event) {
    eventProcessor.wrap(event);
    return eventProcessor;
  }

  @Override
  public void onOpen(StreamProcessorContext context) {
    actorControl = context.getActorControl();

    for (final ExporterContainer container : containers) {
      container.exporter.configure(container.context);
    }
  }

  @Override
  public void onRecovered() {
    for (final ExporterContainer container : containers) {
      container.exporter.open(container);
    }
  }

  @Override
  public void onClose() {
    for (final ExporterContainer container : containers) {
      container.exporter.close();
    }
  }

  private boolean shouldCommitPositions() {
    return false;
  }

  private class ExporterContainer implements Controller {
    private static final String LOGGER_NAME_FORMAT = "exporter-%s";

    private final ExporterContext context;
    private final Exporter exporter;
    private final long startPosition;

    public ExporterContainer(ExporterDescriptor descriptor) {
      context =
          new ExporterContext(
              LoggerFactory.getLogger(String.format(LOGGER_NAME_FORMAT, descriptor.getId())),
              descriptor.getConfiguration());
      exporter = descriptor.newInstance();
      startPosition = state.getPosition(descriptor.getId());
    }

    @Override
    public void updateLastExportedRecordPosition(final long position) {
      actorControl.run(() -> state.setPosition(context.getConfiguration().getId(), position));
    }

    @Override
    public void scheduleTask(final Duration delay, final Runnable task) {
      actorControl.runDelayed(delay, task);
    }
  }

  private class Processor implements EventProcessor {
    private final RecordMetadata metadata = new RecordMetadata();

    private Record record;
    private boolean shouldExecuteSideEffects;

    void wrap(LoggedEvent rawEvent) {
      rawEvent.readMetadata(metadata);

      shouldExecuteSideEffects = true;
      switch (metadata.getValueType()) {
        case JOB:
          break;
        case RAFT:
          break;
        case SUBSCRIPTION:
          break;
        case SUBSCRIBER:
          break;
        case DEPLOYMENT:
          break;
        case WORKFLOW_INSTANCE:
          break;
        case INCIDENT:
          break;
        case TOPIC:
          break;
        case ID:
          break;
        case MESSAGE:
          break;
        case MESSAGE_SUBSCRIPTION:
          break;
        case WORKFLOW_INSTANCE_SUBSCRIPTION:
          break;
        default:
          shouldExecuteSideEffects = false;
          // NOOP
          break;
      }
    }

    @Override
    public void processEvent(EventLifecycleContext ctx) {}

    @Override
    public boolean executeSideEffects() {
      int exporterIndex = 0;
      int exportersCount = containers.size();

      // current error handling strategy is simply to repeat forever until the record can be
      // successfully exported.
      while (exporterIndex < exportersCount) {
        final ExporterContainer container = containers.get(exporterIndex);

        try {
          if (container.startPosition < record.getMetadata().getPosition()) {
            container.exporter.export(record);
          }

          exporterIndex++;
        } catch (final Exception ex) {
          container.context.getLogger().error("Error exporting record {}", record, ex);
        }
      }

      return true;
    }

    @Override
    public long writeEvent(LogStreamRecordWriter writer) {
      if (shouldCommitPositions()) {
        final ExporterRecord record = state.newExporterRecord();

        metadata
            .reset()
            .recordType(RecordType.EVENT)
            .valueType(ValueType.EXPORTER)
            .intent(ExporterIntent.POSITIONS_COMMITTED);

        return writer.positionAsKey().valueWriter(record).metadataWriter(metadata).tryWrite();
      }

      return 0;
    }
  }
}

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
package io.zeebe.broker.subscription.message.processor;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.subscription.message.data.MessageSubscriptionRecord;
import io.zeebe.broker.subscription.message.state.MessageDataStore;
import io.zeebe.broker.subscription.message.state.MessageDataStore.Message;
import io.zeebe.broker.subscription.message.state.MessageSubscriptionDataStore;
import io.zeebe.logstreams.processor.EventLifecycleContext;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.intent.MessageSubscriptionIntent;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class OpenMessageSubscriptionProcessor
    implements TypedRecordProcessor<MessageSubscriptionRecord> {

  private final MessageDataStore messageStore;
  private final MessageSubscriptionDataStore subscriptionStore;

  private final DirectBuffer messagePayload = new UnsafeBuffer(0, 0);

  private SubscriptionCommandSender commandSender;

  private MessageSubscriptionRecord subscriptionRecord;

  public OpenMessageSubscriptionProcessor(
      MessageDataStore messageStore, MessageSubscriptionDataStore subscriptionStore) {
    this.messageStore = messageStore;
    this.subscriptionStore = subscriptionStore;
  }

  @Override
  public void processRecord(
      TypedRecord<MessageSubscriptionRecord> record,
      TypedResponseWriter responseWriter,
      TypedStreamWriter streamWriter,
      Consumer<SideEffectProducer> sideEffect,
      EventLifecycleContext ctx) {

    subscriptionRecord = record.getValue();

    final boolean added = subscriptionStore.addSubscription(subscriptionRecord);
    if (!added) {
      sideEffect.accept(this::sendOpenedCommand);

      streamWriter.writeRejection(
          record, RejectionType.NOT_APPLICABLE, "subscription is already open");
      return;
    }

    // handle new subscription
    final Message message =
        messageStore.findMessage(
            bufferAsString(subscriptionRecord.getMessageName()),
            bufferAsString(subscriptionRecord.getCorrelationKey()));

    if (message != null) {
      messagePayload.wrap(message.getPayload());

      sideEffect.accept(this::sendCorrelateCommand);
    } else {
      sideEffect.accept(this::sendOpenedCommand);
    }

    streamWriter.writeFollowUpEvent(
        record.getKey(), MessageSubscriptionIntent.OPENED, subscriptionRecord);
  }

  private boolean sendCorrelateCommand() {
    return commandSender.correlateWorkflowInstanceSubscription(
        subscriptionRecord.getWorkflowInstancePartitionId(),
        subscriptionRecord.getWorkflowInstanceKey(),
        subscriptionRecord.getActivityInstanceKey(),
        subscriptionRecord.getMessageName(),
        messagePayload);
  }

  private boolean sendOpenedCommand() {
    return commandSender.openedMessageSubscription(
        subscriptionRecord.getWorkflowInstancePartitionId(),
        subscriptionRecord.getWorkflowInstanceKey(),
        subscriptionRecord.getActivityInstanceKey(),
        subscriptionRecord.getMessageName());
  }

  public void setSubscriptionCommandSender(SubscriptionCommandSender sender) {
    this.commandSender = sender;
  }
}

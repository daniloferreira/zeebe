/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.exporter.spi;

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;

/**
 * Minimal interface to be implemented by concrete exporters.
 *
 * <p>A concrete implementation should always provide a default, no-arguments constructor. It is not
 * recommended to do anything in the constructor, but rather use the provided callbacks for
 * configuration, setup, resource allocation, etc.
 */
public interface Exporter {
  /**
   * Use the provided configuration at this point to configure your exporter. This method is called
   * right before opening the exporter.
   *
   * @param context the exporter context
   */
  default void configure(final Context context) {}

  /**
   * Hook to perform any validation when the exporter is loaded. This method is called exactly once
   * at startup when the exported is loaded, allowing an exporter to fail early if mis-configured.
   * It's not recommended to perform complex or slow validation here, as it will block the startup
   * of the broker.
   *
   * <p>If an exception is thrown, or the method returns false, the broker will not start.
   *
   * @param context exporter context
   * @return true if valid, false otherwise
   * @throws Exception any validation exception
   */
  default boolean validate(final Context context) throws Exception {
    return true;
  }

  /**
   * Hook to perform any setup for a given exporter. This method is the first method called during
   * the lifecycle of an exporter, and should be use to create, allocate or configure resources.
   * After this is called, records will be published to this exporter.
   *
   * @param controller execution controller for this exporter
   */
  default void open(final Controller controller) {}

  /**
   * Hook to perform any tear down. This is method is called exactly once at the end of the
   * lifecycle of an exporter, and should be used to close and free any remaining resources.
   */
  default void close() {}

  /**
   * Called at least once for every record to be exporter. Once a record is guaranteed to have been
   * exported, implementations should call {@link Controller#updateLastExportedRecordPosition(long)}
   * to signal that this record should not be received here ever again.
   *
   * <p>Should the export method throw an unexpected {@link RuntimeException}, the method will be
   * called indefinitely until it terminates without any exception. It is up to the implementation
   * to handle errors properly, to implement retry strategies, etc.
   *
   * @param record the record to export
   */
  void export(final Record record);
}

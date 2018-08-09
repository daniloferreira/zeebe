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
package io.zeebe.exporter.record;

import java.util.Map;

/** Shared behaviour for all record values containing a payload document. */
public interface RecordValueWithPayload extends RecordValue {
  /** @return JSON-formatted payload */
  String getPayload();

  /** @return de-serialized payload as map */
  Map<String, Object> getPayloadAsMap();

  /** @return de-serialized payload as the given type */
  <T> T getPayloadAsType(Class<T> payloadType);
}

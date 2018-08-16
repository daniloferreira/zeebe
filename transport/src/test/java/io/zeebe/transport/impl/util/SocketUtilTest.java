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
package io.zeebe.transport.impl.util;

import static io.zeebe.transport.impl.util.SocketUtil.BASE_PORT;
import static io.zeebe.transport.impl.util.SocketUtil.RANGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.transport.SocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class SocketUtilTest {

  @Test
  public void shouldCyclicIterate() {
    final List<Integer> ports =
        IntStream.range(BASE_PORT, BASE_PORT + RANGE_SIZE).boxed().collect(Collectors.toList());

    for (int i = 0; i < 3; i++) {
      for (Integer port : ports) {
        final SocketAddress address = SocketUtil.getNextAddress();
        assertThat(address).isEqualTo(new SocketAddress(SocketUtil.DEFAULT_HOST, port));
      }
    }
  }
}

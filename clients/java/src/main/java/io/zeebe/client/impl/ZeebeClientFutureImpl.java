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

package io.zeebe.client.impl;

import io.grpc.stub.StreamObserver;
import io.zeebe.client.api.ZeebeFuture;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class ZeebeClientFutureImpl<T, R> extends CompletableFuture<T>
    implements ZeebeFuture<T>, StreamObserver<R> {

  private final Function<R, T> responseMapper;

  ZeebeClientFutureImpl(final Function<R, T> responseMapper) {
    this.responseMapper = responseMapper;
  }

  @Override
  public T join() {
    try {
      return get();
    } catch (final InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T join(final long timeout, final TimeUnit unit) {
    try {
      return get(timeout, unit);
    } catch (final InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onNext(final R r) {
    try {
      complete(responseMapper.apply(r));
    } catch (final Exception e) {
      completeExceptionally(e);
    }
  }

  @Override
  public void onError(final Throwable throwable) {
    completeExceptionally(throwable);
  }

  @Override
  public void onCompleted() {
    // do nothing as we don't support streaming
  }
}

/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.r2dbc.pool;

import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;

/**
 * Operator utility.
 *
 * @since 0.8.8
 */
final class Operators {

    private Operators() {
    }

    /**
     * Replay signals from {@link Mono the source} until cancellation. Drains the source for data signals if the subscriber cancels the subscription.
     * <p>
     * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a request/response conversation without leaving
     * previous frames on the stack.
     *
     * @param source the source to decorate.
     * @param <T>    The type of values in both source and output sequences.
     * @return decorated {@link Mono}.
     */
    public static <T> Mono<T> discardOnCancel(Mono<? extends T> source) {
        return new MonoDiscardOnCancel<>(source, () -> {
        });
    }

    /**
     * Replay signals from {@link Mono the source} until cancellation. Drains the source for data signals if the subscriber cancels the subscription.
     * <p>
     * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a request/response conversation without leaving
     * previous frames on the stack.
     * <p>Propagate the {@link Subscription#cancel()}  signal to a {@link Runnable consumer}.
     *
     * @param source         the source to decorate.
     * @param cancelConsumer {@link Runnable} notified when the resulting {@link Mono} receives a {@link Subscription#cancel() cancel} signal.
     * @param <T>            The type of values in both source and output sequences.
     * @return decorated {@link Mono}.
     */
    public static <T> Mono<T> discardOnCancel(Mono<? extends T> source, Runnable cancelConsumer) {
        return new MonoDiscardOnCancel<>(source, cancelConsumer);
    }

}

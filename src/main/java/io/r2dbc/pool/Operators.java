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

import java.util.function.Consumer;

/**
 * Operator utility.
 *
 * @since 0.8.8
 */
final class Operators {

    private Operators() {
    }

    /**
     * Replay signals from {@link Mono the source} until cancellation, and decide on {@link Subscription#cancel() cancel}
     * who owns the value.
     * <p>
     * A cancel before the value cancels the source, runs {@code onCancel} and drops a late value. A cancel from inside
     * the delivery, on the delivering thread, leaves the value with the subscriber. Any other cancel after the value was
     * delivered passes the value to {@code onAbandoned}. Each callback runs at most once per subscription, and never
     * both; an exception from a callback is dropped through {@code Operators.onErrorDropped}.
     *
     * @param source      the source to decorate.
     * @param onCancel    called on a cancel before the value
     * @param onAbandoned called with the value on a cancel after the subscriber has given it up
     * @param <T>         The type of values in both source and output sequences.
     * @return decorated {@link Mono}.
     */
    public static <T> Mono<T> discardOnCancel(Mono<? extends T> source, Runnable onCancel, Consumer<? super T> onAbandoned) {
        return new MonoDiscardOnCancel<>(source, onCancel, onAbandoned);
    }

}

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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.publisher.Operators;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

/**
 * A decorating operator that replays signals from its source to a {@link Subscriber} and decides, on
 * {@link Subscription#cancel() cancel}, who owns the value:
 * <ul>
 * <li>A cancel before the value: nobody has it. The source is cancelled, {@code onCancel} runs, and a value the source
 * emits anyway is dropped: {@code onCancel} has taken care of it.</li>
 * <li>A cancel from inside {@code onNext}, on the thread that delivers the value: the subscriber has taken the value and
 * asked for no more, as {@code Flux.next()} does. The value is the subscriber's.</li>
 * <li>Any other cancel after the value was delivered, or during the delivery from another thread: the subscriber gave
 * the value up. {@code onAbandoned} runs with it.</li>
 * </ul>
 * Each callback runs at most once per subscription, and never both.
 */
class MonoDiscardOnCancel<T> extends MonoOperator<T, T> {

    private static final Logger logger = Loggers.getLogger(MonoDiscardOnCancel.class);

    private final Runnable onCancel;

    private final Consumer<? super T> onAbandoned;

    MonoDiscardOnCancel(Mono<? extends T> source, Runnable onCancel, Consumer<? super T> onAbandoned) {
        super(source);
        this.onCancel = onCancel;
        this.onAbandoned = onAbandoned;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.source.subscribe(new MonoDiscardOnCancelSubscriber<>(actual, this.onCancel, this.onAbandoned));
    }

    static class MonoDiscardOnCancelSubscriber<T> implements CoreSubscriber<T>, Subscription {

        /**
         * No value yet.
         */
        static final int ACTIVE = 0;

        /**
         * {@code actual.onNext} is running.
         */
        static final int EMITTING = 1;

        /**
         * The delivery has returned.
         */
        static final int EMITTED = 2;

        /**
         * The subscriber cancelled from inside the delivery and keeps the value.
         */
        static final int KEPT = 3;

        /**
         * Cancelled before the value.
         */
        static final int CANCELLED = 4;

        /**
         * Cancelled after the delivery; the value was handed to {@code onAbandoned}.
         */
        static final int RECLAIMED = 5;

        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<MonoDiscardOnCancelSubscriber> STATE = AtomicIntegerFieldUpdater.newUpdater(MonoDiscardOnCancelSubscriber.class, "state");

        final CoreSubscriber<? super T> actual;

        final Context ctx;

        final Runnable onCancel;

        final Consumer<? super T> onAbandoned;

        Subscription s;

        volatile int state;

        // Written before the transition to EMITTING, read after it.
        @Nullable
        T value;

        @Nullable
        Thread deliveringThread;

        MonoDiscardOnCancelSubscriber(CoreSubscriber<? super T> actual, Runnable onCancel, Consumer<? super T> onAbandoned) {

            this.actual = actual;
            this.ctx = actual.currentContext();
            this.onCancel = onCancel;
            this.onAbandoned = onAbandoned;
        }

        @Override
        public Context currentContext() {
            return this.ctx;
        }

        @Override
        public void onSubscribe(Subscription s) {

            if (Operators.validate(this.s, s)) {
                this.s = s;
                this.actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {

            if (this.state == CANCELLED) {
                return;
            }

            this.value = t;
            this.deliveringThread = Thread.currentThread();

            if (!STATE.compareAndSet(this, ACTIVE, EMITTING)) {
                // A cancel won the race; onCancel has taken care of the value.
                return;
            }

            this.actual.onNext(t);
            STATE.compareAndSet(this, EMITTING, EMITTED);
        }

        @Override
        public void onError(Throwable t) {
            if (this.state != CANCELLED) {
                this.actual.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (this.state != CANCELLED) {
                this.actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            this.s.request(n);
        }

        @Override
        public void cancel() {

            for (; ; ) {

                int state = this.state;

                if (state == ACTIVE) {
                    if (STATE.compareAndSet(this, ACTIVE, CANCELLED)) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("received cancel signal before the value");
                        }
                        this.s.cancel();
                        run(this.onCancel);
                        return;
                    }
                } else if (state == EMITTING) {
                    boolean fromDelivery = Thread.currentThread() == this.deliveringThread;
                    if (STATE.compareAndSet(this, EMITTING, fromDelivery ? KEPT : RECLAIMED)) {
                        if (!fromDelivery) {
                            abandon();
                        }
                        this.s.cancel();
                        return;
                    }
                } else if (state == EMITTED) {
                    if (STATE.compareAndSet(this, EMITTED, RECLAIMED)) {
                        abandon();
                        this.s.cancel();
                        return;
                    }
                } else {
                    return;
                }
            }
        }

        private void abandon() {

            if (logger.isTraceEnabled()) {
                logger.trace("received cancel signal after the value was delivered");
            }

            T value = this.value;
            run(() -> this.onAbandoned.accept(value));
        }

        private void run(Runnable callback) {
            try {
                callback.run();
            } catch (Exception e) {
                Operators.onErrorDropped(e, this.ctx);
            }
        }

    }

}

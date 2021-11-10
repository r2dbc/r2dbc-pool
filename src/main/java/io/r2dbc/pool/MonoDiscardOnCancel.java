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
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * A decorating operator that replays signals from its source to a {@link Subscriber} and drains the source upon {@link Subscription#cancel() cancel} and drops data signals until termination.
 * Draining data is required to complete a particular request/response window and clear the protocol state as client code expects to start a request/response conversation without any previous
 * response state.
 */
class MonoDiscardOnCancel<T> extends MonoOperator<T, T> {

    private static final Logger logger = Loggers.getLogger(MonoDiscardOnCancel.class);

    private final BooleanSupplier cancelConsumer;

    MonoDiscardOnCancel(Mono<? extends T> source, BooleanSupplier cancelConsumer) {
        super(source);
        this.cancelConsumer = cancelConsumer;
    }

    @Override
    public void subscribe(CoreSubscriber<? super T> actual) {
        this.source.subscribe(new MonoDiscardOnCancelSubscriber<>(actual, this.cancelConsumer));
    }

    static class MonoDiscardOnCancelSubscriber<T> extends AtomicBoolean implements CoreSubscriber<T>, Subscription {

        final CoreSubscriber<T> actual;

        final Context ctx;

        final BooleanSupplier cancelConsumer;

        Subscription s;

        MonoDiscardOnCancelSubscriber(CoreSubscriber<T> actual, BooleanSupplier cancelConsumer) {

            this.actual = actual;
            this.ctx = actual.currentContext();
            this.cancelConsumer = cancelConsumer;
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

            if (this.get()) {
                Operators.onDiscard(t, this.ctx);
                return;
            }

            this.actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            if (!this.get()) {
                this.actual.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (!this.get()) {
                this.actual.onComplete();
            }
        }

        @Override
        public void request(long n) {
            this.s.request(n);
        }

        @Override
        public void cancel() {

            if (compareAndSet(false, true)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("received cancel signal");
                }
                try {
                    boolean mayCancelUpstream = this.cancelConsumer.getAsBoolean();
                    if (mayCancelUpstream) {
                        this.s.cancel();
                        return;
                    }
                } catch (Exception e) {
                    Operators.onErrorDropped(e, this.ctx);
                }
                this.s.request(Long.MAX_VALUE);
            }
        }

    }

}

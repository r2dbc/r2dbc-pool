/*
 * Copyright 2026 the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.test.publisher.TestPublisher;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link MonoDiscardOnCancel}.
 */
class MonoDiscardOnCancelUnitTests {

    final AtomicInteger cancels = new AtomicInteger();

    final List<String> abandoned = new CopyOnWriteArrayList<>();

    final List<Object> discarded = new CopyOnWriteArrayList<>();

    final List<Throwable> droppedErrors = new CopyOnWriteArrayList<>();

    /**
     * What is to be tested: a value that is delivered without any cancel.
     * <p>
     * How will the test case be deemed successful and why: the subscriber receives the value and the completion, and
     * neither callback runs. Without a cancel the operator must be invisible.
     * <p>
     * Why is it important to test this test case: it is the path of every acquisition.
     */
    @Test
    void valueWithoutCancelShouldBeDelivered() {

        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber subscriber = subscribe(source);

        source.next("connection");
        source.complete();

        assertThat(subscriber.values).containsExactly("connection");
        assertThat(subscriber.completed).isTrue();
        assertThat(this.cancels).hasValue(0);
        assertThat(this.abandoned).isEmpty();
    }

    /**
     * What is to be tested: a cancel before the source emitted its value.
     * <p>
     * How will the test case be deemed successful and why: {@code onCancel} runs once, the source is cancelled, and a
     * value the source emits anyway reaches neither the subscriber, nor {@code onAbandoned}, nor a discard hook.
     * Nobody owns the value at that point, and {@code onCancel} has already taken care of it; any further reaction
     * would act on a connection that is being destroyed.
     * <p>
     * Why is it important to test this test case: this is a cancel during the preparation of a connection. The pool
     * destroys the connection instead of letting the preparation run on (#232).
     */
    @Test
    void cancelBeforeValueShouldRunOnCancel() {

        TestPublisher<String> source = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);
        RecordingSubscriber subscriber = subscribe(source);

        subscriber.cancel();
        source.next("connection");

        assertThat(this.cancels).hasValue(1);
        assertThat(source.wasCancelled()).as("source cancelled").isTrue();
        assertThat(subscriber.values).isEmpty();
        assertThat(this.abandoned).isEmpty();
        assertThat(this.discarded).isEmpty();
    }

    /**
     * What is to be tested: a subscriber that cancels from inside {@code onNext}, on the delivering thread, as
     * {@code Flux.next()} does.
     * <p>
     * How will the test case be deemed successful and why: the subscriber has the value, and no callback runs, also not
     * on a later cancel. A subscriber that cancels while it receives the value has asked for one element and has it.
     * <p>
     * Why is it important to test this test case: this is the subscriber of #211 whose connection the pool released
     * while it was in use.
     */
    @Test
    void cancelFromInsideDeliveryShouldKeepTheValue() {

        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber subscriber = subscribe(source);
        subscriber.onNextAction = value -> subscriber.cancel();

        source.next("connection");
        subscriber.cancel();

        assertThat(subscriber.values).containsExactly("connection");
        assertThat(this.cancels).hasValue(0);
        assertThat(this.abandoned).isEmpty();
    }

    /**
     * What is to be tested: a cancel from another thread while {@code onNext} is still running.
     * <p>
     * How will the test case be deemed successful and why: {@code onAbandoned} runs once with the value. The cancel
     * does not come from the subscriber's own handling of the value, so the subscriber has given it up.
     * <p>
     * Why is it important to test this test case: a timeout or a client that went away can cancel at any moment, also
     * during the delivery. Such a cancel must not be mistaken for a subscriber that keeps the value.
     */
    @Test
    void cancelDuringDeliveryFromAnotherThreadShouldAbandon() {

        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber subscriber = subscribe(source);
        subscriber.onNextAction = value -> {
            Thread other = new Thread(subscriber::cancel);
            other.start();
            try {
                other.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        };

        source.next("connection");

        assertThat(this.abandoned).containsExactly("connection");
        assertThat(this.cancels).hasValue(0);
    }

    /**
     * What is to be tested: a cancel after {@code onNext} has returned, before the completion.
     * <p>
     * How will the test case be deemed successful and why: {@code onAbandoned} runs once with that value, and the
     * source is cancelled. The subscriber gave the value up after it had received it.
     * <p>
     * Why is it important to test this test case: this is Spring's {@code doBegin} cancelled while the begin is in
     * flight: the connection was handed over, and the subscriber will never close it.
     */
    @Test
    void cancelAfterDeliveryShouldAbandon() {

        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber subscriber = subscribe(source);

        source.next("connection");
        subscriber.cancel();

        assertThat(this.abandoned).containsExactly("connection");
        assertThat(source.wasCancelled()).as("source cancelled").isTrue();
        assertThat(this.cancels).hasValue(0);
    }

    /**
     * What is to be tested: a cancel after the source has completed.
     * <p>
     * How will the test case be deemed successful and why: {@code onAbandoned} runs once with the value. Reactor's
     * {@code flatMap} cancels its completed source, so a cancel after the completion still means that the subscriber
     * gave the value up.
     * <p>
     * Why is it important to test this test case: Reactive Streams makes such a cancel a no-op, but the pool relies on
     * it to take back a connection that its subscriber abandoned.
     */
    @Test
    void cancelAfterCompleteShouldAbandon() {

        TestPublisher<String> source = TestPublisher.create();
        RecordingSubscriber subscriber = subscribe(source);

        source.next("connection");
        source.complete();
        subscriber.cancel();

        assertThat(subscriber.completed).isTrue();
        assertThat(this.abandoned).containsExactly("connection");
    }

    /**
     * What is to be tested: a second cancel, before and after the value.
     * <p>
     * How will the test case be deemed successful and why: each callback ran at most once. The first cancel decides
     * the owner; every later one has nothing left to decide.
     * <p>
     * Why is it important to test this test case: a connection released twice can be handed to two borrowers.
     */
    @Test
    void secondCancelShouldBeNoOp() {

        TestPublisher<String> before = TestPublisher.create();
        RecordingSubscriber cancelledBefore = subscribe(before);
        cancelledBefore.cancel();
        cancelledBefore.cancel();

        TestPublisher<String> after = TestPublisher.create();
        RecordingSubscriber cancelledAfter = subscribe(after);
        after.next("connection");
        cancelledAfter.cancel();
        cancelledAfter.cancel();

        assertThat(this.cancels).hasValue(1);
        assertThat(this.abandoned).containsExactly("connection");
    }

    /**
     * What is to be tested: an error of the source after a cancel before the value.
     * <p>
     * How will the test case be deemed successful and why: the error does not reach the subscriber, and
     * {@code onAbandoned} does not run. The subscriber is gone, and there is no value to give back.
     * <p>
     * Why is it important to test this test case: a preparation that fails after the cancel is cleaned up by the pool
     * itself; the operator must not add a second reaction.
     */
    @Test
    void cancelThenErrorShouldBeDropped() {

        TestPublisher<String> source = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);
        RecordingSubscriber subscriber = subscribe(source);

        subscriber.cancel();
        source.error(new IllegalStateException("preparation failed"));

        assertThat(subscriber.error).isNull();
        assertThat(this.abandoned).isEmpty();
        assertThat(this.cancels).hasValue(1);
    }

    /**
     * What is to be tested: callbacks that throw.
     * <p>
     * How will the test case be deemed successful and why: both exceptions go to the {@code onErrorDropped} hook of the
     * subscriber's context, and {@code cancel()} returns normally. A cancel has no channel for an error.
     * <p>
     * Why is it important to test this test case: an exception thrown out of {@code cancel()} would break the operator
     * that cancels, and the failure would be lost.
     */
    @Test
    void callbackExceptionShouldBeDropped() {

        TestPublisher<String> before = TestPublisher.create();
        RecordingSubscriber cancelledBefore = new RecordingSubscriber();
        Mono.fromDirect(before).as(mono -> Operators.discardOnCancel(mono, () -> {
            throw new IllegalStateException("onCancel failed");
        }, value -> {
        })).subscribe(cancelledBefore);
        cancelledBefore.cancel();

        TestPublisher<String> after = TestPublisher.create();
        RecordingSubscriber cancelledAfter = new RecordingSubscriber();
        Mono.fromDirect(after).as(mono -> Operators.discardOnCancel(mono, () -> {
        }, value -> {
            throw new IllegalStateException("onAbandoned failed");
        })).subscribe(cancelledAfter);
        after.next("connection");
        cancelledAfter.cancel();

        assertThat(this.droppedErrors).extracting(Throwable::getMessage).containsExactly("onCancel failed", "onAbandoned failed");
    }

    /**
     * What is to be tested: the value and a cancel arriving at the same time, from two threads, 10 000 times.
     * <p>
     * How will the test case be deemed successful and why: in every round either {@code onCancel} ran and the value
     * reached nobody, or the value reached the subscriber and {@code onAbandoned} ran exactly once with it; never both
     * callbacks. The cancel always comes from the other thread and always runs, so a delivered value must always be
     * abandoned. Whatever the order, the value has exactly one owner.
     * <p>
     * Why is it important to test this test case: a cancel by a timeout races with the delivery of the connection. A
     * lost value leaks a slot; a value with two owners is a connection shared by two borrowers.
     */
    @Test
    void valueAndCancelShouldRace() {

        for (int i = 0; i < 10_000; i++) {

            this.cancels.set(0);
            this.abandoned.clear();

            TestPublisher<String> source = TestPublisher.createNoncompliant(TestPublisher.Violation.DEFER_CANCELLATION);
            RecordingSubscriber subscriber = subscribe(source);

            RaceTestUtils.race(() -> source.next("connection"), subscriber::cancel);

            boolean cancelledBefore = this.cancels.get() == 1 && subscriber.values.isEmpty() && this.abandoned.isEmpty();
            boolean delivered = this.cancels.get() == 0 && subscriber.values.size() == 1 && this.abandoned.equals(Collections.singletonList("connection"));

            assertThat(cancelledBefore || delivered).as("round %d: cancels=%s, values=%s, abandoned=%s", i, this.cancels, subscriber.values, this.abandoned)
                .isTrue();
        }
    }

    private RecordingSubscriber subscribe(TestPublisher<String> source) {

        RecordingSubscriber subscriber = new RecordingSubscriber();
        Operators.discardOnCancel(Mono.fromDirect(source), this.cancels::incrementAndGet, this.abandoned::add).subscribe(subscriber);
        return subscriber;
    }

    final class RecordingSubscriber implements CoreSubscriber<String> {

        final List<String> values = new CopyOnWriteArrayList<>();

        volatile boolean completed;

        volatile Throwable error;

        volatile Consumer<String> onNextAction = value -> {
        };

        volatile Subscription subscription;

        @Override
        public Context currentContext() {
            return Context.of("reactor.onDiscard.local", (Consumer<Object>) MonoDiscardOnCancelUnitTests.this.discarded::add,
                "reactor.onErrorDropped.local", (Consumer<Throwable>) MonoDiscardOnCancelUnitTests.this.droppedErrors::add);
        }

        @Override
        public void onSubscribe(Subscription s) {
            this.subscription = s;
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(String value) {
            this.values.add(value);
            this.onNextAction.accept(value);
        }

        @Override
        public void onError(Throwable t) {
            this.error = t;
        }

        @Override
        public void onComplete() {
            this.completed = true;
        }

        void cancel() {
            this.subscription.cancel();
        }
    }
}

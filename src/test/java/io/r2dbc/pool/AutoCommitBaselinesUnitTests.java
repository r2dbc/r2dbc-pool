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

import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AutoCommitBaselines}.
 */
class AutoCommitBaselinesUnitTests {

    final AutoCommitBaselines baselines = new AutoCommitBaselines();

    /**
     * What is to be tested: a record, its lookup and its removal.
     * <p>
     * How will the test case be deemed successful and why: the recorded mode is returned until the record is
     * forgotten, then {@code null}. {@code null} means "unknown" to the release, which then restores nothing.
     * <p>
     * Why is it important to test this test case: it is the life cycle of every record, from the creation of a
     * connection to its destroy.
     */
    @Test
    void shouldRecordAndForget() {

        Connection connection = connection();

        this.baselines.record(connection, false);

        assertThat(this.baselines.get(connection)).isFalse();
        assertThat(this.baselines.size()).isOne();

        this.baselines.forget(connection);

        assertThat(this.baselines.get(connection)).isNull();
        assertThat(this.baselines.size()).isZero();
    }

    /**
     * What is to be tested: two driver connections that are equal by {@code equals} and share a hash code.
     * <p>
     * How will the test case be deemed successful and why: each keeps its own record, and forgetting one leaves the
     * other. The records are keyed by identity.
     * <p>
     * Why is it important to test this test case: a driver connection may override {@code equals}/{@code hashCode},
     * for example by the session it belongs to. Records keyed by equality would mix up two connections.
     */
    @Test
    void shouldKeyByIdentity() {

        Connection first = connection();
        Connection second = connection();
        assertThat(first).isEqualTo(second);

        this.baselines.record(first, true);
        this.baselines.record(second, false);

        assertThat(this.baselines.get(first)).isTrue();
        assertThat(this.baselines.get(second)).isFalse();

        this.baselines.forget(first);

        assertThat(this.baselines.get(first)).isNull();
        assertThat(this.baselines.get(second)).isFalse();
    }

    /**
     * A connection that is equal to every other one created here and has the same hash code.
     */
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(AutoCommitBaselinesUnitTests.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {

            if (method.getName().equals("equals")) {
                return args[0] instanceof Connection;
            }

            if (method.getName().equals("hashCode")) {
                return 42;
            }

            if (method.getName().equals("toString")) {
                return "Connection";
            }

            throw new UnsupportedOperationException(method.getName());
        });
    }
}

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
import reactor.util.annotation.Nullable;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The auto-commit mode of each connection when the {@link io.r2dbc.spi.ConnectionFactory} delivered it.
 * <p>
 * Keyed by identity, because a driver connection may override {@code equals}/{@code hashCode}, and weakly, so that a
 * record does not keep its connection reachable. The pool's destroy handler forgets a record at once; the weak keys
 * cover a destroy handler that a customizer replaced.
 */
final class AutoCommitBaselines {

    private final Map<Key, Boolean> baselines = new ConcurrentHashMap<>();

    private final ReferenceQueue<Connection> collected = new ReferenceQueue<>();

    void record(Connection connection, boolean autoCommit) {
        expungeCollected();
        this.baselines.put(new Key(connection, this.collected), autoCommit);
    }

    @Nullable
    Boolean get(Connection connection) {
        return this.baselines.get(new Key(connection, null));
    }

    void forget(Connection connection) {
        this.baselines.remove(new Key(connection, null));
        expungeCollected();
    }

    /**
     * The number of records of connections that are still reachable, for tests.
     */
    int size() {
        expungeCollected();
        return this.baselines.size();
    }

    private void expungeCollected() {

        Reference<? extends Connection> reference;
        while ((reference = this.collected.poll()) != null) {
            this.baselines.remove(reference);
        }
    }

    /**
     * A weak key that compares its referents by identity. A key whose referent was collected equals only itself, so
     * that {@link #expungeCollected()} can remove it.
     */
    static final class Key extends WeakReference<Connection> {

        private final int hash;

        Key(Connection connection, @Nullable ReferenceQueue<Connection> queue) {
            super(connection, queue);
            this.hash = System.identityHashCode(connection);
        }

        @Override
        public boolean equals(Object other) {

            if (other == this) {
                return true;
            }

            if (!(other instanceof Key)) {
                return false;
            }

            Connection connection = get();
            return connection != null && connection == ((Key) other).get();
        }

        @Override
        public int hashCode() {
            return this.hash;
        }
    }
}

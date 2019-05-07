/*
 * Copyright 2019 the original author or authors.
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

import reactor.pool.Pool;

/**
 * An object that can be used to get live information about a {@link ConnectionPool}, suitable
 * for gauge metrics.
 * <p>
 * {@code getXxx} methods are configuration accessors, their values that won't change over time, whereas other methods can be used as gauges to introspect the current state of the pool.
 *
 * @author Mark Paluch
 */
public interface PoolMetrics {

    /**
     * Measure the current number of connections that have been successfully
     * {@link ConnectionPool#create()} acquired} and are in active use.
     *
     * @return the number of acquired connections
     */
    int acquiredSize();

    /**
     * Measure the current number of allocated connections in the {@link ConnectionPool}, acquired
     * or idle.
     *
     * @return the total number of allocated connections managed by the {@link Pool}
     */
    int allocatedSize();

    /**
     * Measure the current number of idle connections in the {@link ConnectionPool}.
     * <p>
     * Note that some connections might be lazily evicted when they're next considered
     * for an incoming {@link ConnectionPool#create()} call. Such connections would still count
     * towards this method.
     *
     * @return the number of idle connections
     */
    int idleSize();

    /**
     * Measure the current number of "pending" {@link ConnectionPool#create()} acquire Monos in
     * the {@link ConnectionPool}.
     * <p>
     * An acquire is in the pending state when it is attempted at a point when no idle
     * connections is available in the pool, and no new connection can be created.
     *
     * @return the number of pending acquire
     */
    int pendingAcquireSize();

    /**
     * Get the maximum number of live connections this {@link ConnectionPool} will allow.
     * <p>
     * A {@link ConnectionPool} might be unbounded, in which case this method returns {@link Integer#MAX_VALUE}.
     *
     * @return the maximum number of live connections that can be allocated by the {@link ConnectionPool}.
     */
    int getMaxAllocatedSize();

    /**
     * Get the maximum number of {@link ConnectionPool#create()} this {@link ConnectionPool} can queue in
     * a pending state when no available connection is immediately handy (and the {@link ConnectionPool}
     * cannot allocate more connections).
     * <p>
     * A {@link ConnectionPool} pending queue might be unbounded, in which case this method returns
     * {@link Integer#MAX_VALUE}.
     *
     * @return the maximum number of pending acquire that can be enqueued by the {@link ConnectionPool}.
     */
    int getMaxPendingAcquireSize();
}

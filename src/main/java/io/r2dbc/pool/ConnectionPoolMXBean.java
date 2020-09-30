package io.r2dbc.pool;

import reactor.pool.Pool;

/**
 * MBean for {@link ConnectionPool}.
 * <p>
 * Expose {@link PoolMetrics} values to JMX.
 *
 * @author Tadaya Tsuyukubo
 * @see PoolMetrics
 */
public interface ConnectionPoolMXBean {

    String DOMAIN = "io.r2dbc.pool";

    /**
     * Measure the current number of connections that have been successfully
     * {@link ConnectionPool#create()} acquired and are in active use.
     *
     * @return the number of acquired connections
     * @see PoolMetrics#acquiredSize()
     */
    int getAcquiredSize();

    /**
     * Measure the current number of allocated connections in the {@link ConnectionPool}, acquired
     * or idle.
     *
     * @return the total number of allocated connections managed by the {@link Pool}
     * @see PoolMetrics#allocatedSize()
     */
    int getAllocatedSize();

    /**
     * Measure the current number of idle connections in the {@link ConnectionPool}.
     * <p>
     * Note that some connections might be lazily evicted when they're next considered
     * for an incoming {@link ConnectionPool#create()} call. Such connections would still count
     * towards this method.
     *
     * @return the number of idle connections
     * @see PoolMetrics#idleSize()
     */
    int getIdleSize();

    /**
     * Measure the current number of "pending" {@link ConnectionPool#create()} acquire Monos in
     * the {@link ConnectionPool}.
     * <p>
     * An acquire is in the pending state when it is attempted at a point when no idle
     * connections is available in the pool, and no new connection can be created.
     *
     * @return the number of pending acquire
     * @see PoolMetrics#pendingAcquireSize()
     */
    int getPendingAcquireSize();

    /**
     * Get the maximum number of live connections this {@link ConnectionPool} will allow.
     * <p>
     * A {@link ConnectionPool} might be unbounded, in which case this method returns {@link Integer#MAX_VALUE}.
     *
     * @return the maximum number of live connections that can be allocated by the {@link ConnectionPool}.
     * @see PoolMetrics#getMaxAllocatedSize()
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
     * @see PoolMetrics#getMaxPendingAcquireSize()
     */
    int getMaxPendingAcquireSize();

}

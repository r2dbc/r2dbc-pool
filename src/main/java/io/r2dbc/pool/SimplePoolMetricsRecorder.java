package io.r2dbc.pool;

import reactor.pool.PoolMetricsRecorder;

import java.time.Clock;

/**
 * Simple {@link PoolMetricsRecorder} which supports time related feature using {@link Clock}.
 *
 * <p>All "record**" methods are not supported.
 *
 * @author Tadaya Tsuyukubo
 * @author Mark Paluch
 */
public class SimplePoolMetricsRecorder implements PoolMetricsRecorder {

    private final Clock clock;

    /**
     * Use {@link Clock} with UTC time-zone.
     */
    public SimplePoolMetricsRecorder() {
        this(Clock.systemUTC());
    }

    /**
     * Construct with given {@link Clock} instance.
     *
     * @param clock clock to use
     * @throws IllegalArgumentException if {@code clock} is {@code null}.
     */
    public SimplePoolMetricsRecorder(Clock clock) {
        this.clock = Assert.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public long now() {
        return this.clock.millis();
    }

    @Override
    public long measureTime(long startTimeMillis) {
        return now() - startTimeMillis;
    }

    @Override
    public void recordAllocationSuccessAndLatency(long latencyMs) {
        // no-op
    }

    @Override
    public void recordAllocationFailureAndLatency(long latencyMs) {
        // no-op
    }

    @Override
    public void recordResetLatency(long latencyMs) {
        // no-op
    }

    @Override
    public void recordDestroyLatency(long latencyMs) {
        // no-op
    }

    @Override
    public void recordRecycled() {
        // no-op
    }

    @Override
    public void recordLifetimeDuration(long millisecondsSinceAllocation) {
        // no-op
    }

    @Override
    public void recordIdleTime(long millisecondsIdle) {
        // no-op
    }

    @Override
    public void recordSlowPath() {
        // no-op
    }

    @Override
    public void recordFastPath() {
        // no-op
    }

}

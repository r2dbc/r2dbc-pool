package io.r2dbc.pool;

import reactor.pool.PoolMetricsRecorder;

/**
 * Simple {@link PoolMetricsRecorder}.
 *
 * <p>All "record**" methods are not supported.
 *
 * @author Tadaya Tsuyukubo
 * @author Mark Paluch
 */
public class SimplePoolMetricsRecorder implements PoolMetricsRecorder {

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

package io.r2dbc.pool;

import io.r2dbc.spi.R2dbcException;

/**
 * Generic R2DBC Pool exception.
 *
 * @author Tadaya Tsuyukubo
 * @author Mark Paluch
 */
public class ConnectionPoolException extends R2dbcException {

    /**
     * Creates a new {@link ConnectionPoolException}.
     */
    public ConnectionPoolException() {
    }

    /**
     * Creates a new {@link ConnectionPoolException}.
     *
     * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     */
    public ConnectionPoolException(String reason) {
        super(reason);
    }

    /**
     * Creates a new {@link ConnectionPoolException}.
     *
     * @param reason the reason for the error. Set as the exception's message and retrieved with {@link #getMessage()}.
     * @param cause  the cause
     */
    public ConnectionPoolException(String reason, Throwable cause) {
        super(reason, cause);
    }

    /**
     * Creates a new {@link ConnectionPoolException}.
     *
     * @param cause the cause
     */
    public ConnectionPoolException(Throwable cause) {
        super(cause);
    }

}

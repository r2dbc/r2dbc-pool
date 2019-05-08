package io.r2dbc.pool;

/**
 * Generic R2DBC Pool exception.
 *
 * @author Tadaya Tsuyukubo
 */
public class ConnectionPoolException extends RuntimeException {

    public ConnectionPoolException() {
    }

    public ConnectionPoolException(String message) {
        super(message);
    }

    public ConnectionPoolException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionPoolException(Throwable cause) {
        super(cause);
    }

}

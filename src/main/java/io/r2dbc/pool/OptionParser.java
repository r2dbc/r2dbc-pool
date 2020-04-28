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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.time.Duration;
import java.util.Locale;

/**
 * An utility data parser for {@link Option}.
 *
 * @author Rodolpho S. Couto
 */
final class OptionParser {

    private OptionParser() {
    }

    /**
     * Parse an {@link Option} to int.
     */
    static int parseInt(ConnectionFactoryOptions options, Option<?> option) {
        final Object value = options.getRequiredValue(option);

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            try {
                return Integer.parseInt(value.toString());
            } catch (Exception ex) {
                throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value), ex);
            }
        }

        throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value));
    }

    /**
     * Parse an {@link Option} to {@link Enum}.
     */
    @SuppressWarnings("unchecked")
    static <T extends Enum<T>> T parseEnum(ConnectionFactoryOptions options, Option<?> option, Class<T> enumType) {
        final Object value = options.getRequiredValue(option);

        if (enumType.isInstance(value)) {
            return ((T) value);
        }

        if (value instanceof String) {
            try {
                return T.valueOf(enumType, value.toString().toUpperCase(Locale.ENGLISH));
            } catch (Exception ex) {
                throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value), ex);
            }
        }

        throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value));
    }

    /**
     * Parse an ISO-8601 formatted {@link Option} to {@link Duration}.
     */
    static Duration parseDuration(ConnectionFactoryOptions options, Option<?> option) {
        final Object value = options.getRequiredValue(option);

        if (value instanceof Duration) {
            return ((Duration) value);
        }

        if (value instanceof String) {
            try {
                return Duration.parse(value.toString());
            } catch (Exception ex) {
                throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value), ex);
            }
        }

        throw new IllegalArgumentException(String.format("Invalid %s option: %s", option.name(), value));
    }
}

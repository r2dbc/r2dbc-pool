/*
 * Copyright 2020-2021 the original author or authors.
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An utility data parser for {@link Option}.
 *
 * @author Rodolpho S. Couto
 * @author Mark Paluch
 * @since 0.9
 */
final class OptionMapper {

    private final ConnectionFactoryOptions options;

    private OptionMapper(ConnectionFactoryOptions options) {
        this.options = options;
    }

    /**
     * Construct a new {@link OptionMapper} given {@link ConnectionFactoryOptions}.
     *
     * @param options must not be {@code null}.
     * @return the option mapper.
     */
    public static OptionMapper create(ConnectionFactoryOptions options) {
        return new OptionMapper(options);
    }

    /**
     * Construct a new {@link Source} for a {@link Option}. Options without a value are not bound or mapped in the later stages of {@link Source}.
     *
     * @param option
     * @param <T>
     * @return the source object.
     */
    public <T> Source<T> from(Option<T> option) {

        if (this.options.hasOption(option)) {

            return new AvailableSource<T>(() -> {
                return this.options.getRequiredValue(option);
            }, option.name());
        }

        return NullSource.instance();
    }

    /**
     * Parse an {@link Option} to int.
     */
    static int toInteger(Object value) {

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            return Integer.parseInt(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s into integer", value));
    }

    /**
     * Parse an {@link Option} to {@link Enum}.
     */
    @SuppressWarnings("unchecked")
    static <T extends Enum<T>> T toEnum(Object value, Class<T> enumType) {

        if (enumType.isInstance(value)) {
            return ((T) value);
        }

        if (value instanceof String) {
            return T.valueOf(enumType, value.toString().toUpperCase(Locale.ENGLISH));
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s into %s", value, enumType.getName()));
    }

    /**
     * Parse an {@link Option} to {@link boolean}.
     */
    static boolean toBoolean(Object value) {

        if (value instanceof Boolean) {
            return ((Boolean) value);
        }

        if (value instanceof String) {
            return Boolean.parseBoolean(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s into Boolean", value));
    }

    /**
     * Parse an ISO-8601 formatted {@link Option} to {@link Duration}.
     */
    static Duration toDuration(Object value) {

        if (value instanceof Duration) {
            return ((Duration) value);
        }

        if (value instanceof String) {
            return Duration.parse(value.toString());
        }

        throw new IllegalArgumentException(String.format("Cannot convert value %s into Duration", value));
    }

    public interface Source<T> {

        /**
         * Return an mapped version of the source changed via the given mapping function.
         *
         * @param <R>             the resulting type
         * @param mappingFunction the mapping function to apply
         * @return a new adapted source instance
         */
        <R> Source<R> as(Function<Object, R> mappingFunction);

        /**
         * Complete the mapping by passing any non-filtered value to the specified
         * consumer.
         *
         * @param consumer the consumer that should accept the value if it's not been
         *                 filtered
         */
        void to(Consumer<T> consumer);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private enum NullSource implements Source<Object> {

        INSTANCE;

        public static <T> Source<T> instance() {
            return (Source) INSTANCE;
        }

        @Override
        public <R> Source<R> as(Function<Object, R> mappingFunction) {
            return (Source) this;
        }

        @Override
        public void to(Consumer<Object> consumer) {

        }
    }

    private static class AvailableSource<T> implements Source<T> {

        private final Supplier<T> supplier;

        private final String optionName;

        private AvailableSource(Supplier<T> supplier, String optionName) {
            this.supplier = supplier;
            this.optionName = optionName;
        }

        /**
         * Return an mapped version of the source changed via the given mapping function.
         *
         * @param <R>             the resulting type.
         * @param mappingFunction the mapping function to apply.
         * @return a new mapped source instance.
         */
        @Override
        public <R> Source<R> as(Function<Object, R> mappingFunction) {
            Assert.requireNonNull(mappingFunction, "Mapping function must not be null");

            Supplier<R> supplier = () -> mappingFunction.apply(this.supplier.get());

            return new AvailableSource<>(supplier, this.optionName);
        }

        /**
         * Complete the mapping by passing any non-filtered value to the specified
         * consumer.
         *
         * @param consumer the consumer that should accept the value.
         */
        @Override
        public void to(Consumer<T> consumer) {
            Assert.requireNonNull(consumer, "Consumer must not be null");
            try {
                T value = this.supplier.get();

                if (value != null) {
                    consumer.accept(value);
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(String.format("Cannot assign option %s", this.optionName), e);
            }
        }
    }
}

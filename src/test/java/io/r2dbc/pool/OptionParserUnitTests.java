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
import io.r2dbc.spi.ValidationDepth;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

/**
 * Unit tests for {@link OptionParser}.
 *
 * @author Rodolpho S. Couto
 */
final class OptionParserUnitTests {

    private static <T> ConnectionFactoryOptions options(Option<T> option, T value) {
        return ConnectionFactoryOptions.builder().option(option, value).build();
    }

    @TestFactory
    public Stream<DynamicTest> shouldParseInt() {
        final Map<Option<Object>, Object> validOptions = new HashMap<>();
        validOptions.put(Option.valueOf("short"), (short) 100);
        validOptions.put(Option.valueOf("int"), 100);
        validOptions.put(Option.valueOf("long"), 100L);
        validOptions.put(Option.valueOf("float"), 100F);
        validOptions.put(Option.valueOf("double"), 100D);
        validOptions.put(Option.valueOf("bigInteger"), BigInteger.valueOf(100L));
        validOptions.put(Option.valueOf("bigDecimal"), BigDecimal.valueOf(100L));
        validOptions.put(Option.valueOf("string"), "100");

        return validOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should parse %s option to int", option.name()), () -> {
                final Object value = validOptions.get(option);
                assertThat(OptionParser.parseInt(options(option, value), option)).isEqualTo(100);
            }));
    }

    @TestFactory
    public Stream<DynamicTest> failParseInt() {
        final Map<Option<Object>, Object> invalidOptions = new HashMap<>();
        invalidOptions.put(Option.valueOf("string"), "abc");
        invalidOptions.put(Option.valueOf("char"), 'a');
        invalidOptions.put(Option.valueOf("date"), new Date());
        invalidOptions.put(Option.valueOf("enum"), ValidationDepth.REMOTE);

        return invalidOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should fail parsing %s option to int", option.name()), () -> {
                final Object value = invalidOptions.get(option);
                assertThatThrownBy(() -> OptionParser.parseInt(options(option, value), option))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(String.format("Invalid %s option: %s", option.name(), value));
            }));
    }

    @TestFactory
    public Stream<DynamicTest> shouldParseEnum() {
        final Map<Option<Object>, Object> validOptions = new HashMap<>();
        validOptions.put(Option.valueOf("remote"), ValidationDepth.REMOTE);
        validOptions.put(Option.valueOf("remoteString"), "remote");
        validOptions.put(Option.valueOf("REMOTEString"), "REMOTE");

        return validOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should parse %s option to enum", option.name()), () -> {
                final Object value = validOptions.get(option);
                assertThat(OptionParser.parseEnum(options(option, value), option, ValidationDepth.class))
                    .isEqualTo(ValidationDepth.REMOTE);
            }));
    }

    @TestFactory
    public Stream<DynamicTest> failParseEnum() {
        final Map<Option<Object>, Object> invalidOptions = new HashMap<>();
        invalidOptions.put(Option.valueOf("string"), "abc");
        invalidOptions.put(Option.valueOf("char"), 'a');
        invalidOptions.put(Option.valueOf("date"), new Date());
        invalidOptions.put(Option.valueOf("int"), 100);

        return invalidOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should fail parsing %s option to enum", option.name()), () -> {
                final Object value = invalidOptions.get(option);
                assertThatThrownBy(() -> OptionParser.parseEnum(options(option, value), option, ValidationDepth.class))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(String.format("Invalid %s option: %s", option.name(), value));
            }));
    }

    @TestFactory
    public Stream<DynamicTest> shouldParseDuration() {
        final Map<Option<Object>, Object> validOptions = new HashMap<>();
        validOptions.put(Option.valueOf("duration"), Duration.ofMinutes(30L));
        validOptions.put(Option.valueOf("durationString"), "PT30M");

        return validOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should parse %s option to duration", option.name()), () -> {
                final Object value = validOptions.get(option);
                assertThat(OptionParser.parseDuration(options(option, value), option))
                    .isEqualTo(Duration.ofMinutes(30L));
            }));
    }

    @TestFactory
    public Stream<DynamicTest> failParseDuration() {
        final Map<Option<Object>, Object> invalidOptions = new HashMap<>();
        invalidOptions.put(Option.valueOf("string"), "abc");
        invalidOptions.put(Option.valueOf("char"), 'a');
        invalidOptions.put(Option.valueOf("date"), new Date());
        invalidOptions.put(Option.valueOf("int"), 100);
        invalidOptions.put(Option.valueOf("enum"), ValidationDepth.REMOTE);

        return invalidOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should fail parsing %s option to duration", option.name()), () -> {
                final Object value = invalidOptions.get(option);
                assertThatThrownBy(() -> OptionParser.parseDuration(options(option, value), option))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(String.format("Invalid %s option: %s", option.name(), value));
            }));
    }
}

/*
 * Copyright 2020 the original author or authors.
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Unit tests for {@link OptionMapper}.
 *
 * @author Rodolpho S. Couto
 * @author Mark Paluch
 */
final class OptionMapperUnitTests {

    @TestFactory
    Stream<DynamicTest> shouldParseInt() {
        Map<Option<Object>, Object> validOptions = new HashMap<>();
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

                Object value = validOptions.get(option);
                assertThat(OptionMapper.toInteger(value)).isEqualTo(100);
            }));
    }

    @TestFactory
    Stream<DynamicTest> failParseInt() {
        Map<Option<Object>, Object> invalidOptions = new HashMap<>();
        invalidOptions.put(Option.valueOf("string"), "abc");
        invalidOptions.put(Option.valueOf("char"), 'a');
        invalidOptions.put(Option.valueOf("date"), new Date());
        invalidOptions.put(Option.valueOf("enum"), ValidationDepth.REMOTE);

        return invalidOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should fail parsing %s option to int", option.name()), () -> {

                ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(option, invalidOptions.get(option)).build();

                Object value = invalidOptions.get(option);
                assertThatThrownBy(() -> {
                    OptionMapper.create(options).from(option).as(OptionMapper::toInteger).to(integer -> {
                    });
                })
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(String.format("Cannot assign option %s", option.name()));
            }));
    }

    @TestFactory
    Stream<DynamicTest> shouldParseEnum() {
        Map<Option<Object>, Object> validOptions = new HashMap<>();
        validOptions.put(Option.valueOf("remote"), ValidationDepth.REMOTE);
        validOptions.put(Option.valueOf("remoteString"), "remote");
        validOptions.put(Option.valueOf("REMOTEString"), "REMOTE");

        return validOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should parse %s option to enum", option.name()), () -> {

                ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(option, validOptions.get(option)).build();

                OptionMapper.create(options).from(option).as(o -> OptionMapper.toEnum(o, ValidationDepth.class)).to(value -> {
                    assertThat(value).isEqualTo((ValidationDepth.REMOTE));
                });
            }));
    }

    @TestFactory
    Stream<DynamicTest> shouldParseDuration() {
        Map<Option<Object>, Object> validOptions = new HashMap<>();
        validOptions.put(Option.valueOf("duration"), Duration.ofMinutes(30L));
        validOptions.put(Option.valueOf("durationString"), "PT30M");

        return validOptions.keySet()
            .stream()
            .map(option -> dynamicTest(format("Should parse %s option to duration", option.name()), () -> {
                Object value = validOptions.get(option);
                assertThat(OptionMapper.toDuration(value))
                    .isEqualTo(Duration.ofMinutes(30L));
            }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldApplyAndConvertOption() {

        Consumer<Integer> consumer = mock(Consumer.class);

        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().option(Option.valueOf("value"), "123").build();
        OptionMapper.create(options).from(Option.valueOf("value")).as(OptionMapper::toInteger).to(consumer);

        verify(consumer).accept(123);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNotApplyAbsentOption() {

        Consumer<Integer> consumer = mock(Consumer.class);

        ConnectionFactoryOptions options = ConnectionFactoryOptions.builder().build();
        OptionMapper.create(options).from(Option.valueOf("value")).as(OptionMapper::toInteger).to(consumer);

        verifyNoInteractions(consumer);
    }
}

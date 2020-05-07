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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * Validation utilities.
 */
final class Validation {

    static Mono<Void> validate(Connection connection, String validationQuery) {
        return Flux.from(connection.createStatement(validationQuery).execute()).flatMap(it -> it.map((row, rowMetadata) -> Optional.ofNullable(row.get(0)))).then().name(String.format("Connection " +
            "Validation [%s]", validationQuery));
    }

    static Mono<Void> validate(Connection connection, ValidationDepth depth) {
        return Flux.from(connection.validate(depth)).handle((state, sink) -> {

            if (state) {
                sink.complete();
                return;
            }

            sink.error(new R2dbcNonTransientResourceException("Connection validation failed"));
        }).then().name(String.format("Connection Validation [%s]", depth));
    }
}

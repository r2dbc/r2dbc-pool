# Reactive Relational Database Connectivity Connection Pool Implementation

This project contains a [R2DBC][r] connection pool using `reactor-pool` for reactive connection pooling.

[r]: https://github.com/r2dbc/r2dbc-spi

## Maven
Both milestone and snapshot artifacts (library, source, and javadoc) can be found in Maven repositories.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-pool</artifactId>
  <version>0.8.0.BUILD-SNAPSHOT</version>
</dependency>
```

Artifacts can be found at the following repositories.

### Repositories
```xml
<repository>
    <id>spring-snapshots</id>
    <name>Spring Snapshots</name>
    <url>https://repo.spring.io/snapshot</url>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
```

```xml
<repository>
    <id>spring-milestones</id>
    <name>Spring Milestones</name>
    <url>https://repo.spring.io/milestone</url>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
```

## Usage
Configuration of the `ConnectionPool` can be accomplished in two ways:

### Connection Factory Discovery
```java
ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
   .option(DRIVER, "pool")
   .option(PROTOCOL, "h2") // driver identifier, PROTOCOL is delegated as DRIVER by the pool.
   .option(HOST, "…")
   .option(PORT, "…") 
   .option(USER, "…")
   .option(PASSWORD, "…")
   .option(DATABASE, "…")
   .build());

The delegated `DRIVER` (via `PROTOCOL`) above refers to the r2dbc-driver, currently one of `h2`, `postgresql`, `mssql`.

Publisher<? extends Connection> connectionPublisher = connectionFactory.create();

// Alternative: Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

Supported Connection Factory Discovery options:

| Option            | Description
| ------            | -----------
| `driver`          | Must be `pool`
| `protocol`        | Driver identifier. The value is propagated by the pool to the `driver` property.
| `maxSize`         | Maximum pool size. Defaults to `10`.
| `validationDepth` | Validation depth used to validate an R2DBC connection. Defaults to `LOCAL`.
| `validationQuery` | Query that will be executed just before a connection is given to you from the pool to validate that the connection to the database is still alive.

All other properties are driver-specific

### Programmatic
```java
ConnectionFactory connectionFactory = …;

ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
   .maxIdleTime(Duration.ofMillis(1000))
   .maxSize(20)
   .build();

ConnectionPool pool = new ConnectionPool(configuration);
 

Mono<Connection> connectionMono = pool.create();

// later

Connection connection = …;
Mono<Void> release = connection.close(); // released the connection back to the pool

// application shutdown
pool.dispose();
```

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0

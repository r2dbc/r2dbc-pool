# Reactive Relational Database Connectivity Connection Pool Implementation [![Java CI with Maven](https://github.com/r2dbc/r2dbc-pool/workflows/Java%20CI%20with%20Maven/badge.svg?branch=main)](https://github.com/r2dbc/r2dbc-pool/actions?query=workflow%3A%22Java+CI+with+Maven%22+branch%3Amain) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-pool/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.r2dbc/r2dbc-pool)

This project contains a [R2DBC][r] connection pool using `reactor-pool` for reactive connection pooling.

[r]: https://github.com/r2dbc/r2dbc-spi

## Code of Conduct

This project is governed by the [R2DBC Code of Conduct](https://github.com/r2dbc/.github/blob/main/CODE_OF_CONDUCT.adoc). By participating, you are expected to uphold this code of conduct. Please report unacceptable behavior to [info@r2dbc.io](mailto:info@r2dbc.io).

## Getting Started

Configuration of the `ConnectionPool` can be accomplished in several ways:

**URL Connection Factory Discovery**

```java
// Creates a ConnectionPool wrapping an underlying ConnectionFactory 
ConnectionFactory pooledConnectionFactory = ConnectionFactories.get("r2dbc:pool:<my-driver>://<host>:<port>/<database>[?maxIdleTime=PT60S[&…]");

// Make sure to close the connection after usage.
Publisher<? extends Connection> connectionPublisher = pooledConnectionFactory.create();
```

**Programmatic Connection Factory Discovery**

```java
// Creates a ConnectionPool wrapping an underlying ConnectionFactory
ConnectionFactory pooledConnectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER,"pool")
        .option(PROTOCOL,"postgresql") // driver identifier, PROTOCOL is delegated as DRIVER by the pool.
        .option(HOST,"…")
        .option(PORT,"…")
        .option(USER,"…")
        .option(PASSWORD,"…")
        .option(DATABASE,"…")
        .build());
```

The delegated `DRIVER` (via `PROTOCOL`) above refers to the r2dbc-driver, such as `h2`, `postgresql`, `mssql`, `mysql`, `spanner`.

```java

// Make sure to close the connection after usage.
Publisher<? extends Connection> connectionPublisher = pooledConnectionFactory.create();

// Creating a Mono using Project Reactor. Using `usingWhen` for resource management and disposal.
Mono<Object> resultMono = Mono.usingWhen(pooledConnectionFactory.create(),
        connection -> …,
        Connection::close);
```

**Supported ConnectionFactory Discovery Options**

| Option                       | Description
|------------------------------| -----------
| `driver`                     | Must be `pool`
| `protocol`                   | Driver identifier. The value is propagated by the pool to the `driver` property.
| `acquireRetry`               | Number of retries if the first connection acquisition attempt fails. Defaults to `1`.
| `backgroundEvictionInterval` | Interval for background eviction enabling background eviction. Disabled by default. Setting the value to `Duration.ZERO` disables background eviction even if `maxIdleTime` is configured.
| `initialSize`                | Initial pool size. Defaults to `10`.
| `minIdle`                    | Minimum idle connection count. Defaults to `0`.
| `maxSize`                    | Maximum pool size. Defaults to `10`.
| `maxLifeTime`                | Maximum lifetime of the connection in the pool. Negative values indicate no timeout. Defaults to no timeout.
| `maxIdleTime`                | Maximum idle time of the connection in the pool. Negative values indicate no timeout. Defaults to `30` minutes.<br />This value is used as an interval for background eviction of idle connections unless configuring `backgroundEvictionInterval`.
| `maxAcquireTime`             | Maximum time to acquire connection from pool. Negative values indicate no timeout. Defaults to no timeout.
| `maxCreateConnectionTime`    | Maximum time to create a new connection. Negative values indicate no timeout. Defaults to no timeout.
| `maxValidationTime`          | Maximum time to validate connection from pool. Negative values indicate no timeout. Defaults to no timeout.
| `poolName`                   | Name of the Connection Pool.
| `postAllocate`               | Lifecycle function to prepare a connection after allocating it.
| `preRelease `                | Lifecycle function to prepare/cleanup a connection before releasing it.
| `registerJmx`                | Whether to register the pool to JMX.
| `validationDepth`            | Validation depth used to validate an R2DBC connection. Defaults to `LOCAL`.
| `validationQuery`            | Query that will be executed just before a connection is given to you from the pool to validate that the connection to the database is still alive.

All other properties are driver-specific.

**Programmatic Configuration**

```java
// Creates a ConnectionFactory for the specified DRIVER
ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER,"postgresql")
        .option(HOST,"…")
        .option(PORT,"…")
        .option(USER,"…")
        .option(PASSWORD,"…")
        .option(DATABASE,"…")
        .build());

// Create a ConnectionPool for connectionFactory
ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
        .maxIdleTime(Duration.ofMillis(1000))
        .maxSize(20)
        .build();

ConnectionPool pool = new ConnectionPool(configuration);


Using `usingWhen` for resource management and disposal.
Mono<Object> resultMono = Mono.usingWhen(pooledConnectionFactory.create(), // allocates a connection from the pool
        connection -> …,    // you get hold of the connection to run queries, manage transactions, …
        Connection::close); // releases the connection back to the pool

// application shutdown
pool.dispose();
```

## `Lifecycle` support

The R2DBC specification defines as of version 0.9 lifecycle support for connections (`Lifecycle.postAllocate`, `Lifecycle.preRelease`). R2DBC Pool integrates with connections that implement lifecycle methods by inspecting the actual connection. `postAllocate` is called after allocating a connection and before returning it to the caller. `preRelease` is called upon `Connection.close()`, right before returning the connection into the pool. Any error signals in `postAllocate` are propagated to the allocation subscriber. Error signals of `preRelease` are logged and suppressed. In both cases, error signals lead to immediate invalidation of the connection.

Additionally, the pool accepts custom `postAllocate` and `preRelease` functions through the builder to prepare the connection or to cleanup the connection before returning it into the pool. Custom lifecycle methods are called within the `Lifecycle` closure to ensure the connection-side lifecycle.

```java
ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
    .postAllocate(connection -> Flux.from(connection.createStatement("SET schema = …").execute()).flatMap(Result::getRowsUpdated).then())
    .preRelease(Connection::rollbackTransaction)
    .build();

ConnectionPool pool = new ConnectionPool(configuration);
```

### Maven configuration

Artifacts can be found on [Maven Central](https://search.maven.org/search?q=r2dbc-pool):

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-pool</artifactId>
  <version>${version}</version>
</dependency>
```

If you'd rather like the latest snapshots of the upcoming major version, use our Maven snapshot repository and declare the appropriate dependency version.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-pool</artifactId>
  <version>${version}.BUILD-SNAPSHOT</version>
</dependency>

<repository>
  <id>sonatype-nexus-snapshots</id>
  <name>Sonatype OSS Snapshot Repository</name>
  <url>https://oss.sonatype.org/content/repositories/snapshots</url>
</repository>
```

## Getting Help

Having trouble with R2DBC? We'd love to help!

* Check the [spec documentation](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/), and [Javadoc](https://r2dbc.io/spec/1.0.0.RELEASE/api/).
* If you are upgrading, check out the [changelog](https://r2dbc.io/spec/1.0.0.RELEASE/CHANGELOG.txt) for "new and noteworthy" features.
* Ask a question - we monitor [stackoverflow.com](https://stackoverflow.com) for questions
  tagged with [`r2dbc`](https://stackoverflow.com/tags/r2dbc). 
  You can also chat with the community on [Gitter](https://gitter.im/r2dbc/r2dbc).
* Report bugs with R2DBC Pool at [github.com/r2dbc/r2dbc-pool/issues](https://github.com/r2dbc/r2dbc-pool/issues).

## Reporting Issues

R2DBC uses GitHub as issue tracking system to record bugs and feature requests. 
If you want to raise an issue, please follow the recommendations below:

* Before you log a bug, please search the [issue tracker](https://github.com/r2dbc/r2dbc-pool/issues) to see if someone has already reported the problem.
* If the issue doesn't already exist, [create a new issue](https://github.com/r2dbc/r2dbc-pool/issues/new).
* Please provide as much information as possible with the issue report, we like to know the version of R2DBC Pool that you are using and JVM version.
* If you need to paste code, or include a stack trace use Markdown ``` escapes before and after your text.
* If possible try to create a test-case or project that replicates the issue. 
Attach a link to your code or a compressed file containing your code.

## Building from Source

You don't need to build from source to use R2DBC Pool (binaries in Maven Central), but if you want to try out the latest and greatest, R2DBC Pool can be easily built with the
[maven wrapper](https://github.com/takari/maven-wrapper). You also need JDK 1.8.

```bash
 $ ./mvnw clean install
```

If you want to build with the regular `mvn` command, you will need [Maven v3.5.0 or above](https://maven.apache.org/run-maven/index.html).

_Also see [CONTRIBUTING.adoc](https://github.com/r2dbc/.github/blob/main/CONTRIBUTING.adoc) if you wish to submit pull requests. Commits require `Signed-off-by` (`git commit -s`) to ensure [Developer Certificate of Origin](https://developercertificate.org/)._

## Staging to Maven Central

To stage a release to Maven Central, you need to create a release tag (release version) that contains the desired state and version numbers (`mvn versions:set versions:commit -q -o -DgenerateBackupPoms=false -DnewVersion=x.y.z.(RELEASE|Mnnn|RCnnn`) and force-push it to the `release-0.x` branch. This push will trigger a Maven staging build (see `build-and-deploy-to-maven-central.sh`).

## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0

# Java-to-Rust transfer

This is a Java 21 application. Its
[`JavaToRustExample.java`](src/main/java/ai/pipestream/examples/JavaToRustExample.java)
imports the reusable Netty `PipeStreamClient` and sends one immutable entity to
a separately running Rust/Quinn server.

Build the pinned transport extension and Java implementation in an isolated Maven
repository, then build this example using that same repository:

```sh
transport_repository=$(bash ../../implementations/java-netty/transport/build.sh)
mvn "-Dmaven.repo.local=$transport_repository" install -q -f ../../implementations/java-netty/pom.xml
mvn "-Dmaven.repo.local=$transport_repository" verify -q
```

With a Rust server and test certificates already running:

```sh
java --enable-native-access=ALL-UNNAMED \
  -jar target/java-to-rust-0.1.0-SNAPSHOT-all.jar \
  --connect 127.0.0.1:9443 --ca /path/to/ca.crt \
  --server-name localhost --entity-id 101 --input /path/to/input.bin
```

The `pipestream-conformance examples` command starts the surrounding processes
and executes this Java program during the full repository gate. The driver only
orchestrates compiled processes; the example behavior above is Java.

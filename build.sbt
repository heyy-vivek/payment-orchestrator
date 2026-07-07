name         := "payment-orchestrator"
version      := "0.1.0"
scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .enablePlugins(PlayScala)

// Akka 2.6.x is the last line that supports Java 8
val akkaVersion = "2.6.21"

libraryDependencies ++= Seq(
  guice,
  ws,

  // Akka — 2.6.x max for Java 8
  "com.typesafe.akka" %% "akka-actor-typed"  % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"        % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j"         % akkaVersion,

  // MongoDB — 4.x supports Java 8
  "org.mongodb.scala" %% "mongo-scala-driver" % "4.9.0",

  // Circe JSON — 0.14.x supports Java 8
  "io.circe" %% "circe-core"    % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser"  % "0.14.6",

  // Kafka — 3.3.x is the last line with solid Java 8 support
  "org.apache.kafka" % "kafka-clients" % "3.3.2",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.2.13",

  // Test
  "com.typesafe.akka" %% "akka-testkit"     % akkaVersion  % Test,
  "org.scalatest"     %% "scalatest"         % "3.2.17"     % Test,
  "org.scalatestplus" %% "mockito-4-11"      % "3.2.17.0"   % Test,
)

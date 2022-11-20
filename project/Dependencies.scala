import sbt._

object Dependencies {

  val flinkVersion = "1.13.2"
  val avroCoreVersion = "1.11.0"

  // logging
  val log4sVersion = "1.10.0"
  val logbackClassicVersion = "1.4.4"

  // aws
  val AWSSDKVersion = "2.17.295"

  lazy val FlinkDependencies: Seq[ModuleID] = Seq(
    "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
    "org.apache.flink" %% "flink-connector-kinesis" % flinkVersion,
    "org.apache.flink" %% "flink-connector-jdbc" % flinkVersion,
    "org.apache.flink" % "flink-avro" % flinkVersion,
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "org.apache.flink" %% "flink-clients" % flinkVersion,
    "org.apache.flink" %% "flink-test-utils" % flinkVersion % Test
      excludeAll (ExclusionRule(organization = "log4j", name = "log4j"))
  )

  lazy val FlinkUI: Seq[ModuleID] = Seq(
    "org.apache.flink" %% "flink-runtime-web" % flinkVersion % Test
  )

  lazy val FlinkTableDeps: Seq[ModuleID] = Seq(
    "org.apache.flink" % "flink-table-common" % flinkVersion,
    "org.apache.flink" % "flink-table-api-java" % flinkVersion,
    "org.apache.flink" %% "flink-table-api-java-bridge" % flinkVersion,
    "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion,
    // below to run Table API & SQL programs locally in IDE
    "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion,
    "org.apache.flink" %% "flink-table-runtime-blink" % flinkVersion
  )

  lazy val AvroDependencies: Seq[ModuleID] = Seq(
    "org.apache.avro" % "avro" % avroCoreVersion
  )

  lazy val AWSKDADeps: Seq[ModuleID] = Seq(
    "com.amazonaws" % "aws-kinesisanalytics-flink" % "2.0.0",
    "com.amazonaws" % "aws-kinesisanalytics-runtime" % "1.2.0"
  )

  lazy val AWSDynamoDeps: Seq[ModuleID] = Seq(
    "software.amazon.awssdk" % "dynamodb" % AWSSDKVersion
  )

  lazy val AWSDeps: Seq[ModuleID] = Seq(
    "software.amazon.awssdk" % "glue" % AWSSDKVersion,
    "software.amazon.awssdk" % "kinesis" % AWSSDKVersion,
    "software.amazon.glue" % "schema-registry-flink-serde" % "1.1.13",
    "software.amazon.awssdk" % "sso" % AWSSDKVersion
  )

  lazy val LoggingDependencies: Seq[ModuleID] = Seq(
    "ch.qos.logback" % "logback-classic" % logbackClassicVersion,
    "org.log4s" %% "log4s" % log4sVersion
  )

  lazy val PostgresDeps: Seq[ModuleID] = Seq(
    "org.postgresql" % "postgresql" % "42.4.2"
  )

  lazy val TestDeps: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % "3.2.14" % Test,
    "org.junit.jupiter" % "junit-jupiter-engine" % "5.9.0" % Test,
    "org.assertj" % "assertj-core" % "3.23.1" % Test,
    "org.testcontainers" % "testcontainers" % "1.17.4" % Test,
    "org.testcontainers" % "localstack" % "1.17.4" % Test
  )

  lazy val CommonDeps: Seq[ModuleID] =
    TestDeps ++
      AWSDeps ++
      LoggingDependencies

  lazy val flinkConnectorDynamoDeps: Seq[ModuleID] =
    TestDeps ++
      AWSDeps ++
      AWSDynamoDeps ++
      LoggingDependencies ++
      FlinkDependencies

}

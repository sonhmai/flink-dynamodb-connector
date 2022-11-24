package example.flink.sink.datastream;

import example.common.flink.sink.aws.DynamoDbClientUtil;
import example.common.flink.sink.datastream.DynamoDbBatchingOutputFormat;
import example.common.flink.sink.datastream.DynamoDbSyncSink;
import example.flink.sink.DockerLocalstack;
import example.flink.sink.TestFixture;
import example.flink.sink.fixture.DynamoDbBatchingOutputFormatTestEntry;
import example.flink.sink.fixture.DynamoDbBooksRepo;
import example.flink.sink.fixture.TestEntry;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.*;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.assertj.core.api.Assertions.assertThat;

public class DynamoDBSinkTest {

  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(new MiniClusterResourceConfiguration
          .Builder()
          .setNumberSlotsPerTaskManager(2)
          .setNumberTaskManagers(1)
          .build()
      );
  private final DockerLocalstack dockerLocalstack = new DockerLocalstack();
  private final DynamoDbClient ddb = createDynamoDbClient(getDynamoDbConfig());
  private final DynamoDbBooksRepo booksRepo = new DynamoDbBooksRepo(ddb);
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSinkTest.class);

  @BeforeAll
  public static void init() {
    System.setProperty(
        "software.amazon.awssdk.http.service.impl",
        "software.amazon.awssdk.http.apache.ApacheSdkHttpService"
    );
  }

  private DynamoDbBatchingOutputFormat<TestEntry> createOutputFormat() {
    return new DynamoDbBatchingOutputFormatTestEntry(getDynamoDbConfig());
  }

  private DynamoDbClient createDynamoDbClient(Properties configProps) {
    return DynamoDbClientUtil.build(configProps);
  }

  private Properties getDynamoDbConfig() {
    Properties properties = new Properties();
    properties.setProperty(AWSConfigConstants.AWS_REGION, dockerLocalstack.getRegion());
    properties.setProperty(AWSConfigConstants.AWS_ENDPOINT,
        dockerLocalstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString());
    properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
    properties.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER),
        dockerLocalstack.getAccessKey());
    properties.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER),
        dockerLocalstack.getSecretKey());
    return properties;
  }

  @Test
  public void testInsert() throws Exception {
    booksRepo.createTable();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
    env.setParallelism(1);
    DataStream<TestEntry> ds = env.fromElements(TestFixture.TEST_DATA);
    ds.addSink(new DynamoDbSyncSink<>(createOutputFormat()));
    env.execute();

    assertThat(booksRepo.getAllBooks())
        .isEqualTo(new HashSet<>(Arrays.asList(TestFixture.TEST_DATA)));
  }

}


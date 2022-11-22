package example.flink.sink.datastream;

import example.common.flink.sink.aws.DynamoDbClientUtil;
import example.common.flink.sink.datastream.DynamoDbBatchingOutputFormat;
import example.common.flink.sink.datastream.DynamoDbSyncSink;
import example.flink.sink.DockerLocalstack;
import example.flink.sink.TestFixture;
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
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

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
  private static final String TABLE_NAME = "books";
  private final DockerLocalstack dockerLocalstack = new DockerLocalstack();
  private final DynamoDbClient ddb = createDynamoDbClient(getDynamoDbConfig());
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSinkTest.class);

  @BeforeAll
  public static void init() {
    System.setProperty(
        "software.amazon.awssdk.http.service.impl",
        "software.amazon.awssdk.http.apache.ApacheSdkHttpService"
    );
  }

  private DynamoDbBatchingOutputFormat<TestFixture.TestEntry> createOutputFormat() {
    return new DynamoDbBatchingOutputFormat<>(getDynamoDbConfig());
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
    createBooksTable(this.ddb);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
    env.setParallelism(1);
    DataStream<TestFixture.TestEntry> ds = env.fromElements(TestFixture.TEST_DATA);
    ds.addSink(new DynamoDbSyncSink<>(createOutputFormat()));
    env.execute();

    assertThat(selectBooks(this.ddb))
        .isEqualTo(Arrays.asList(TestFixture.TEST_DATA));
  }

  public CreateTableResponse createBooksTable(DynamoDbClient ddb) {
    AttributeDefinition attributeDefinition = AttributeDefinition
        .builder()
        .attributeName("id")
        .attributeType(ScalarAttributeType.S)
        .build();
    KeySchemaElement keySchemaElement = KeySchemaElement
        .builder()
        .attributeName("id")
        .keyType(KeyType.HASH)
        .build();
    CreateTableRequest request = CreateTableRequest
        .builder()
        .tableName(TABLE_NAME)
        .attributeDefinitions(attributeDefinition)
        .keySchema(keySchemaElement)
        .provisionedThroughput(ProvisionedThroughput.builder()
            .readCapacityUnits(10L)
            .writeCapacityUnits(10L)
            .build())
        .build();
    CreateTableResponse response = ddb.createTable(request);

    // wait until table is created, table creation is async per aws doc
    DescribeTableRequest describeTableRequest = DescribeTableRequest
        .builder()
        .tableName(TABLE_NAME)
        .build();
    DynamoDbWaiter waiter = ddb.waiter();
    waiter.waitUntilTableExists(describeTableRequest);

    LOG.info("Created table, response: {}", response);
    return response;
  }

  private List<TestFixture.TestEntry> selectBooks(DynamoDbClient ddb) {
    List<TestFixture.TestEntry> books = new ArrayList<>();
    // TODO - query dynamodb
    ScanRequest scanRequest = ScanRequest.builder()
        .tableName(TABLE_NAME)
        .build();
    ScanResponse response = ddb.scan(scanRequest);
    for (Map<String, AttributeValue> item : response.items()) {
      LOG.info("Item: {}", item);
      Set<String> keys = item.keySet();
      LOG.info("Keys: {}", keys);
    }
    return books;
  }
}

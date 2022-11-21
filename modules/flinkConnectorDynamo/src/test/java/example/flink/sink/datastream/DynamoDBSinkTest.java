package example.flink.sink.datastream;

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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

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
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDBSinkTest.class);

  @BeforeAll
  public static void init() {
    System.setProperty(
        "software.amazon.awssdk.http.service.impl",
        "software.amazon.awssdk.http.apache.ApacheSdkHttpService"
    );
  }

  private DynamoDbBatchingOutputFormat<TestFixture.TestEntry> createOutputFormat() {
    Properties properties = new Properties();
    properties.setProperty(AWSConfigConstants.AWS_REGION, dockerLocalstack.getRegion());
    properties.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
    properties.setProperty(AWSConfigConstants.accessKeyId(AWS_CREDENTIALS_PROVIDER), dockerLocalstack.getAccessKey());
    properties.setProperty(AWSConfigConstants.secretKey(AWS_CREDENTIALS_PROVIDER), dockerLocalstack.getSecretKey());

    return new DynamoDbBatchingOutputFormat<>(properties);
  }

  @Test
  public void testInsert() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
    env.setParallelism(1);
    DataStream<TestFixture.TestEntry> ds = env.fromElements(TestFixture.TEST_DATA);
    ds.addSink(new DynamoDbSyncSink<>(createOutputFormat()));
    env.execute();

    assertThat(selectBooks(dockerLocalstack.ddb))
        .isEqualTo(Arrays.asList(TestFixture.TEST_DATA));
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

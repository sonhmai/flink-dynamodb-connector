package example.common.flink.sink.aws;

// TODO - this brings in flink kinesis connector dep which is not desired

import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.net.URI;
import java.util.Properties;

public class DynamoDbClientUtil {
  public static DynamoDbClient build(Properties configProps) {
    DynamoDbClientBuilder builder = DynamoDbClient
        .builder()
        .credentialsProvider(AwsV2Util.getCredentialsProvider(configProps))
        .region(Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION)))
        .httpClient(ApacheHttpClient.create());

    if (configProps.containsKey(AWSConfigConstants.AWS_ENDPOINT)) {
      final URI endpointOverride = URI.create(
          configProps.getProperty(AWSConfigConstants.AWS_ENDPOINT)
      );
      builder.endpointOverride(endpointOverride);
    }

    return builder.build();
  }
}

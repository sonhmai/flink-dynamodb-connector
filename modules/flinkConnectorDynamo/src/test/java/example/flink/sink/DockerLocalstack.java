package example.flink.sink;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DockerLocalstack {
  private static final DockerImageName LOCALSTACK_IMAGE =
      DockerImageName.parse("localstack/localstack:0.13.3");
  public final DynamoDbClient ddb;
  private final LocalStackContainer localstack;

  public DockerLocalstack() {
    this.localstack = new LocalStackContainer(LOCALSTACK_IMAGE)
        .withServices(LocalStackContainer.Service.DYNAMODB);
    localstack.start();
    localstack.waitingFor(Wait.forHttp("/").forStatusCode(200));
    this.ddb = DynamoDbClient
        .builder()
        .endpointOverride(localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
        ))
        .region(Region.of(localstack.getRegion()))
        .httpClient(ApacheHttpClient.create())
        .build();
  }

  public String getRegion() {
    return localstack.getRegion();
  }

  public String getAccessKey() {
    return localstack.getAccessKey();
  }

  public String getSecretKey() {
    return localstack.getSecretKey();
  }

}

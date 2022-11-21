package example.common.flink.sink.aws;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.CredentialProvider;
import org.apache.flink.streaming.connectors.kinesis.util.AWSUtil;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;

/**
 * Copying from flink-connector-kinesis-1.13.2: org.apache.flink.streaming.connectors.kinesis.util.AwsV2Util
 * Utility methods specific to Amazon Web Service SDK v2.x. */
public class AwsV2Util {

  static ClientOverrideConfiguration createClientOverrideConfiguration(
      final ClientConfiguration config,
      final ClientOverrideConfiguration.Builder overrideConfigurationBuilder) {

    overrideConfigurationBuilder
        .putAdvancedOption(
            SdkAdvancedClientOption.USER_AGENT_PREFIX,
            AWSUtil.formatFlinkUserAgentPrefix())
        .putAdvancedOption(
            SdkAdvancedClientOption.USER_AGENT_SUFFIX, config.getUserAgentSuffix());

    if (config.getRequestTimeout() > 0) {
      overrideConfigurationBuilder.apiCallAttemptTimeout(
          Duration.ofMillis(config.getRequestTimeout()));
    }

    if (config.getClientExecutionTimeout() > 0) {
      overrideConfigurationBuilder.apiCallTimeout(
          Duration.ofMillis(config.getClientExecutionTimeout()));
    }

    return overrideConfigurationBuilder.build();
  }

  /**
   * Return a {@link AWSCredentialsProvider} instance corresponding to the configuration
   * properties.
   *
   * @param configProps the configuration properties
   * @return The corresponding AWS Credentials Provider instance
   */
  public static AwsCredentialsProvider getCredentialsProvider(final Properties configProps) {
    return getCredentialsProvider(configProps, AWSConfigConstants.AWS_CREDENTIALS_PROVIDER);
  }

  private static AwsCredentialsProvider getCredentialsProvider(
      final Properties configProps, final String configPrefix) {
    CredentialProvider credentialProviderType =
        getCredentialProviderType(configProps, configPrefix);

    switch (credentialProviderType) {
      case ENV_VAR:
        return EnvironmentVariableCredentialsProvider.create();

      case SYS_PROP:
        return SystemPropertyCredentialsProvider.create();

      case PROFILE:
        return getProfileCredentialProvider(configProps, configPrefix);

      case BASIC:
        return () ->
            AwsBasicCredentials.create(
                configProps.getProperty(
                    AWSConfigConstants.accessKeyId(configPrefix)),
                configProps.getProperty(
                    AWSConfigConstants.secretKey(configPrefix)));

      case ASSUME_ROLE:
        return getAssumeRoleCredentialProvider(configProps, configPrefix);

      case WEB_IDENTITY_TOKEN:
        return getWebIdentityTokenFileCredentialsProvider(
            WebIdentityTokenFileCredentialsProvider.builder(),
            configProps,
            configPrefix);

      case AUTO:
        return DefaultCredentialsProvider.create();

      default:
        throw new IllegalArgumentException(
            "Credential provider not supported: " + credentialProviderType);
    }
  }

  private static AwsCredentialsProvider getProfileCredentialProvider(
      final Properties configProps, final String configPrefix) {
    String profileName =
        configProps.getProperty(AWSConfigConstants.profileName(configPrefix), null);
    String profileConfigPath =
        configProps.getProperty(AWSConfigConstants.profilePath(configPrefix), null);

    ProfileCredentialsProvider.Builder profileBuilder =
        ProfileCredentialsProvider.builder().profileName(profileName);

    if (profileConfigPath != null) {
      profileBuilder.profileFile(
          ProfileFile.builder()
              .type(ProfileFile.Type.CREDENTIALS)
              .content(Paths.get(profileConfigPath))
              .build());
    }

    return profileBuilder.build();
  }

  private static AwsCredentialsProvider getAssumeRoleCredentialProvider(
      final Properties configProps, final String configPrefix) {
    return StsAssumeRoleCredentialsProvider.builder()
        .refreshRequest(
            AssumeRoleRequest.builder()
                .roleArn(
                    configProps.getProperty(
                        AWSConfigConstants.roleArn(configPrefix)))
                .roleSessionName(
                    configProps.getProperty(
                        AWSConfigConstants.roleSessionName(configPrefix)))
                .externalId(
                    configProps.getProperty(
                        AWSConfigConstants.externalId(configPrefix)))
                .build())
        .stsClient(
            StsClient.builder()
                .credentialsProvider(
                    getCredentialsProvider(
                        configProps,
                        AWSConfigConstants.roleCredentialsProvider(
                            configPrefix)))
                .region(getRegion(configProps))
                .build())
        .build();
  }

  static AwsCredentialsProvider getWebIdentityTokenFileCredentialsProvider(
      final WebIdentityTokenFileCredentialsProvider.Builder webIdentityBuilder,
      final Properties configProps,
      final String configPrefix) {

    webIdentityBuilder
        .roleArn(configProps.getProperty(AWSConfigConstants.roleArn(configPrefix), null))
        .roleSessionName(
            configProps.getProperty(
                AWSConfigConstants.roleSessionName(configPrefix), null));

    Optional.ofNullable(
            configProps.getProperty(
                AWSConfigConstants.webIdentityTokenFile(configPrefix), null))
        .map(Paths::get)
        .ifPresent(webIdentityBuilder::webIdentityTokenFile);

    return webIdentityBuilder.build();
  }

  /**
   * Creates a {@link Region} object from the given Properties.
   *
   * @param configProps the properties containing the region
   * @return the region specified by the properties
   */
  public static Region getRegion(final Properties configProps) {
    return Region.of(configProps.getProperty(AWSConfigConstants.AWS_REGION));
  }

  public static boolean isRecoverableException(Exception e) {
    Throwable cause = e.getCause();
    return cause instanceof LimitExceededException
        || cause instanceof ProvisionedThroughputExceededException;
  }

  /**
   * copying from org.apache.flink.streaming.connectors.kinesis.util.AWSUtil
   * Determines and returns the credential provider type from the given properties.
   *
   * @return the credential provider type
   */
  static CredentialProvider getCredentialProviderType(
      final Properties configProps, final String configPrefix) {
    if (!configProps.containsKey(configPrefix)) {
      if (configProps.containsKey(AWSConfigConstants.accessKeyId(configPrefix))
          && configProps.containsKey(AWSConfigConstants.secretKey(configPrefix))) {
        // if the credential provider type is not specified, but the Access Key ID and
        // Secret Key are given, it will default to BASIC
        return CredentialProvider.BASIC;
      } else {
        // if the credential provider type is not specified, it will default to AUTO
        return CredentialProvider.AUTO;
      }
    } else {
      return CredentialProvider.valueOf(configProps.getProperty(configPrefix));
    }
  }
}

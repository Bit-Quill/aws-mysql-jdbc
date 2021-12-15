package testsuite.integration.host;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.LogManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.*;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import testsuite.integration.utility.ContainerFeedLogger;
import testsuite.integration.utility.ExecInContainerUtility;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisIntegrationEnvironmentTest {

  //private static final String TEST_CONTAINER_IMAGE_NAME = "gradle:4.7.0-jdk8-alpine";
  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";
  //private static final String TEST_CONTAINER_IMAGE_NAME = "ubuntu";
  //private static final String TEST_CONTAINER_IMAGE_NAME = "amazoncorretto:8";
  //private static final String TEST_CONTAINER_IMAGE_NAME = "gradle:4.7.0-jdk8-alpine";

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";
  private static final String REDIS_A_NETWORK_ALIAS = "redis-a";
  private static final String REDIS_B_NETWORK_ALIAS = "redis-b";
  private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

  private static final int REDIS_PORT = 6379;
  private static int REDIS_PROXY_PORT;

  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:5.0.4");
  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> testContainer;
  private static GenericContainer<?> redisContainer_A;
  private static GenericContainer<?> redisContainer_B;
  private static ToxiproxyContainer toxiproxyContainer;

  @BeforeAll
  public static void setUp() {
    Network network = Network.newNetwork();

    redisContainer_A = new GenericContainer<>(REDIS_IMAGE)
      .withExposedPorts(REDIS_PORT)
      .withNetworkAliases(REDIS_A_NETWORK_ALIAS)
      .withNetwork(network);
    redisContainer_A.start();
    System.out.println("Redis A container name: " + redisContainer_A.getContainerName());

    redisContainer_B = new GenericContainer<>(REDIS_IMAGE)
      .withExposedPorts(REDIS_PORT)
      .withNetworkAliases(REDIS_B_NETWORK_ALIAS)
      .withNetwork(network);
    redisContainer_B.start();
    System.out.println("Redis B container name: " + redisContainer_B.getContainerName());

    toxiproxyContainer = new ToxiproxyContainer(TOXIPROXY_IMAGE)
      .withNetwork(network)
      .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
    toxiproxyContainer.start();
    System.out.println("Toxyproxy container name: " + toxiproxyContainer.getContainerName());

    ToxiproxyContainer.ContainerProxy proxy_a = toxiproxyContainer.getProxy(REDIS_A_NETWORK_ALIAS, REDIS_PORT);
    REDIS_PROXY_PORT = proxy_a.getOriginalProxyPort();

    testContainer = new GenericContainer<>(
      new ImageFromDockerfile("bq/rds-test-container", false)
        .withDockerfileFromBuilder(builder ->
          builder
            .from(TEST_CONTAINER_IMAGE_NAME)
            .run("mkdir", "app")
            .workDir("/app")
            .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
            .build()))
      .withNetworkAliases(TEST_CONTAINER_NETWORK_ALIAS)
      .withNetwork(network)
      .withFileSystemBind("./.git", "/app/.git", BindMode.READ_WRITE)
      .withFileSystemBind("./build", "/app/build", BindMode.READ_WRITE)
      .withFileSystemBind("./config", "/app/config", BindMode.READ_WRITE)
      .withFileSystemBind("./docs", "/app/docs", BindMode.READ_WRITE)
      .withFileSystemBind("./src", "/app/src", BindMode.READ_WRITE)
      .withFileSystemBind("./gradle", "/app/gradle", BindMode.READ_WRITE)
      .withCopyFileToContainer(MountableFile.forHostPath("./gradlew"), "app/gradlew")
      .withCopyFileToContainer(MountableFile.forHostPath("./gradle.properties"), "app/gradle.properties")
      .withCopyFileToContainer(MountableFile.forHostPath("./build.gradle.kts"), "app/build.gradle.kts")
      .withEnv("REDIS_A_NETWORK_ALIAS", REDIS_A_NETWORK_ALIAS)
      .withEnv("REDIS_A_PORT", Integer.toString(REDIS_PORT))
      .withEnv("REDIS_B_NETWORK_ALIAS", REDIS_B_NETWORK_ALIAS)
      .withEnv("REDIS_B_PORT", Integer.toString(REDIS_PORT))
      .withEnv("TOXIPROXY_NETWORK_ALIAS", TOXIPROXY_NETWORK_ALIAS)
      .withEnv("REDIS_PROXY_PORT", Integer.toString(REDIS_PROXY_PORT));
    testContainer.start();
    System.out.println("Test container name: " + testContainer.getContainerName());
  }

  @AfterAll
  public static void tearDown() {
    redisContainer_A.stop();
    redisContainer_B.stop();
    toxiproxyContainer.stop();
    testContainer.stop();
  }

  @Test
  public void testRunTestInContainer() throws UnsupportedOperationException, IOException, InterruptedException {
    //Logger logger = LoggerFactory.getLogger(RedisIntegrationEnvironmentTest.class);
    Logger logger = new ContainerFeedLogger();
    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger);
    testContainer.followOutput(logConsumer);

    Integer exitCode = ExecInContainerUtility.execInContainer(testContainer, logConsumer, "./gradlew", "test-integration-container");
    assertEquals(0, exitCode, "Some tests failed.");

    //Container.ExecResult results = testContainer.execInContainer("./gradlew", "test-integration-container");
    //System.out.println("==================\n" + results.getStdout() + "\n===============================");
    //assertEquals(0, results.getExitCode(), "Some tests failed.");
  }

}

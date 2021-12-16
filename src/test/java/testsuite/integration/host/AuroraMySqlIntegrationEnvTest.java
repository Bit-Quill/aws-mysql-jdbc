package testsuite.integration.host;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.*;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;

import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuroraMySqlIntegrationEnvTest {

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";

  private static final String PROXIED_DOMAIN_NAME_PREFIX = ".proxied";

  private static final String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS = "toxiproxy-instance-1";
  private static final String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS = "toxiproxy-instance-2";
  private static final String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS = "toxiproxy-instance-3";
  private static final String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS = "toxiproxy-instance-4";
  private static final String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS = "toxiproxy-instance-5";
  private static final String MYSQL_INSTANCE_1_URL = "mysql-instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com";
  private static final String MYSQL_INSTANCE_2_URL = "mysql-instance-2.czygpppufgy4.us-east-2.rds.amazonaws.com";
  private static final String MYSQL_INSTANCE_3_URL = "mysql-instance-3.czygpppufgy4.us-east-2.rds.amazonaws.com";
  private static final String MYSQL_INSTANCE_4_URL = "mysql-instance-4.czygpppufgy4.us-east-2.rds.amazonaws.com";
  private static final String MYSQL_INSTANCE_5_URL = "mysql-instance-5.czygpppufgy4.us-east-2.rds.amazonaws.com";
  private static final int MYSQL_PORT = 3306;
  private static int MYSQL_PROXY_PORT;

  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";

  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> testContainer;
  private static ToxiproxyContainer toxiproxyContainerInstance_1;
  private static ToxiproxyContainer toxiproxyContainerInstance_2;
  private static ToxiproxyContainer toxiproxyContainerInstance_3;
  private static ToxiproxyContainer toxiproxyContainerInstance_4;
  private static ToxiproxyContainer toxiproxyContainerInstance_5;

  @BeforeAll
  public static void setUp() {
    Network network = Network.newNetwork();

    // mysql instance 1
    toxiproxyContainerInstance_1 = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(
                    TOXIPROXY_INSTANCE_1_NETWORK_ALIAS,
                    MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX);
    toxiproxyContainerInstance_1.start();
    System.out.println("Toxyproxy Instance 1 container name: " + toxiproxyContainerInstance_1.getContainerName());

    ToxiproxyContainer.ContainerProxy mysql_instance_1_proxy = toxiproxyContainerInstance_1.getProxy(MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    MYSQL_PROXY_PORT = mysql_instance_1_proxy.getOriginalProxyPort();

    // mysql instance 2
    toxiproxyContainerInstance_2 = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(
                    TOXIPROXY_INSTANCE_2_NETWORK_ALIAS,
                    MYSQL_INSTANCE_2_URL + PROXIED_DOMAIN_NAME_PREFIX);
    toxiproxyContainerInstance_2.start();
    System.out.println("Toxyproxy Instance 2 container name: " + toxiproxyContainerInstance_2.getContainerName());

    ToxiproxyContainer.ContainerProxy mysql_instance_2_proxy = toxiproxyContainerInstance_2.getProxy(MYSQL_INSTANCE_2_URL, MYSQL_PORT);

    // mysql instance 3
    toxiproxyContainerInstance_3 = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(
                    TOXIPROXY_INSTANCE_3_NETWORK_ALIAS,
                    MYSQL_INSTANCE_3_URL + PROXIED_DOMAIN_NAME_PREFIX);
    toxiproxyContainerInstance_3.start();
    System.out.println("Toxyproxy Instance 3 container name: " + toxiproxyContainerInstance_3.getContainerName());

    ToxiproxyContainer.ContainerProxy mysql_instance_3_proxy = toxiproxyContainerInstance_3.getProxy(MYSQL_INSTANCE_3_URL, MYSQL_PORT);

    // mysql instance 4
    toxiproxyContainerInstance_4 = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(
                    TOXIPROXY_INSTANCE_4_NETWORK_ALIAS,
                    MYSQL_INSTANCE_4_URL + PROXIED_DOMAIN_NAME_PREFIX);
    toxiproxyContainerInstance_4.start();
    System.out.println("Toxyproxy Instance 4 container name: " + toxiproxyContainerInstance_4.getContainerName());

    ToxiproxyContainer.ContainerProxy mysql_instance_4_proxy = toxiproxyContainerInstance_4.getProxy(MYSQL_INSTANCE_4_URL, MYSQL_PORT);

    // mysql instance 5
    toxiproxyContainerInstance_5 = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(
                    TOXIPROXY_INSTANCE_5_NETWORK_ALIAS,
                    MYSQL_INSTANCE_5_URL + PROXIED_DOMAIN_NAME_PREFIX);
    toxiproxyContainerInstance_5.start();
    System.out.println("Toxyproxy Instance 5 container name: " + toxiproxyContainerInstance_5.getContainerName());

    ToxiproxyContainer.ContainerProxy mysql_instance_5_proxy = toxiproxyContainerInstance_5.getProxy(MYSQL_INSTANCE_5_URL, MYSQL_PORT);

    System.out.println("Toxyproxy Instances port: " + MYSQL_PROXY_PORT);

    // test container
    testContainer = new GenericContainer<>(
            new ImageFromDockerfile("bq/rds-test-container", true)
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
            .withEnv("MYSQL_INSTANCE_1_URL", MYSQL_INSTANCE_1_URL)
            .withEnv("MYSQL_INSTANCE_2_URL", MYSQL_INSTANCE_2_URL)
            .withEnv("MYSQL_INSTANCE_3_URL", MYSQL_INSTANCE_3_URL)
            .withEnv("MYSQL_INSTANCE_4_URL", MYSQL_INSTANCE_4_URL)
            .withEnv("MYSQL_INSTANCE_5_URL", MYSQL_INSTANCE_5_URL)
            .withEnv("MYSQL_PORT", Integer.toString(MYSQL_PORT))
            .withEnv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS", TOXIPROXY_INSTANCE_1_NETWORK_ALIAS)
            .withEnv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS", TOXIPROXY_INSTANCE_2_NETWORK_ALIAS)
            .withEnv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS", TOXIPROXY_INSTANCE_3_NETWORK_ALIAS)
            .withEnv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS", TOXIPROXY_INSTANCE_4_NETWORK_ALIAS)
            .withEnv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS", TOXIPROXY_INSTANCE_5_NETWORK_ALIAS)
            .withEnv("PROXIED_DOMAIN_NAME_PREFIX", PROXIED_DOMAIN_NAME_PREFIX)
            .withEnv("MYSQL_PROXY_PORT", Integer.toString(MYSQL_PROXY_PORT));
    testContainer.start();
    System.out.println("Test container name: " + testContainer.getContainerName());
  }

  @AfterAll
  public static void tearDown() {
    toxiproxyContainerInstance_1.stop();
    toxiproxyContainerInstance_2.stop();
    toxiproxyContainerInstance_3.stop();
    toxiproxyContainerInstance_4.stop();
    toxiproxyContainerInstance_5.stop();
    testContainer.stop();
  }

  @Test
  public void testRunTestInContainer() throws UnsupportedOperationException, IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Integer exitCode = ExecInContainerUtility.execInContainer(testContainer, consumer, "./gradlew", "test-integration-container-aurora");
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

}

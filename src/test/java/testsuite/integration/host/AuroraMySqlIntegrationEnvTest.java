package testsuite.integration.host;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuroraMySqlIntegrationEnvTest {

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String TEST_DB_CLUSTER_IDENTIFIER =
      System.getenv("TEST_DB_CLUSTER_IDENTIFIER");

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID "
          + "FROM information_schema.replica_host_status ";

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";

  private static final String PROXIED_DOMAIN_NAME_PREFIX = ".proxied";

  private static List<String> mySqlInstances = new ArrayList<>();
  private static final int MYSQL_PORT = 3306;
  private static int MYSQL_PROXY_PORT;

  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";

  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> testContainer;
  private static List<ToxiproxyContainer> toxiproxyContainerList = new ArrayList<>();

  @BeforeAll
  public static void setUp() {
    Network network = Network.newNetwork();
    setUpToxiProxy(network);
    setUpTestContainer(network);
  }

  @AfterAll
  public static void tearDown() {
    for (ToxiproxyContainer proxy : toxiproxyContainerList) {
      proxy.stop();
    }
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

  private static void setUpToxiProxy(Network network) {
    try (Connection conn = DriverManager.getConnection(getClusterEndpoint(), TEST_USERNAME, TEST_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        // Get instances
        try (ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
          int instanceCount = 0;
          while (resultSet.next()) {
            // Get Instance endpoints
            String hostEndpoint = resultSet.getString("SERVER_ID") + DB_CONN_STR_SUFFIX;
            mySqlInstances.add(hostEndpoint);

            // Create & Start Toxi Proxy Container
            ToxiproxyContainer toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(
                    "toxiproxy-instance-" + (++instanceCount),
                    hostEndpoint + PROXIED_DOMAIN_NAME_PREFIX);
            toxiProxy.start();
            ToxiproxyContainer.ContainerProxy mysqlInstanceProxy  = toxiProxy.getProxy(hostEndpoint, MYSQL_PORT);
            MYSQL_PROXY_PORT = mysqlInstanceProxy.getOriginalProxyPort();
            toxiproxyContainerList.add(toxiProxy);
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static void setUpTestContainer(Network network) {
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
        .withEnv("TEST_USERNAME", TEST_USERNAME)
        .withEnv("TEST_PASSWORD", TEST_PASSWORD);

    // Add mysql instances & proxies to container env
    for (int i = 0; i < mySqlInstances.size(); i++){
      // Add instance
      testContainer.addEnv(
          "MYSQL_INSTANCE_" + (i + 1) + "_URL",
          mySqlInstances.get(i));

      // Add proxies
      testContainer.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS",
          "toxiproxy-instance-" + (i + 1));
    }
    testContainer.addEnv("MYSQL_PORT", Integer.toString(MYSQL_PORT));
    testContainer.addEnv("PROXIED_DOMAIN_NAME_PREFIX", PROXIED_DOMAIN_NAME_PREFIX);
    testContainer.addEnv("MYSQL_PROXY_PORT", Integer.toString(MYSQL_PROXY_PORT));

    System.out.println("Toxyproxy Instances port: " + MYSQL_PROXY_PORT);

    testContainer.start();
    System.out.println("Test container name: " + testContainer.getContainerName());
  }

  private static String getClusterEndpoint() {
    String suffix = DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX;
    return DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + suffix;
  }
}

package testsuite.integration.host;

import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
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

import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuroraMySqlIntegrationEnvTest {

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2"; // Encounters SSL errors without it on GH Actions
  private static final String TEST_DB_CLUSTER_IDENTIFIER = System.getenv("TEST_DB_CLUSTER_IDENTIFIER");

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status ";

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";

  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";

  private static final List<ToxiproxyContainer> toxiproxyContainerList = new ArrayList<>();

  private static final List<String> mySqlInstances = new ArrayList<>();
  private static final int MYSQL_PORT = 3306;
  private static int MYSQL_PROXY_PORT;

  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";

  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> testContainer;

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
  public void testRunTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException, SQLException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Integer exitCode = ExecInContainerUtility.execInContainer(testContainer, consumer, "./gradlew", "test-integration-container-aurora");
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  private static void setUpToxiProxy(Network network) {
    try {
      DriverManager.registerDriver(new Driver());
      try (Connection conn = DriverManager.getConnection(getClusterEndpoint(), TEST_USERNAME, TEST_PASSWORD)) {
        try (Statement stmt = conn.createStatement()) {
          // Get instances
          try (ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
            int writerCount = 0;
            while (resultSet.next()) {
              String instanceEndpoint = resultSet.getString("SERVER_ID") + DB_CONN_STR_SUFFIX;

              if (!"MASTER_SESSION_ID".equalsIgnoreCase(resultSet.getString("SESSION_ID"))) {
                mySqlInstances.add(instanceEndpoint);
                continue;
              }

              if (writerCount == 0) {
                // store the first writer to its expected position [0]
                mySqlInstances.add(0, instanceEndpoint);
              } else {
                // append other writers, if any, to the end of the host list
                mySqlInstances.add(instanceEndpoint);
              }
              writerCount++;
            }
          }
        }
      }
    } catch (SQLException e) {
      Assertions.fail(String.format("Failed initialize instances. Got exception: \n%s", e.getMessage()));
    }

    int instanceCount = 0;
    for (String endpoints : mySqlInstances) {
      // Create & Start Toxi Proxy Container
      ToxiproxyContainer toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
          .withNetwork(network)
          .withNetworkAliases(
              "toxiproxy-instance-" + (++instanceCount),
              endpoints + PROXIED_DOMAIN_NAME_SUFFIX);
      toxiProxy.start();
      System.out.println("Toxi Proxy Container Name: " + toxiProxy.getContainerName());
      ToxiproxyContainer.ContainerProxy mysqlInstanceProxy = toxiProxy.getProxy(endpoints, MYSQL_PORT);
      MYSQL_PROXY_PORT = mysqlInstanceProxy.getOriginalProxyPort();

      toxiproxyContainerList.add(toxiProxy);
    }
  }

  private static String getClusterEndpoint() {
    String suffix = DB_CONN_STR_SUFFIX.startsWith(".") ? DB_CONN_STR_SUFFIX.substring(1) : DB_CONN_STR_SUFFIX;
    return DB_CONN_STR_PREFIX + TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + suffix + DB_CONN_PROP;
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
        .withEnv("TEST_PASSWORD", TEST_PASSWORD)
        .withEnv("PROXIED_CLUSTER_TEMPLATE", "?" + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX);

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
    testContainer.addEnv("PROXIED_DOMAIN_NAME_PREFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    testContainer.addEnv("MYSQL_PROXY_PORT", Integer.toString(MYSQL_PROXY_PORT));

    System.out.println("Toxyproxy Instances port: " + MYSQL_PROXY_PORT);

    testContainer.start();
    System.out.println("Test Container Name: " + testContainer.getContainerName());
  }
}

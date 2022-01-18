/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package testsuite.integration.host;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import software.aws.rds.jdbc.mysql.Driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class AuroraIntegrationContainerTest extends ContainerBaseTest {
  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2";
      // Encounters SSL errors without it on GH Actions
  private static final String TEST_DB_CLUSTER_IDENTIFIER =
      System.getenv("TEST_DB_CLUSTER_IDENTIFIER");
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID FROM information_schema.replica_host_status ";
  private static final DockerImageName TOXIPROXY_IMAGE =
      DockerImageName.parse("shopify/toxiproxy:2.1.0");
  private static final List<ToxiproxyContainer> toxiproxyContainerList = new ArrayList<>();
  private static final List<String> mySqlInstances = new ArrayList<>();

  private static int mySQLProxyPort;
  private static GenericContainer<?> integrationTestContainer;
  private static String dbHostCluster = "";
  private static String dbHostClusterRo = "";

  private Network network;

  @BeforeEach
  void setUp() {
    final String dbConnHostBase =
        DB_CONN_STR_SUFFIX.startsWith(".")
            ? DB_CONN_STR_SUFFIX.substring(1)
            : DB_CONN_STR_SUFFIX;
    dbHostCluster = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + dbConnHostBase;
    dbHostClusterRo = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-ro-" + dbConnHostBase;
    network = Network.newNetwork();
  }

  @AfterEach
  void tearDown() {
    for (ToxiproxyContainer proxy : toxiproxyContainerList) {
      proxy.stop();
    }
    integrationTestContainer.stop();
  }

  @Test
  public void testRunTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException, SQLException {
    initializeToxiProxy(network, toxiproxyContainerList);
    integrationTestContainer = initializeTestContainer(network);
    runTest(integrationTestContainer, "test-integration-container-aurora");
  }

  private void initializeToxiProxy(Network network, List<ToxiproxyContainer> toxiproxyContainerList) {
    try {
      DriverManager.registerDriver(new Driver());
      try (final Connection conn = DriverManager.getConnection(DB_CONN_STR_PREFIX + dbHostCluster
          + DB_CONN_PROP, TEST_USERNAME, TEST_PASSWORD);
           final Statement stmt = conn.createStatement()) {
          // Get instances
          try (final ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
            int instanceCount = 0;
            while (resultSet.next()) {
              // Get Instance endpoints
              final String hostEndpoint = resultSet.getString("SERVER_ID") + DB_CONN_STR_SUFFIX;
              mySqlInstances.add(hostEndpoint);

              // Create & Start Toxi Proxy Container
              final ToxiproxyContainer toxiProxy = initializeToxiProxy(
                  network,
                  "toxiproxy-instance-" + (++instanceCount),
                  hostEndpoint + PROXIED_DOMAIN_NAME_SUFFIX,
                  hostEndpoint);

              toxiproxyContainerList.add(toxiProxy);
              mySQLProxyPort = toxiProxy.getProxy(hostEndpoint, MYSQL_PORT).getOriginalProxyPort();
          }
        }
      }
    } catch (SQLException e) {
      Assertions.fail(String.format("Failed to initialize instances. Got exception: \n%s", e.getMessage()));
    }

    toxiproxyContainerList.add(initializeToxiProxy(
        network,
        "toxiproxy-instance-cluster",
        dbHostCluster + PROXIED_DOMAIN_NAME_SUFFIX,
        dbHostCluster)
    );

    toxiproxyContainerList.add(initializeToxiProxy(
        network,
        "toxiproxy-ro-instance-cluster",
        dbHostClusterRo + PROXIED_DOMAIN_NAME_SUFFIX,
        dbHostClusterRo)
    );
  }

  private ToxiproxyContainer initializeToxiProxy(final Network network, String networkAlias, String networkUrl, String hostname) {
    final ToxiproxyContainer container = new ToxiproxyContainer(TOXIPROXY_IMAGE)
        .withNetwork(network)
        .withNetworkAliases(networkAlias, networkUrl);
    container.start();
    container.getProxy(hostname, MYSQL_PORT);
    return container;
  }

  @Override
  GenericContainer<?> initializeTestContainer(final Network network) {
    final GenericContainer<?> container = createTestContainerTemplate("bq/rds-test-container")
        .withNetworkAliases(TEST_CONTAINER_NETWORK_ALIAS)
        .withNetwork(network)
        .withEnv("TEST_USERNAME", TEST_USERNAME)
        .withEnv("TEST_PASSWORD", TEST_PASSWORD)
        .withEnv("DB_CLUSTER_CONN", dbHostCluster)
        .withEnv("DB_RO_CLUSTER_CONN", dbHostClusterRo)
        .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
        .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
        .withEnv("PROXIED_CLUSTER_TEMPLATE", "?" + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX);

    // Add mysql instances & proxies to container env
    for (int i = 0; i < mySqlInstances.size(); i++) {
      // Add instance
      container.addEnv(
          "MYSQL_INSTANCE_" + (i + 1) + "_URL",
          mySqlInstances.get(i));

      // Add proxies
      container.addEnv(
          "TOXIPROXY_INSTANCE_" + (i + 1) + "_NETWORK_ALIAS",
          "toxiproxy-instance-" + (i + 1));
    }
    container.addEnv("MYSQL_PORT", Integer.toString(MYSQL_PORT));
    container.addEnv("PROXIED_DOMAIN_NAME_SUFFIX", PROXIED_DOMAIN_NAME_SUFFIX);
    container.addEnv("MYSQL_PROXY_PORT", Integer.toString(mySQLProxyPort));

    System.out.println("Toxyproxy Instances port: " + mySQLProxyPort);
    System.out.println("Instances Proxied: " + mySqlInstances.size());

    container.start();
    return container;
  }
}

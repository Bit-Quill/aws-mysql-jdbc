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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import software.aws.rds.jdbc.mysql.Driver;
import testsuite.integration.utility.ContainerHelper;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests against RDS Aurora cluster. The following environment variables
 * should be set. Provided values are just examples.
 *
 * Assuming cluster endpoint is "database-cluster-name.XYZ.us-east-2.rds.amazonaws.com"
 *
 * DB_CONN_STR_SUFFIX=.XYZ.us-east-2.rds.amazonaws.com   (pay attention on period in front of the value!!!)
 * TEST_DB_CLUSTER_IDENTIFIER=database-cluster-name
 * TEST_USERNAME=user-name
 * TEST_PASSWORD=user-secret-password
 */
public class AuroraIntegrationContainerTest {

  private static final int MYSQL_PORT = 3306;
  private static final String TEST_CONTAINER_NAME = "test-container";

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static final String DB_CONN_STR_PREFIX = "jdbc:mysql://";
  private static final String DB_CONN_STR_SUFFIX = System.getenv("DB_CONN_STR_SUFFIX");
  private static final String DB_CONN_PROP = "?enabledTLSProtocols=TLSv1.2";

  // Encounters SSL errors without it on GH Actions
  private static final String TEST_DB_CLUSTER_IDENTIFIER = System.getenv("TEST_DB_CLUSTER_IDENTIFIER");
  private static final String PROXIED_DOMAIN_NAME_SUFFIX = ".proxied";
  private static List<ToxiproxyContainer> proxyContainers = new ArrayList<>();
  private static List<String> mySqlInstances = new ArrayList<>();

  private static int mySQLProxyPort;
  private static GenericContainer<?> integrationTestContainer;
  private static String dbHostCluster = "";
  private static String dbHostClusterRo = "";

  private static Network network;

  private static final ContainerHelper containerHelper = new ContainerHelper();

  @BeforeAll
  static void setUp() throws SQLException {
    assertNotNull(DB_CONN_STR_SUFFIX, "DB_CONN_STR_SUFFIX should be set.");
    assertTrue(DB_CONN_STR_SUFFIX.startsWith("."), "DB_CONN_STR_SUFFIX should start with period.");
    assertNotNull(TEST_DB_CLUSTER_IDENTIFIER, "TEST_DB_CLUSTER_IDENTIFIER should be set.");
    assertNotNull(TEST_USERNAME, "TEST_USERNAME should be set.");
    assertNotNull(TEST_PASSWORD, "TEST_PASSWORD should be set.");

    final String dbConnHostBase =
        DB_CONN_STR_SUFFIX.startsWith(".")
            ? DB_CONN_STR_SUFFIX.substring(1)
            : DB_CONN_STR_SUFFIX;
    dbHostCluster = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-" + dbConnHostBase;
    dbHostClusterRo = TEST_DB_CLUSTER_IDENTIFIER + ".cluster-ro-" + dbConnHostBase;

    DriverManager.registerDriver(new Driver());

    network = Network.newNetwork();
    mySqlInstances = containerHelper.getAuroraClusterInstances(
            DB_CONN_STR_PREFIX + dbHostCluster + DB_CONN_PROP,
            TEST_USERNAME,
            TEST_PASSWORD,
            dbConnHostBase);
    proxyContainers = containerHelper.createProxyContainers(network, mySqlInstances, PROXIED_DOMAIN_NAME_SUFFIX);
    for(ToxiproxyContainer container : proxyContainers) {
      container.start();
    }
    mySQLProxyPort = containerHelper.createAuroraInstanceProxies(mySqlInstances, proxyContainers, MYSQL_PORT);

    proxyContainers.add(containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-instance-cluster",
            dbHostCluster + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostCluster,
            MYSQL_PORT,
            mySQLProxyPort)
    );

    proxyContainers.add(containerHelper.createAndStartProxyContainer(
            network,
            "toxiproxy-ro-instance-cluster",
            dbHostClusterRo + PROXIED_DOMAIN_NAME_SUFFIX,
            dbHostClusterRo,
            MYSQL_PORT,
            mySQLProxyPort)
    );

    integrationTestContainer = initializeTestContainer(network, mySqlInstances);
  }

  @AfterAll
  static void tearDown() {
    for (ToxiproxyContainer proxy : proxyContainers) {
      proxy.stop();
    }
    integrationTestContainer.stop();
  }

  @Test
  public void testRunTestInContainer()
    throws UnsupportedOperationException, IOException, InterruptedException, SQLException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora");
  }

  @Test
  public void testRunPerformanceTestInContainer()
          throws UnsupportedOperationException, IOException, InterruptedException, SQLException {

    containerHelper.runTest(integrationTestContainer, "in-container-aurora-performance");
  }

  protected static GenericContainer<?> initializeTestContainer(final Network network, List<String> mySqlInstances) {

    final GenericContainer<?> container = containerHelper.createTestContainer("bq/rds-test-container")
        .withNetworkAliases(TEST_CONTAINER_NAME)
        .withNetwork(network)
        .withEnv("TEST_USERNAME", TEST_USERNAME)
        .withEnv("TEST_PASSWORD", TEST_PASSWORD)
        .withEnv("DB_CLUSTER_CONN", dbHostCluster)
        .withEnv("DB_RO_CLUSTER_CONN", dbHostClusterRo)
        .withEnv("TOXIPROXY_CLUSTER_NETWORK_ALIAS", "toxiproxy-instance-cluster")
        .withEnv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS", "toxiproxy-ro-instance-cluster")
        .withEnv("PROXIED_CLUSTER_TEMPLATE", "?" + DB_CONN_STR_SUFFIX + PROXIED_DOMAIN_NAME_SUFFIX)
        .withEnv("DB_CONN_STR_SUFFIX", DB_CONN_STR_SUFFIX);

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

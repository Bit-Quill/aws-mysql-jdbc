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

package testsuite.integration.container;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.zaxxer.hikari.HikariPoolMXBean;
import eu.rekawek.toxiproxy.Proxy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HikariCPIntegrationTest extends AuroraMysqlIntegrationBaseTest {
  private static final String JDBC_URL = "jdbc:mysql:aws://" + MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX + ":" + MYSQL_PROXY_PORT;

  private static HikariDataSource data_source = null;

  @BeforeAll
  static void setup() throws IOException, SQLException {
    AuroraMysqlIntegrationBaseTest.setUp();

    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
    final HikariConfig config = new HikariConfig();

    config.setJdbcUrl(JDBC_URL);
    config.setUsername(TEST_USERNAME);
    config.setPassword(TEST_PASSWORD);
    config.setMaximumPoolSize(3);
    config.setReadOnly(true);
    config.setInitializationFailTimeout(75000);

    data_source = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = data_source.getHikariPoolMXBean();
    System.out.println("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    System.out.println("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    System.out.println("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  @AfterAll
  static void teardown() {
    data_source.close();
  }

  @BeforeEach
  public void disconnectCluster() {
    System.out.println("Putting down all instances before test");
    putDownAllInstances();
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable
   */
  @Test
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    bringUpAllInstances();

    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(3));

      putDownAllInstances();

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
      assertEquals("08001", exception.getSQLState());
      assertFalse(conn.isValid(3));
    }

    assertThrows(SQLTransientConnectionException.class, () -> data_source.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance
   */
  @Test
  public void test_1_2_hikariCP_basic_failover() throws SQLException, IOException {
    List<String> currentClusterTopology = getTopology();
    String startingEndpoint = (currentClusterTopology.size() > 1) ? currentClusterTopology.get(1) : null;
    String startingIdentifier = startingEndpoint.split("\\.")[0];
    bringUpInstance(startingIdentifier);
    containerHelper.enableConnectivity(proxyReadOnlyCluster);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(3));
      String readerEndpoint = (currentClusterTopology.size() > 2) ? currentClusterTopology.get(2) : null;
      String readerIdentifier = readerEndpoint.split("\\.")[0];
      bringUpInstance(readerIdentifier);
      putDownInstance(startingIdentifier);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      String currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
      assertTrue(currentInstance.equalsIgnoreCase(startingEndpoint));
      assertTrue(conn.isValid(3));
    }
  }

  /**
   * After a successful failover, a connection is retrieved to check that connections to failed instances are not
   * returned
   */
  @Test
  public void test_1_3_hikariCP_get_dead_connection() throws SQLException {
    List<String> currentClusterTopology = getTopology();
    String startingEndpoint = (currentClusterTopology.size() > 1) ? currentClusterTopology.get(1) : null;
    String startingIdentifier = startingEndpoint.split("\\.")[0];
    bringUpInstance(startingIdentifier);
    containerHelper.enableConnectivity(proxyReadOnlyCluster);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn1 = data_source.getConnection()) {
      assertTrue(conn1.isValid(3));
      String readerEndpoint = (currentClusterTopology.size() > 2) ? currentClusterTopology.get(2) : null;
      String readerIdentifier = readerEndpoint.split("\\.")[0];
      bringUpInstance(readerIdentifier);
      putDownInstance(startingEndpoint);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn1, QUERY_FOR_INSTANCE));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      assertTrue(conn1.isValid(3));
      String currentInstance = selectSingleRow(conn1, QUERY_FOR_INSTANCE);
      assertTrue(currentInstance.equalsIgnoreCase(startingIdentifier));

      // Get a new connection
      try (Connection conn2 = data_source.getConnection()) {
        assertTrue(conn2.isValid(3));
      }
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor
   */
  @Test
  public void test_2_1_hikariCP_efm_failover() {
    // Will be the same as 1_2, but with a query that takes longer, ex. SLEEP(10)
  }

  private void putDownInstance(String targetInstance) {
    Proxy toPutDown = proxyMap.get(targetInstance);
    disableInstanceConnection(toPutDown);
  }

  private void putDownAllInstances() {
    proxyMap.forEach((instance, proxy) -> {
      disableInstanceConnection(proxy);
    });
  }

  private void disableInstanceConnection(Proxy proxy) {
    try {
      containerHelper.disableConnectivity(proxy);
    } catch (IOException e) {
      fail("Couldn't disable proxy connectivity");
    }
  }

  private void bringUpInstance(String targetInstance) {
    Proxy toBringUp = proxyMap.get(targetInstance);
    containerHelper.enableConnectivity(toBringUp);
  }

  private void bringUpAllInstances() {
    proxyMap.forEach((instance, proxy) -> {
      containerHelper.enableConnectivity(proxy);
    });
  }
}

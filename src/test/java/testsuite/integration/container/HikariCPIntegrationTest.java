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

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.LogFactory;
import com.mysql.cj.log.StandardLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import eu.rekawek.toxiproxy.Proxy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


@TestMethodOrder(MethodOrderer.MethodName.class)
public class HikariCPIntegrationTest extends AuroraMysqlIntegrationBaseTest {

  private static Log log = null;
  private static final String JDBC_URL = "jdbc:mysql:aws://" + MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX + ":" + MYSQL_PROXY_PORT;
  private static HikariDataSource data_source = null;

  @BeforeAll
  static void setup() throws ClassNotFoundException, SQLException, IOException {
    Class.forName("software.aws.rds.jdbc.mysql.Driver");
    log = LogFactory.getLogger(StandardLogger.class.getName(), Log.LOGGER_INSTANCE_NAME);

    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");

    final HikariConfig config = new HikariConfig();

    config.setJdbcUrl(JDBC_URL);
    config.setUsername(TEST_USERNAME);
    config.setPassword(TEST_PASSWORD);
    config.setMaximumPoolSize(3);
    config.setReadOnly(true);
    config.setInitializationFailTimeout(75000);
    config.setConnectionTimeout(1000);
    config.addDataSourceProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "5000");
    config.addDataSourceProperty(PropertyKey.failoverReaderConnectTimeoutMs.getKeyName(), "1000");
    config.addDataSourceProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    config.addDataSourceProperty(PropertyKey.failureDetectionTime.getKeyName(), "3000");
    config.addDataSourceProperty(PropertyKey.failureDetectionInterval.getKeyName(), "1500");

    data_source = new HikariDataSource(config);

    final HikariPoolMXBean hikariPoolMXBean = data_source.getHikariPoolMXBean();

    log.logDebug("Starting idle connections: " + hikariPoolMXBean.getIdleConnections());
    log.logDebug("Starting active connections: " + hikariPoolMXBean.getActiveConnections());
    log.logDebug("Starting total connections: " + hikariPoolMXBean.getTotalConnections());
  }

  @AfterAll
  static void teardown() {
    data_source.close();
  }

  @Override
  @BeforeEach
  public void setUpEach() {
    putDownAllInstances(false);
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable
   */
  @Test
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    bringUpAllInstances();
    
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));

      putDownAllInstances(true);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
      assertEquals("08001", exception.getSQLState());
      assertFalse(conn.isValid(5));
    }

    assertThrows(SQLTransientConnectionException.class, () -> data_source.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance
   */
  @Test
  public void test_1_2_hikariCP_basic_failover() throws SQLException {
    putDownAllInstances(false);

    List<String> currentClusterTopology = getTopology();
    String writer = (currentClusterTopology.size() > 0) ? currentClusterTopology.get(0) : "";
    String reader = (currentClusterTopology.size() > 1) ? currentClusterTopology.get(1) : "";
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    log.logDebug("Instance to connect to: " + writerIdentifier);
    log.logDebug("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      conn.setReadOnly(true);
      assertTrue(conn.isValid(5));
      String currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
      log.logDebug("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));

      bringUpInstance(readerIdentifier);
      log.logDebug("Brought up " + readerIdentifier);
      putDownInstance(writerIdentifier);
      log.logDebug("Took down " + currentInstance);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      Connection temp = data_source.getConnection();
      currentInstance = selectSingleRow(temp, QUERY_FOR_INSTANCE);
      log.logDebug("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
      assertTrue(conn.isValid(5));

      putDownInstance(readerIdentifier);
    }
  }

  /**
   * After a successful failover, a connection is retrieved to check that connections to failed instances are not
   * returned
   */
  @Test
  public void test_1_3_hikariCP_get_dead_connection() throws SQLException {
    putDownAllInstances(false);

    List<String> currentClusterTopology = getTopology();
    String writer = (currentClusterTopology.size() > 0) ? currentClusterTopology.get(0) : "";
    String reader = (currentClusterTopology.size() > 1) ? currentClusterTopology.get(1) : "";
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    log.logDebug("Instance to connect to: " + writerIdentifier);
    log.logDebug("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      bringUpInstance(readerIdentifier);
      log.logDebug("Brought up " + readerIdentifier);
      putDownInstance(currentInstance);
      log.logDebug("Took down " + currentInstance);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
      log.logDebug("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
      assertTrue(conn.isValid(5));

      // Get a new connection
      try (Connection newConn = data_source.getConnection()) {
        assertTrue(newConn.isValid(3));
      }

      putDownInstance(readerIdentifier);
    }
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor
   */
  @Test
  public void test_2_1_hikariCP_efm_failover() throws SQLException {
    putDownAllInstances(false);

    List<String> currentClusterTopology = getTopology();
    String writer = (currentClusterTopology.size() > 0) ? currentClusterTopology.get(0) : "";
    String reader = (currentClusterTopology.size() > 1) ? currentClusterTopology.get(1) : "";
    String writerIdentifier = writer.split("\\.")[0];
    String readerIdentifier = reader.split("\\.")[0];
    log.logDebug("Instance to connect to: " + writerIdentifier);
    log.logDebug("Instance to fail over to: " + readerIdentifier);

    bringUpInstance(writerIdentifier);

    // Get a valid connection, then make it fail over to a different instance
    try (Connection conn = data_source.getConnection()) {
      assertTrue(conn.isValid(5));
      String currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
      assertTrue(currentInstance.equalsIgnoreCase(writerIdentifier));
      log.logDebug("Connected to instance: " + currentInstance);

      bringUpInstance(readerIdentifier);
      log.logDebug("Brought up " + writerIdentifier);
      putDownInstance(writerIdentifier);
      log.logDebug("Took down " + readerIdentifier);

      final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, "SELECT SLEEP(100)"));
      assertEquals("08S02", exception.getSQLState());

      // Check the connection is valid after connecting to a different instance
      currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
      log.logDebug("Connected to instance: " + currentInstance);
      assertTrue(currentInstance.equalsIgnoreCase(readerIdentifier));
      assertTrue(conn.isValid(5));

      putDownInstance(readerIdentifier);
    }
  }

  private void putDownInstance(String targetInstance) {
    Proxy toPutDown = proxyMap.get(targetInstance);
    disableInstanceConnection(toPutDown);
  }

  private void putDownAllInstances(Boolean putDownClusters) {
    log.logDebug("Putting down all instances");
    proxyMap.forEach((instance, proxy) -> {
      if (putDownClusters || (proxy != proxyCluster && proxy != proxyReadOnlyCluster)) {
        disableInstanceConnection(proxy);
      }
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

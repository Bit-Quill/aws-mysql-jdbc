package testsuite.integration.container;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HikariCPIntegrationTest extends AuroraMysqlIntegrationBaseTest {
  private static HikariDataSource DATA_SOURCE = null;
  private static final String jdbcUrl = "jdbc:mysql:aws://" + MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX + ":" + MYSQL_PROXY_PORT;

  @BeforeAll
  static void setup() throws IOException, SQLException {
    AuroraMysqlIntegrationBaseTest.setUp();

    System.setProperty("com.zaxxer.hikari.blockUntilFilled", "true");
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(jdbcUrl);
    config.setUsername(TEST_USERNAME);
    config.setPassword(TEST_PASSWORD);
    config.setMaximumPoolSize(3);
    config.setReadOnly(true);
    config.setInitializationFailTimeout(75000);

    DATA_SOURCE = new HikariDataSource(config);
    System.out.println("Starting idle connections: " + DATA_SOURCE.getHikariPoolMXBean().getIdleConnections());
    System.out.println("Starting active connections: " + DATA_SOURCE.getHikariPoolMXBean().getActiveConnections());
    System.out.println("Starting total connections: " + DATA_SOURCE.getHikariPoolMXBean().getTotalConnections());
  }

  @AfterAll
  static void teardown() {
    DATA_SOURCE.close();
  }

  @BeforeEach
  public void disconnectCluster() {
    System.out.println("Putting down all instances before test");
    putDownInstance(null);
  }

  /**
   * After getting successful connections from the pool, the cluster becomes unavailable
   */
  @Test
  public void test_1_1_hikariCP_lost_connection() throws SQLException {
    bringUpInstance(null);

    Connection conn = DATA_SOURCE.getConnection();
    assertTrue(conn.isValid(3));

    putDownInstance(null);

    final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
    assertEquals("08001", exception.getSQLState());
    assertFalse(conn.isValid(3));
    conn.close();

    assertThrows(SQLTransientConnectionException.class, () -> DATA_SOURCE.getConnection());
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance
   */
  @Test
  public void test_1_2_hikariCP_basic_failover() throws SQLException {
    List<String> currentClusterTopology = getTopology();
    String startingEndpoint = (currentClusterTopology.size() >= 1) ? currentClusterTopology.get(1) : null;
    bringUpInstance(startingEndpoint);
    containerHelper.enableConnectivity(proxyReadOnlyCluster);

    // Get a valid connection, then make it fail over to a different instance
    Connection conn = DATA_SOURCE.getConnection();
    assertTrue(conn.isValid(3));
    String readerEndpoint = (currentClusterTopology.size() >= 2) ? currentClusterTopology.get(2) : null;
    bringUpInstance(readerEndpoint);
    putDownInstance(startingEndpoint);

    final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn, QUERY_FOR_INSTANCE));
    assertEquals("08S02", exception.getSQLState());

    // Check the connection is valid after connecting to a different instance
    String currentInstance = selectSingleRow(conn, QUERY_FOR_INSTANCE);
    assertTrue(currentInstance.equalsIgnoreCase(startingEndpoint));
    assertTrue(conn.isValid(3));

    conn.close();
  }

  /**
   * After a successful failover, a connection is retrieved to check that connections to failed instances are not
   * returned
   */
  @Test
  public void test_1_3_hikariCP_get_dead_connection() throws SQLException {
    List<String> currentClusterTopology = getTopology();
    String startingEndpoint = (currentClusterTopology.size() >= 1) ? currentClusterTopology.get(1) : null;
    bringUpInstance(startingEndpoint);
    containerHelper.enableConnectivity(proxyReadOnlyCluster);

    // Get a valid connection, then make it fail over to a different instance
    Connection conn1 = DATA_SOURCE.getConnection();
    assertTrue(conn1.isValid(3));
    String readerEndpoint = (currentClusterTopology.size() >= 2) ? currentClusterTopology.get(2) : null;
    bringUpInstance(readerEndpoint);
    putDownInstance(startingEndpoint);

    final SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(conn1, QUERY_FOR_INSTANCE));
    assertEquals("08S02", exception.getSQLState());

    // Check the connection is valid after connecting to a different instance
    String currentInstance = selectSingleRow(conn1, QUERY_FOR_INSTANCE);
    assertTrue(currentInstance.equalsIgnoreCase(startingEndpoint));
    assertTrue(conn1.isValid(3));

    // Get a new connection
    Connection conn2 = DATA_SOURCE.getConnection();
    assertTrue(conn2.isValid(3));

    conn1.close();
    conn2.close();
  }

  /**
   * After getting a successful connection from the pool, the connected instance becomes unavailable and the
   * connection fails over to another instance through the Enhanced Failure Monitor
   */
  @Test
  public void test_2_1_hikariCP_efm_failover() {
    // Will be the same as 1_2, but with a query that takes longer, ex. SLEEP(10)
  }

  /**
   * Put down network for specified instance or all instances if given argument is null
   */
  private void putDownInstance(String targetInstance) {
    System.out.println("=============== Disable target instance1: " + targetInstance);
    proxyMap.forEach((instance, proxy) -> {
      if (targetInstance == null || targetInstance.toLowerCase().contains(instance.toLowerCase())) {
        try {
          if (proxy != null) {
            containerHelper.disableConnectivity(proxy);
          }
          else {
            System.out.println("=========== Proxy was null");
          }
        } catch (IOException e) {
          fail("Couldn't disable proxy connectivity");
        }
      }
    });
  }

  /**
   * Bring up network for specified instance or all instances if given argument is null
   */
  private void bringUpInstance(String targetInstance) {
    proxyMap.forEach((instance, proxy) -> {
      if (targetInstance == null || (targetInstance.toLowerCase().contains(instance.toLowerCase()))) {
        containerHelper.enableConnectivity(proxy);
      }
    });
  }
}

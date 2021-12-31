package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import software.aws.rds.jdbc.mysql.Driver;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class AuroraMySqlIntegrationTest {

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  private static final String TEST_DB_USER = System.getenv("TEST_DB_USER");

  private static final String PROXIED_DOMAIN_NAME_SUFFIX = System.getenv("PROXIED_DOMAIN_NAME_SUFFIX");
  private static final String PROXIED_CLUSTER_TEMPLATE = System.getenv("PROXIED_CLUSTER_TEMPLATE");

  private static final String MYSQL_INSTANCE_1_URL = System.getenv("MYSQL_INSTANCE_1_URL");
  private static final String MYSQL_INSTANCE_2_URL = System.getenv("MYSQL_INSTANCE_2_URL");
  private static final String MYSQL_INSTANCE_3_URL = System.getenv("MYSQL_INSTANCE_3_URL");
  private static final String MYSQL_INSTANCE_4_URL = System.getenv("MYSQL_INSTANCE_4_URL");
  private static final String MYSQL_INSTANCE_5_URL = System.getenv("MYSQL_INSTANCE_5_URL");
  private static final String MYSQL_CLUSTER_URL = System.getenv("DB_CLUSTER_CONN");
  private static final String MYSQL_RO_CLUSTER_URL = System.getenv("DB_RO_CLUSTER_CONN");

  private static final int MYSQL_PORT = Integer.parseInt(System.getenv("MYSQL_PORT"));
  private static final int MYSQL_PROXY_PORT = Integer.parseInt(System.getenv("MYSQL_PROXY_PORT"));

  private static final String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS");
  private static final String TOXIPROXY_CLUSTER_NETWORK_ALIAS = System.getenv("TOXIPROXY_CLUSTER_NETWORK_ALIAS");
  private static final String TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS = System.getenv("TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS");
  private static final int TOXIPROXY_CONTROL_PORT = 8474;

  private static ToxiproxyClient toxyproxyClientInstance_1;
  private static ToxiproxyClient toxyproxyClientInstance_2;
  private static ToxiproxyClient toxyproxyClientInstance_3;
  private static ToxiproxyClient toxyproxyClientInstance_4;
  private static ToxiproxyClient toxyproxyClientInstance_5;
  private static ToxiproxyClient toxyproxyCluster;
  private static ToxiproxyClient toxyproxyReadOnlyCluster;

  private static Proxy proxyInstance_1;
  private static Proxy proxyInstance_2;
  private static Proxy proxyInstance_3;
  private static Proxy proxyInstance_4;
  private static Proxy proxyInstance_5;
  private static Proxy proxyCluster;
  private static Proxy proxyReadOnlyCluster;
  private static final List<Proxy> proxyList = new ArrayList<>(5);
  private static final Map<String, Proxy> proxyMap = new HashMap<>(7);

  private static final int REPEAT_TIMES = 5;
  private static final List<Integer> downtimesDefault = new ArrayList<>(REPEAT_TIMES);
  private static final List<Integer> downtimesAggressive = new ArrayList<>(REPEAT_TIMES);

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    toxyproxyClientInstance_1 = new ToxiproxyClient(TOXIPROXY_INSTANCE_1_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_2 = new ToxiproxyClient(TOXIPROXY_INSTANCE_2_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_3 = new ToxiproxyClient(TOXIPROXY_INSTANCE_3_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_4 = new ToxiproxyClient(TOXIPROXY_INSTANCE_4_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_5 = new ToxiproxyClient(TOXIPROXY_INSTANCE_5_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyCluster = new ToxiproxyClient(TOXIPROXY_CLUSTER_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyReadOnlyCluster = new ToxiproxyClient(TOXIPROXY_RO_CLUSTER_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);

    proxyInstance_1 = getProxy(toxyproxyClientInstance_1, MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    proxyInstance_2 = getProxy(toxyproxyClientInstance_2, MYSQL_INSTANCE_2_URL, MYSQL_PORT);
    proxyInstance_3 = getProxy(toxyproxyClientInstance_3, MYSQL_INSTANCE_3_URL, MYSQL_PORT);
    proxyInstance_4 = getProxy(toxyproxyClientInstance_4, MYSQL_INSTANCE_4_URL, MYSQL_PORT);
    proxyInstance_5 = getProxy(toxyproxyClientInstance_5, MYSQL_INSTANCE_5_URL, MYSQL_PORT);
    proxyCluster = getProxy(toxyproxyCluster, MYSQL_CLUSTER_URL, MYSQL_PORT);
    proxyReadOnlyCluster = getProxy(toxyproxyReadOnlyCluster, MYSQL_RO_CLUSTER_URL, MYSQL_PORT);

    proxyMap.put(MYSQL_INSTANCE_1_URL.substring(0, MYSQL_INSTANCE_1_URL.indexOf('.')), proxyInstance_1);
    proxyMap.put(MYSQL_INSTANCE_2_URL.substring(0, MYSQL_INSTANCE_2_URL.indexOf('.')), proxyInstance_2);
    proxyMap.put(MYSQL_INSTANCE_3_URL.substring(0, MYSQL_INSTANCE_3_URL.indexOf('.')), proxyInstance_3);
    proxyMap.put(MYSQL_INSTANCE_4_URL.substring(0, MYSQL_INSTANCE_4_URL.indexOf('.')), proxyInstance_4);
    proxyMap.put(MYSQL_INSTANCE_5_URL.substring(0, MYSQL_INSTANCE_5_URL.indexOf('.')), proxyInstance_5);
    proxyMap.put(MYSQL_CLUSTER_URL, proxyCluster);
    proxyMap.put(MYSQL_RO_CLUSTER_URL, proxyReadOnlyCluster);

    proxyList.add(proxyInstance_1);
    proxyList.add(proxyInstance_2);
    proxyList.add(proxyInstance_3);
    proxyList.add(proxyInstance_4);
    proxyList.add(proxyInstance_5);

    DriverManager.registerDriver(new Driver());
  }

  private static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  @AfterAll
  public static void cleanUp() {
    final IntSummaryStatistics statSumDefault = downtimesDefault.stream().mapToInt(a -> a).summaryStatistics();
    final double avgDowntimeDefault = statSumDefault.getAverage();
    final long minDowntimeDefault = statSumDefault.getMin();
    final long maxDowntimeDefault = statSumDefault.getMax();

    final IntSummaryStatistics statSumAggressive = downtimesAggressive.stream().mapToInt(a -> a).summaryStatistics();
    final double avgDowntimeAggressive = statSumAggressive.getAverage();
    final long minDowntimeAggressive = statSumAggressive.getMin();
    final long maxDowntimeAggressive = statSumAggressive.getMax();

    System.out.printf("Failure Detection Results (in Milliseconds)\n" +
            "%11s| %-9s| %-90s| %-90s\n" +
            "%-11s| %-9d| %-9d| %-9.2f\n" +
            "%-11s| %-9d| %-9d| %-9.2f",
        "", "Min", "Max", "Avg",
        "Default", minDowntimeDefault, maxDowntimeDefault, avgDowntimeDefault,
        "Aggressive", minDowntimeAggressive, maxDowntimeAggressive, avgDowntimeAggressive);
  }

  @Test
  public void testConnectNotProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testConnectProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testClusterConnectNotProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_CLUSTER_URL, MYSQL_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testClusterConnectProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testROClusterConnectNotProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_RO_CLUSTER_URL, MYSQL_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testROClusterConnectProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Disabled
  @Test
  public void testValidateConnectionWhenNetworkDown() throws SQLException, IOException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));

    proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
    proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server

    assertFalse(conn.isValid(5));

    proxyInstance_1.toxics().get("DOWN-STREAM").remove();
    proxyInstance_1.toxics().get("UP-STREAM").remove();

    conn.close();
  }

  @Disabled
  @Test
  public void testConnectWhenNetworkDown() throws SQLException, IOException {
    proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
    proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server

    assertThrows(Exception.class, () -> {
      // expected to fail since communication is cut
      Connection tmp = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    });

    proxyInstance_1.toxics().get("DOWN-STREAM").remove();
    proxyInstance_1.toxics().get("UP-STREAM").remove();

    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  @Disabled
  @ParameterizedTest(name = "test_FailureDetectionTime")
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailureDetectionTime(Properties props, List<Integer> downtimes) {
    for(int i = 0; i < REPEAT_TIMES; i++) {
      try (final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props);
          final Statement statement = conn.createStatement()) {
        // Start timer to stop network
        final AtomicLong downtime = new AtomicLong();
        Thread thread = new Thread(() -> {
          try {
            Thread.sleep(5000); // 5s
            // Kill network
            proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
            proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
            downtime.set(System.currentTimeMillis());
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

        thread.start();
        // Execute long query
        try {
          final String QUERY = "select sleep(600)"; // 600s -> 10min
          final ResultSet result = statement.executeQuery(QUERY);
          Assertions.fail("Sleep query finished, should not be possible with network downed.");
        } catch (Exception throwables) { // Catching executing query
          // Calculate and add detection time
          downtimes.add((int)(System.currentTimeMillis() - downtime.get()));
        }

      } catch (Exception throwables) { // Catching Connection connect & Statement creations
        Assertions.fail(throwables);
      } finally {
        try {
          proxyInstance_1.toxics().get("DOWN-STREAM").remove();
          proxyInstance_1.toxics().get("UP-STREAM").remove();
        } catch (Exception e) {
          // Ignore as toxics were never set
        }
      }
    }
  }

  private static Stream<Arguments> generateFailureDetectionTimeParams() {
    final Properties defaultProps = new Properties();
    defaultProps.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    defaultProps.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    defaultProps.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    // Monitoring Props
    defaultProps.setProperty("monitoring-connectTimeout", "3000"); // 3s
    defaultProps.setProperty("monitoring-socketTimeout", "3000"); // 3s

    final Properties aggressiveProps = new Properties();
    defaultProps.forEach((key, value) -> aggressiveProps.setProperty((String) key, (String) value));
    aggressiveProps.setProperty(PropertyKey.failureDetectionTime.getKeyName(), "6000"); // 6s
    aggressiveProps.setProperty(PropertyKey.failureDetectionInterval.getKeyName(), "1000"); // 1s
    aggressiveProps.setProperty(PropertyKey.failureDetectionCount.getKeyName(), "1"); // 1

    return Stream.of(
        Arguments.of(defaultProps, downtimesDefault),
        Arguments.of(aggressiveProps, downtimesAggressive)
    );
  }

  // Don't think this works like I want it to
  // I think this needs to actually restart the server
  @Disabled
  @Test
  @Timeout(3 * 60000)
  public void test_ReplicationFailover() throws SQLException {
    final String DB_CONN_STR_PREFIX = "jdbc:mysql:replication://";
    final List<String> endpoints = new ArrayList<>();
    endpoints.add(MYSQL_INSTANCE_1_URL); // Writer
    endpoints.add(MYSQL_INSTANCE_2_URL);
    endpoints.add(MYSQL_INSTANCE_3_URL);
    endpoints.add(MYSQL_INSTANCE_4_URL);
    endpoints.add(MYSQL_INSTANCE_5_URL);

    final String initialWriterInstance = MYSQL_INSTANCE_1_URL.substring(0, MYSQL_INSTANCE_1_URL.indexOf('.'));
    String replicationHosts = getReplicationHosts(endpoints);

    Connection testConnection =
        DriverManager.getConnection(DB_CONN_STR_PREFIX + replicationHosts,
            TEST_USERNAME,
            TEST_PASSWORD);

    // Switch to a replica
    testConnection.setReadOnly(true);
    String replicaInstance = queryInstanceId(testConnection);
    assertNotEquals(initialWriterInstance, replicaInstance);

    // Mimic reboot to replicaInstance
    // I don't think this works
    // I think this needs to actually restart the server
    Thread thread = new Thread(() -> {
      try {
        // Kill network
        Thread.sleep(5000); // Sleep 5s
        proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
        proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
        Thread.sleep(5000); // Sleep 5s
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    thread.start();

    try {
      while (true) {
        queryInstanceId(testConnection);
      }
    } catch (SQLException e) {
      // do nothing
    }

    // Assert that we are connected to the new replica after failover happens.
    final String newInstance = queryInstanceId(testConnection);
    assertNotEquals(newInstance, replicaInstance);
    assertNotEquals(newInstance, initialWriterInstance);
  }

  private String getReplicationHosts(List<String> endpoints) {
    StringBuilder hostsStringBuilder = new StringBuilder();
    int numHosts = 3; // Why 3?
    for (int i = 0; i < numHosts; i++) {
      hostsStringBuilder.append(endpoints.get(i)).append(PROXIED_DOMAIN_NAME_SUFFIX);
      if (i < numHosts - 1) {
        hostsStringBuilder.append(",");
      }
    }
    return hostsStringBuilder.toString();
  }

  private String queryInstanceId(Connection connection) throws SQLException {
    try (Statement myStmt = connection.createStatement();
        ResultSet resultSet = myStmt.executeQuery("select @@aurora_server_id")
    ) {
      if (resultSet.next()) {
        return resultSet.getString("@@aurora_server_id");
      }
    }
    throw new SQLException();
  }

  private static String currWriter;
  private static String currReader;
  @Test
  public void test_LostConnectionToWriter() {
    // Connect to cluster
    try (Connection testConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get writer
      currWriter = selectSingleRow(testConnection, "SELECT @@aurora_server_id");

      // Put cluster & writer down
      Proxy proxyInstance = proxyMap.get(currWriter);
      if (proxyInstance != null) {
        proxyInstance.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0);
        proxyInstance.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0);
      } else {
        Assertions.fail(String.format("%s does not have a proxy setup.", currWriter));
      }
      proxyCluster.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
      proxyCluster.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql serve

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08001", exception.getSQLState());

      String newWriter = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      System.out.println("New reader: " + newWriter);
    } catch (Exception e) {
      Assertions.fail(e);
    } finally {
      try {
        Proxy proxyInstance = proxyMap.get(currWriter);
        proxyInstance.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0);
        proxyInstance.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0);
        proxyCluster.toxics().get("DOWN-STREAM").remove();
        proxyCluster.toxics().get("UP-STREAM").remove();
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Disabled
  @Test
  public void test_LostConnectionToReader() {
    // Connect to cluster
    try (Connection testConnection = connectToInstance(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");

      // Put cluster & reader down
      Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        proxyInstance.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0);
        proxyInstance.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0);
      } else {
        Assertions.fail(String.format("%s does not have a proxy setup.", currReader));
      }
      proxyCluster.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
      proxyCluster.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql serve

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      String newReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      System.out.println("New reader: " + newReader);
    } catch (Exception e) {
      Assertions.fail(e);
    } finally {
      try {
        Proxy proxyInstance = proxyMap.get(currWriter);
        proxyInstance.toxics().get("DOWN-STREAM").remove();
        proxyInstance.toxics().get("UP-STREAM").remove();
        proxyCluster.toxics().get("DOWN-STREAM").remove();
        proxyCluster.toxics().get("UP-STREAM").remove();
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Disabled
  @Test
  public void test_LostConnectionToAllReaders() {
    try (Connection checkWriterConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, "SELECT @@aurora_server_id");
    } catch (Exception e) {
      fail(e);
    }

    // Connect to cluster
    try (Connection testConnection = connectToInstance(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");

      // Put all but writer down
      proxyMap.forEach((k, v) -> {
        if (!k.equalsIgnoreCase(currWriter)) {
          try {
            v.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
            v.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql serve
          } catch (Exception e) {
            fail();
          }
        }
      });
      proxyCluster.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
      proxyCluster.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql serve

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      String newReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      System.out.println("New reader: " + newReader);
    } catch (Exception e) {
      Assertions.fail(e);
      e.printStackTrace();
    } finally {
      try {
        proxyMap.forEach((k, v) -> {
          try {
            v.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
            v.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql serve
          } catch (Exception e) {
            // Ignore as toxics were not set
          }
        });
        proxyCluster.toxics().get("DOWN-STREAM").remove();
        proxyCluster.toxics().get("UP-STREAM").remove();
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
    }
  }

  private String selectSingleRow(Connection connection, String sql) throws SQLException {
    try (Statement myStmt = connection.createStatement();
        ResultSet result = myStmt.executeQuery(sql)) {
      if (result.next()) {
        return result.getString(1);
      }
      return null;
    }
  }

  @Disabled
  @ParameterizedTest(name = "test_InvalidAwsIamAuth")
  @MethodSource("generateInvalidAwsIam")
  public void test_InvalidAwsIamAuth(String user, String password) {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.USER.getKeyName(), user);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
    Assertions.assertThrows(
            SQLException.class,
            () -> connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)
    );
  }

  private static Stream<Arguments> generateInvalidAwsIam() {
    return Stream.of(
            Arguments.of("", TEST_PASSWORD),
            Arguments.of("INVALID_" + TEST_DB_USER, "")
    );
  }

  @Disabled
  @ParameterizedTest(name = "test_ValidAwsIamAuth")
  @MethodSource("generateValidAwsIam")
  public void testValidAwsIamAuth(String user, String password) {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.USER.getKeyName(), user);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
    Assertions.assertThrows(
        SQLException.class,
        () -> connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)
    );
  }

  private static Stream<Arguments> generateValidAwsIam() {
    return Stream.of(
        Arguments.of(TEST_DB_USER, TEST_PASSWORD),
        Arguments.of(TEST_DB_USER, ""),
        Arguments.of(TEST_DB_USER, null)
    );
  }

  @Disabled
  @Test
  void test_ValidInvalidValidConnections() throws SQLException {
    final Properties validProp = new Properties();
    validProp.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    validProp.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_USERNAME);
    final Connection validConn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, validProp);
    validConn.close();

    final Properties invalidProp = new Properties();
    validProp.setProperty(PropertyKey.USER.getKeyName(), "INVALID_" + TEST_USERNAME);
    validProp.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_USERNAME);
    Assertions.assertThrows(
            SQLException.class,
            () -> connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, invalidProp)
    );

    final Connection validConn2 = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, validProp);
    validConn2.close();
  }

  private static Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "5000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "5000");
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    return connectToInstance(instanceUrl, port, props);
  }

  private static Connection connectToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    return DriverManager.getConnection("jdbc:mysql:aws://" + instanceUrl + ":" + port + "/test?tcpKeepAlive=false", props);
  }
}

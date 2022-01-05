package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.CsvWriter;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.CsvWriterSettings;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
  private static final String TEST_DB = System.getenv("TEST_DB");

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
  private static final Map<String, Proxy> proxyMap = new HashMap<>(7);

  private static final int REPEAT_TIMES = 5;
  private static final List<List<Integer>> dataList = new ArrayList<>();

  private static String currWriter;
  private static String currReader;

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

    DriverManager.registerDriver(new Driver());
  }

  private static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  @AfterAll
  public static void cleanUp() {
    // TODO Put below into CSV format - look for CSV writer to use
    // Print for CSV Format
    // Title
    System.out.println("Failure Detection Results (in Milliseconds)");
    // Columns
    System.out.println("FailureDetectionGraceTime, FailureDetectionInterval, FailureDetectionCount, SleepDelayMS, "
        + "MinFailureDetectionTime, MaxFailureDetectionTime, AvgFailureDetectionTime");
    // Data
    for (List<Integer> data : dataList) {
      int detectionTime = data.get(0);
      int detectionInterval = data.get(1);
      int detectionCount = data.get(2);
      int sleepDelayMS = data.get(3);
      int min = data.get(4);
      int max = data.get(5);
      int avg = data.get(6);
      System.out.printf("%d, %d, %d, %d, %d, %d, %d\n",
          detectionTime, detectionInterval, detectionCount, sleepDelayMS, min, max, avg
      );
    }
  }

  @BeforeEach
  public void setUpEach() {
    proxyMap.forEach((k, v) -> {
      try {
        v.toxics().get("DOWN-STREAM").remove();
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
      try {
        v.toxics().get("UP-STREAM").remove();
      } catch (Exception e) {
        // Ignore
      }
    });
  }

  @ParameterizedTest(name = "test_ConnectionString")
  @MethodSource("generateConnectionString")
  public void test_ConnectionString(String connStr, int port) throws SQLException {
    Connection conn = connectToInstance(connStr, port);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  private static Stream<Arguments> generateConnectionString() {
    return Stream.of(
        Arguments.of(MYSQL_INSTANCE_1_URL, MYSQL_PORT),
        Arguments.of(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT),
        Arguments.of(MYSQL_CLUSTER_URL, MYSQL_PORT),
        Arguments.of(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT),
        Arguments.of(MYSQL_RO_CLUSTER_URL, MYSQL_PORT),
        Arguments.of(MYSQL_RO_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)
    );
  }

  @Test
  public void test_ValidateConnectionWhenNetworkDown() throws SQLException, IOException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));

    proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
    proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server

    assertFalse(conn.isValid(5));

    proxyInstance_1.toxics().get("DOWN-STREAM").remove();
    proxyInstance_1.toxics().get("UP-STREAM").remove();

    conn.close();
  }

  @Test
  public void test_ConnectWhenNetworkDown() throws SQLException, IOException {
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

  @ParameterizedTest
  @MethodSource("generateFailureDetectionTimeParams")
  public void test_FailureDetectionTime(int detectionTime, int detectionInterval, int detectionCount, int sleepDelayMS) {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "0");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "0");
    props.setProperty(PropertyKey.failureDetectionTime.getKeyName(), Integer.toString(detectionTime));
    props.setProperty(PropertyKey.failureDetectionInterval.getKeyName(), Integer.toString(detectionInterval));
    props.setProperty(PropertyKey.failureDetectionCount.getKeyName(), Integer.toString(detectionCount));
    props.setProperty(PropertyKey.enableClusterAwareFailover.getKeyName(), Boolean.FALSE.toString());

    final AtomicLong downtime = new AtomicLong();
    boolean hasFailConnect = false;
    List<Integer> avg = new ArrayList<>(REPEAT_TIMES);
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int i = 0; i < REPEAT_TIMES; i++) {
      downtime.set(0);
      // Thread to stop network
      final Thread thread = new Thread(() -> {
        try {
          Thread.sleep(sleepDelayMS);
          // Kill network
          proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
          proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
          downtime.set(System.currentTimeMillis());
        } catch (IOException ioException) {
          Assertions.fail("Toxics were already set, should not happen");
        } catch (InterruptedException interruptedException) {
          // Ignore, stop the thread
        }
      });
      try (final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props);
          final Statement statement = conn.createStatement()) {
        hasFailConnect = false;

        // Execute long query
        try {
          final String QUERY = "select sleep(600)"; // 600s -> 10min
          thread.start();
          final ResultSet result = statement.executeQuery(QUERY);
          Assertions.fail("Sleep query finished, should not be possible with network downed.");
        } catch (Exception throwables) { // Catching executing query
          // Calculate and add detection time
          int failureTime = (int)(System.currentTimeMillis() - downtime.get());
          avg.add(failureTime);
          min = Math.min(min, failureTime);
          max = Math.max(max, failureTime);
        }
      } catch (Exception throwables) { // Catching Connection connect & Statement creations
        if (hasFailConnect) {
          // Already failed trying to connect twice in a row
          Assertions.fail("Exception occurred twice when trying to connect or create statement");
        }
        i--; // Retry
        hasFailConnect = true;
      } finally {
        thread.interrupt(); // Ensure thread has stopped running
        try {
          proxyInstance_1.toxics().get("DOWN-STREAM").remove();
          proxyInstance_1.toxics().get("UP-STREAM").remove();
        } catch (Exception e) {
          // Ignore as toxics were never set
        }
      }
    }

    final Integer[] arr = {detectionTime, detectionInterval, detectionCount, sleepDelayMS, min, max, (int)avg.stream().mapToInt(a -> a).summaryStatistics().getAverage()};
    final List<Integer> data = new ArrayList<>(Arrays.asList(arr));
    dataList.add(data);
  }

  private static Stream<Arguments> generateFailureDetectionTimeParams() {
    // detectionTime, detectionInterval, detectionCount, sleepDelayMS
    return Stream.of(
        Arguments.of(30000, 5000, 3, 5000), // Defaults
        Arguments.of(6000, 1000, 1, 5000) // Aggressive
    );
  }

  @Test
  public void test_LostConnectionToWriter() {
    Properties props = initDefaultProps();
    props.setProperty(PropertyKey.failoverTimeoutMs.getKeyName(), "10000");

    String currWriter = "";
    // Connect to cluster
    try (Connection testConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT, props)) {
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
      proxyReadOnlyCluster.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
      proxyReadOnlyCluster.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql serve

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      String newReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
    } catch (Exception e) {
      Assertions.fail(e);
    } finally {
      try {
        Proxy proxyInstance = proxyMap.get(currReader);
        proxyInstance.toxics().get("DOWN-STREAM").remove();
        proxyInstance.toxics().get("UP-STREAM").remove();
        proxyReadOnlyCluster.toxics().get("DOWN-STREAM").remove();
        proxyReadOnlyCluster.toxics().get("UP-STREAM").remove();
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Test
  public void test_LostConnectionToAllReaders() {
    // Get Writer
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

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      String newReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      assertEquals(currWriter, newReader);
    } catch (Exception e) {
      Assertions.fail(e);
      e.printStackTrace();
    } finally {
      try {
        proxyMap.forEach((k, v) -> {
          try {
            v.toxics().get("DOWN-STREAM").remove();
            v.toxics().get("UP-STREAM").remove();
          } catch (Exception e) {
            // Ignore as toxics were not set
          }
        });
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Test
  public void test_LostConnectionToReaderInstance() {
    // Get Writer
    try (Connection checkWriterConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, "SELECT @@aurora_server_id");
    } catch (Exception e) {
      fail(e);
    }

    // Get instance URL not WRITER
    Set<String> readers = proxyMap.keySet();
    readers.remove(currWriter);
    String anyReader = readers.iterator().next();

    // Connect to instance
    String pattern = PROXIED_CLUSTER_TEMPLATE.replace("?", "%s");
    try (Connection testConnection = connectToInstance(String.format(pattern, anyReader), MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      assertEquals(anyReader, currReader);

      // Put down current reader
      Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        proxyInstance.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0);
        proxyInstance.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0);
      } else {
        Assertions.fail(String.format("%s does not have a proxy setup.", currReader));
      }

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      String newInstance = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      assertEquals(currWriter, newInstance);
    } catch (Exception e) {
      Assertions.fail(e);
      e.printStackTrace();
    } finally {
      try {
        proxyMap.forEach((k, v) -> {
          try {
            v.toxics().get("DOWN-STREAM").remove();
            v.toxics().get("UP-STREAM").remove();
          } catch (Exception e) {
            // Ignore as toxics were not set
          }
        });
      } catch (Exception e) {
        // Ignore as toxics were not set
      }
    }
  }

  @Test
  public void test_LostConnectionReadOnly() {
    // Get Writer
    try (Connection checkWriterConnection = connectToInstance(MYSQL_CLUSTER_URL + PROXIED_DOMAIN_NAME_SUFFIX, MYSQL_PROXY_PORT)) {
      currWriter = selectSingleRow(checkWriterConnection, "SELECT @@aurora_server_id");
    } catch (Exception e) {
      fail(e);
    }

    // Get instance URL not WRITER
    Set<String> readers = proxyMap.keySet();
    readers.remove(currWriter);
    String anyReader = readers.iterator().next();

    // Connect to instance
    String pattern = PROXIED_CLUSTER_TEMPLATE.replace("?", "%s");
    try (Connection testConnection = connectToInstance(String.format(pattern, anyReader), MYSQL_PROXY_PORT)) {
      // Get reader
      currReader = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      assertEquals(anyReader, currReader);

      testConnection.setReadOnly(true);

      // Put down current reader
      Proxy proxyInstance = proxyMap.get(currReader);
      if (proxyInstance != null) {
        proxyInstance.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0);
        proxyInstance.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0);
      } else {
        Assertions.fail(String.format("%s does not have a proxy setup.", currReader));
      }

      SQLException exception = assertThrows(SQLException.class, () -> selectSingleRow(testConnection, "SELECT '1'"));
      assertEquals("08S02", exception.getSQLState());

      String newInstance = selectSingleRow(testConnection, "SELECT @@aurora_server_id");
      assertNotEquals(currWriter, newInstance);
    } catch (Exception e) {
      Assertions.fail(e);
      e.printStackTrace();
    } finally {
      try {
        proxyMap.forEach((k, v) -> {
          try {
            v.toxics().get("DOWN-STREAM").remove();
          } catch (Exception e) {
            // Ignore as toxics were not set
          }
          try {
            v.toxics().get("UP-STREAM").remove();
          } catch (Exception e) {
            // Ignore as toxics were not set
          }
        });
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

  @Disabled // Won't work as no aws credentials in env
  @ParameterizedTest(name = "test_InvalidAwsIamAuth")
  @MethodSource("generateInvalidAwsIam")
  public void test_InvalidAwsIamAuth(String user, String password) {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.USER.getKeyName(), user);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
    Assertions.assertThrows(
          SQLException.class,
          () -> connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, props)
    );
  }

  private static Stream<Arguments> generateInvalidAwsIam() {
    return Stream.of(
        Arguments.of("", TEST_PASSWORD),
        Arguments.of("INVALID_" + TEST_DB_USER, "")
    );
  }

  @Disabled // Won't work as no aws credentials in env
  @ParameterizedTest(name = "test_ValidAwsIamAuth")
  @MethodSource("generateValidAwsIam")
  public void testValidAwsIamAuth(String user, String password) {
    final Properties props = initDefaultProps();
    props.setProperty(PropertyKey.useAwsIam.getKeyName(), Boolean.TRUE.toString());
    props.setProperty(PropertyKey.USER.getKeyName(), user);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
    Assertions.assertThrows(
        SQLException.class,
        () -> connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, props)
    );
  }

  private static Stream<Arguments> generateValidAwsIam() {
    return Stream.of(
        Arguments.of(TEST_DB_USER, TEST_PASSWORD),
        Arguments.of(TEST_DB_USER, ""),
        Arguments.of(TEST_DB_USER, " ")
    );
  }

  @Test
  void test_ValidInvalidValidConnections() throws SQLException {
    final Properties validProp = initDefaultProps();
    validProp.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    validProp.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    final Connection validConn = connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, validProp);
    validConn.close();

    final Properties invalidProp = initDefaultProps();
    invalidProp.setProperty(PropertyKey.USER.getKeyName(), "INVALID_" + TEST_USERNAME);
    invalidProp.setProperty(PropertyKey.PASSWORD.getKeyName(), "INVALID_" + TEST_PASSWORD);
    Assertions.assertThrows(
            SQLException.class,
            () -> connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, invalidProp)
    );

    final Connection validConn2 = connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT, validProp);
    validConn2.close();
  }

  private static Properties initDefaultProps() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), "FALSE");
    props.setProperty(PropertyKey.DBNAME.getKeyName(), TEST_DB);

    return props;
  }

  private static Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    return connectToInstance(instanceUrl, port, initDefaultProps());
  }

  private static Connection connectToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    return DriverManager.getConnection("jdbc:mysql:aws://" + instanceUrl + ":" + port, props);
  }
}


//  // Don't think this works like I want it to
//  // I think this needs to actually restart the server
//  @Disabled
//  @Test
//  @Timeout(3 * 60000)
//  public void test_ReplicationFailover() throws SQLException {
//    final String DB_CONN_STR_PREFIX = "jdbc:mysql:replication://";
//    final List<String> endpoints = new ArrayList<>();
//    endpoints.add(MYSQL_INSTANCE_1_URL); // Writer
//    endpoints.add(MYSQL_INSTANCE_2_URL);
//    endpoints.add(MYSQL_INSTANCE_3_URL);
//    endpoints.add(MYSQL_INSTANCE_4_URL);
//    endpoints.add(MYSQL_INSTANCE_5_URL);
//
//    final String initialWriterInstance = MYSQL_INSTANCE_1_URL.substring(0, MYSQL_INSTANCE_1_URL.indexOf('.'));
//    String replicationHosts = getReplicationHosts(endpoints);
//
//    Connection testConnection =
//        DriverManager.getConnection(DB_CONN_STR_PREFIX + replicationHosts,
//            TEST_USERNAME,
//            TEST_PASSWORD);
//
//    // Switch to a replica
//    testConnection.setReadOnly(true);
//    String replicaInstance = queryInstanceId(testConnection);
//    assertNotEquals(initialWriterInstance, replicaInstance);
//
//    // Mimic reboot to replicaInstance
//    // I don't think this works
//    // I think this needs to actually restart the server
//    Thread thread = new Thread(() -> {
//      try {
//        // Kill network
//        Thread.sleep(5000); // Sleep 5s
//        proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
//        proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
//        Thread.sleep(5000); // Sleep 5s
//      } catch (Exception e) {
//        e.printStackTrace();
//      }
//    });
//    thread.start();
//
//    try {
//      while (true) {
//        queryInstanceId(testConnection);
//      }
//    } catch (SQLException e) {
//      // do nothing
//    }
//
//    // Assert that we are connected to the new replica after failover happens.
//    final String newInstance = queryInstanceId(testConnection);
//    assertNotEquals(newInstance, replicaInstance);
//    assertNotEquals(newInstance, initialWriterInstance);
//  }
//
//  private String getReplicationHosts(List<String> endpoints) {
//    StringBuilder hostsStringBuilder = new StringBuilder();
//    int numHosts = 3; // Why 3?
//    for (int i = 0; i < numHosts; i++) {
//      hostsStringBuilder.append(endpoints.get(i)).append(PROXIED_DOMAIN_NAME_SUFFIX);
//      if (i < numHosts - 1) {
//        hostsStringBuilder.append(",");
//      }
//    }
//    return hostsStringBuilder.toString();
//  }
//
//  private String queryInstanceId(Connection connection) throws SQLException {
//    try (Statement myStmt = connection.createStatement();
//        ResultSet resultSet = myStmt.executeQuery("select @@aurora_server_id")
//    ) {
//      if (resultSet.next()) {
//        return resultSet.getString("@@aurora_server_id");
//      }
//    }
//    throw new SQLException();
//  }

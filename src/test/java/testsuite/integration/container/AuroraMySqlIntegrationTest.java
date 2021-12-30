package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import software.aws.rds.jdbc.mysql.Driver;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AuroraMySqlIntegrationTest {

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static final String PROXIED_DOMAIN_NAME_PREFIX = System.getenv("PROXIED_DOMAIN_NAME_PREFIX");
  private static final String PROXIED_CLUSTER_TEMPLATE = System.getenv("PROXIED_CLUSTER_TEMPLATE");

  private static final String MYSQL_INSTANCE_1_URL = System.getenv("MYSQL_INSTANCE_1_URL");
  private static final String MYSQL_INSTANCE_2_URL = System.getenv("MYSQL_INSTANCE_2_URL");
  private static final String MYSQL_INSTANCE_3_URL = System.getenv("MYSQL_INSTANCE_3_URL");
  private static final String MYSQL_INSTANCE_4_URL = System.getenv("MYSQL_INSTANCE_4_URL");
  private static final String MYSQL_INSTANCE_5_URL = System.getenv("MYSQL_INSTANCE_5_URL");

  private static final int MYSQL_PORT = Integer.parseInt(System.getenv("MYSQL_PORT"));
  private static final int MYSQL_PROXY_PORT = Integer.parseInt(System.getenv("MYSQL_PROXY_PORT"));

  private static final String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS");
  private static final String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS");
  private static final int TOXIPROXY_CONTROL_PORT = 8474;

  private static ToxiproxyClient toxyproxyClientInstance_1;
  private static ToxiproxyClient toxyproxyClientInstance_2;
  private static ToxiproxyClient toxyproxyClientInstance_3;
  private static ToxiproxyClient toxyproxyClientInstance_4;
  private static ToxiproxyClient toxyproxyClientInstance_5;

  private static Proxy proxyInstance_1;
  private static Proxy proxyInstance_2;
  private static Proxy proxyInstance_3;
  private static Proxy proxyInstance_4;
  private static Proxy proxyInstance_5;

  private static final int REPEAT_TIMES = 5;
  private static final List<Long> workflow2DefaultDowntimes = new ArrayList<>(REPEAT_TIMES);
  private static final List<Long> workflow2AggressiveDowntimes = new ArrayList<>(REPEAT_TIMES);

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    toxyproxyClientInstance_1 = new ToxiproxyClient(TOXIPROXY_INSTANCE_1_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
//    toxyproxyClientInstance_2 = new ToxiproxyClient(TOXIPROXY_INSTANCE_2_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
//    toxyproxyClientInstance_3 = new ToxiproxyClient(TOXIPROXY_INSTANCE_3_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
//    toxyproxyClientInstance_4 = new ToxiproxyClient(TOXIPROXY_INSTANCE_4_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
//    toxyproxyClientInstance_5 = new ToxiproxyClient(TOXIPROXY_INSTANCE_5_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);

    proxyInstance_1 = getProxy(toxyproxyClientInstance_1, MYSQL_INSTANCE_1_URL, MYSQL_PORT);
//    proxyInstance_2 = getProxy(toxyproxyClientInstance_2, MYSQL_INSTANCE_2_URL, MYSQL_PORT);
//    proxyInstance_3 = getProxy(toxyproxyClientInstance_3, MYSQL_INSTANCE_3_URL, MYSQL_PORT);
//    proxyInstance_4 = getProxy(toxyproxyClientInstance_4, MYSQL_INSTANCE_4_URL, MYSQL_PORT);
//    proxyInstance_5 = getProxy(toxyproxyClientInstance_5, MYSQL_INSTANCE_5_URL, MYSQL_PORT);

    DriverManager.registerDriver(new Driver());
  }

  private static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    String upstream = host + ":" + port;
    return proxyClient.getProxy(upstream);
  }

  @AfterAll
  public static void cleanUp() {
    final double avgWorkflow2DefaultDowntimes = workflow2DefaultDowntimes.stream().mapToDouble(a -> a).average().orElse(0);
    final double avgWorkflow2AggressiveDowntimes = workflow2AggressiveDowntimes.stream().mapToDouble(a -> a).average().orElse(0);

    System.out.printf("Results\n%-50s%5.2fms\n%-50s%5.2fms",
        "Average Failure Detection, Default Properties:",
        avgWorkflow2DefaultDowntimes,
        "Average Failure Detection, Aggressive Properties:",
        avgWorkflow2AggressiveDowntimes);
  }

  @Test
  public void testConnectNotProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testConnectProxied() throws SQLException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testValidateConnectionWhenNetworkDown() throws SQLException, IOException {
    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));

    proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
    proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server

    assertFalse(conn.isValid(5));

    proxyInstance_1.toxics().get("DOWN-STREAM").remove();
    proxyInstance_1.toxics().get("UP-STREAM").remove();

    conn.close();
  }

  @Test
  public void testConnectWhenNetworkDown() throws SQLException, IOException {
    proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
    proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server

    assertThrows(Exception.class, () -> {
      // expected to fail since communication is cut
      Connection tmp = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    });

    proxyInstance_1.toxics().get("DOWN-STREAM").remove();
    proxyInstance_1.toxics().get("UP-STREAM").remove();

    Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  @RepeatedTest(REPEAT_TIMES)
  @MethodSource("generateProperties")
  public void test_workflow2_Default() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    // Monitoring Props
    props.setProperty("monitoring-connectTimeout", "3000"); // 3s
    props.setProperty("monitoring-socketTimeout", "3000"); // 3s

    try (final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT, props);
        final Statement statement = conn.createStatement()) {
      // Start timer to stop network
      final long[] downtime = {0};
      Thread thread = new Thread(() -> {
        try {
          Thread.sleep(5000); // 5s
          // Kill network
          proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
          proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
          downtime[0] = System.currentTimeMillis();
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
        workflow2DefaultDowntimes.add(System.currentTimeMillis() - downtime[0]);
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

  @RepeatedTest(REPEAT_TIMES)
  public void test_workflow2_Aggressive() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.clusterInstanceHostPattern.getKeyName(), PROXIED_CLUSTER_TEMPLATE);
    // Monitoring Props
    props.setProperty(PropertyKey.failureDetectionTime.getKeyName(), "6000"); // 6s
    props.setProperty(PropertyKey.failureDetectionInterval.getKeyName(), "1000"); // 1s
    props.setProperty(PropertyKey.failureDetectionCount.getKeyName(), "1"); // 1
    props.setProperty("monitoring-connectTimeout", "3000"); // 3s
    props.setProperty("monitoring-socketTimeout", "3000"); // 3s

    try (final Connection conn = connectToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT, props);
        final Statement statement = conn.createStatement()) {
      // Start timer to stop network
      final long[] downtime = {0};
      Thread thread = new Thread(() -> {
        try {
          Thread.sleep(5000); // 5s
          // Kill network
          proxyInstance_1.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from mysql server towards mysql driver
          proxyInstance_1.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from mysql driver towards mysql server
          downtime[0] = System.currentTimeMillis();
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
        workflow2AggressiveDowntimes.add(System.currentTimeMillis() - downtime[0]);
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

  private static Connection connectToInstance(String instanceUrl, int port) throws SQLException {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3000");
    return connectToInstance(instanceUrl, port, props);
  }

  private static Connection connectToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    return DriverManager.getConnection("jdbc:mysql:aws://" + instanceUrl + ":" + port + "/test?tcpKeepAlive=false", props);
  }
}

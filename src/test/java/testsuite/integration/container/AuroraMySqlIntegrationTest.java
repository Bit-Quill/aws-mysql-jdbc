package testsuite.integration.container;

import com.mysql.cj.conf.PropertyKey;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import software.aws.rds.jdbc.mysql.Driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class AuroraMySqlIntegrationTest {

  private static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  private static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");

  private static String PROXIED_DOMAIN_NAME_PREFIX = System.getenv("PROXIED_DOMAIN_NAME_PREFIX");

  private static String MYSQL_INSTANCE_1_URL = System.getenv("MYSQL_INSTANCE_1_URL");
  private static String MYSQL_INSTANCE_2_URL = System.getenv("MYSQL_INSTANCE_2_URL");
  private static String MYSQL_INSTANCE_3_URL = System.getenv("MYSQL_INSTANCE_3_URL");
  private static String MYSQL_INSTANCE_4_URL = System.getenv("MYSQL_INSTANCE_4_URL");
  private static String MYSQL_INSTANCE_5_URL = System.getenv("MYSQL_INSTANCE_5_URL");

  private static int MYSQL_PORT = Integer.parseInt(System.getenv("MYSQL_PORT"));
  private static int MYSQL_PROXY_PORT = Integer.parseInt(System.getenv("MYSQL_PROXY_PORT"));

  private static String TOXIPROXY_INSTANCE_1_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_1_NETWORK_ALIAS");
  private static String TOXIPROXY_INSTANCE_2_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_2_NETWORK_ALIAS");
  private static String TOXIPROXY_INSTANCE_3_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_3_NETWORK_ALIAS");
  private static String TOXIPROXY_INSTANCE_4_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_4_NETWORK_ALIAS");
  private static String TOXIPROXY_INSTANCE_5_NETWORK_ALIAS = System.getenv("TOXIPROXY_INSTANCE_5_NETWORK_ALIAS");
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

  @BeforeAll
  public static void setUp() throws IOException, SQLException {
    toxyproxyClientInstance_1 = new ToxiproxyClient(TOXIPROXY_INSTANCE_1_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_2 = new ToxiproxyClient(TOXIPROXY_INSTANCE_2_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_3 = new ToxiproxyClient(TOXIPROXY_INSTANCE_3_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_4 = new ToxiproxyClient(TOXIPROXY_INSTANCE_4_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
    toxyproxyClientInstance_5 = new ToxiproxyClient(TOXIPROXY_INSTANCE_5_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);

    proxyInstance_1 = getProxy(toxyproxyClientInstance_1, MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    proxyInstance_2 = getProxy(toxyproxyClientInstance_2, MYSQL_INSTANCE_2_URL, MYSQL_PORT);
    proxyInstance_3 = getProxy(toxyproxyClientInstance_3, MYSQL_INSTANCE_3_URL, MYSQL_PORT);
    proxyInstance_4 = getProxy(toxyproxyClientInstance_4, MYSQL_INSTANCE_4_URL, MYSQL_PORT);
    proxyInstance_5 = getProxy(toxyproxyClientInstance_5, MYSQL_INSTANCE_5_URL, MYSQL_PORT);

    DriverManager.registerDriver(new Driver());
  }

  private static Proxy getProxy(ToxiproxyClient proxyClient, String host, int port) throws IOException {
    String upstream = host + ":" + port;
    Proxy proxy = proxyClient.getProxy(upstream);
    return proxy;
  }

  @Test
  public void testConnectNotProxied() throws SQLException {
    Connection conn = connectoToInstance(MYSQL_INSTANCE_1_URL, MYSQL_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testConnectProxied() throws SQLException {
    Connection conn = connectoToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    assertTrue(conn.isValid(5));
    conn.close();
  }

  @Test
  public void testValidateConnectionWhenNetworkDown() throws SQLException, IOException {
    Connection conn = connectoToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
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
      Connection tmp = connectoToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    });

    proxyInstance_1.toxics().get("DOWN-STREAM").remove();
    proxyInstance_1.toxics().get("UP-STREAM").remove();

    Connection conn = connectoToInstance(MYSQL_INSTANCE_1_URL + PROXIED_DOMAIN_NAME_PREFIX, MYSQL_PROXY_PORT);
    conn.close();
  }

  private static Connection connectoToInstance(String instanceUrl, int port) throws SQLException {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), TEST_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), TEST_PASSWORD);
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), Integer.toString(5000));
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), Integer.toString(5000));
    return connectoToInstance(instanceUrl, port, props);
  }

  private static Connection connectoToInstance(String instanceUrl, int port, Properties props) throws SQLException {
    return DriverManager.getConnection("jdbc:mysql:aws://" + instanceUrl + ":" + port + "/test", props);
  }
}

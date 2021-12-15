package testsuite.integration.container;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ToxiproxyContainer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RedisIntegrationTest {

  private static String REDIS_A_NETWORK_ALIAS = System.getenv("REDIS_A_NETWORK_ALIAS");
  private static int REDIS_A_PORT = Integer.parseInt(System.getenv("REDIS_A_PORT"));

  private static String REDIS_B_NETWORK_ALIAS = System.getenv("REDIS_B_NETWORK_ALIAS");
  private static int REDIS_B_PORT = Integer.parseInt(System.getenv("REDIS_B_PORT"));

  private static int REDIS_PROXY_PORT = Integer.parseInt(System.getenv("REDIS_PROXY_PORT"));

  private static String TOXIPROXY_NETWORK_ALIAS = System.getenv("TOXIPROXY_NETWORK_ALIAS");
  private static final int TOXIPROXY_CONTROL_PORT = 8474;

  private static ToxiproxyClient toxyproxyClient;

  @BeforeAll
  public static void setUp() {
    toxyproxyClient = new ToxiproxyClient(TOXIPROXY_NETWORK_ALIAS, TOXIPROXY_CONTROL_PORT);
  }

  @Test
  public void testSimplePutAndGet() {
    Jedis jedis_a = new Jedis(REDIS_A_NETWORK_ALIAS, REDIS_A_PORT);
    Jedis jedis_b = new Jedis(REDIS_B_NETWORK_ALIAS, REDIS_B_PORT);

    jedis_a.set("test", "example-a");
    jedis_b.set("test", "example-b");

    String retrieved_a = jedis_a.get("test");
    assertEquals("example-a", retrieved_a);

    String retrieved_b = jedis_b.get("test");
    assertEquals("example-b", retrieved_b);
  }

  @Test
  public void testNetworkIssue() throws IOException {
    Proxy proxy = getProxy(REDIS_A_NETWORK_ALIAS, REDIS_A_PORT, REDIS_PROXY_PORT);

    Jedis jedis_a = new Jedis(TOXIPROXY_NETWORK_ALIAS, REDIS_PROXY_PORT); // via proxy
    Jedis jedis_b = new Jedis(REDIS_B_NETWORK_ALIAS, REDIS_B_PORT);

    jedis_a.set("test", "example-a");
    jedis_b.set("test", "example-b");

    String retrieved_a = jedis_a.get("test");
    assertEquals("example-a", retrieved_a);

    String retrieved_b = jedis_b.get("test");
    assertEquals("example-b", retrieved_b);

    proxy.toxics().bandwidth("DOWN-STREAM", ToxicDirection.DOWNSTREAM, 0); // from redis server towards redis client
    proxy.toxics().bandwidth("UP-STREAM", ToxicDirection.UPSTREAM, 0); // from redis client towards redis server

    assertThrows(
      JedisConnectionException.class, () -> {
        jedis_a.get("test");
      }, "calls fail when the connection is cut");

    proxy.toxics().get("DOWN-STREAM").remove();
    proxy.toxics().get("UP-STREAM").remove();
  }

  private Proxy getProxy(String host, int port, int proxyPort) throws IOException {
    String upstream = host + ":" + port;
    Proxy proxy = toxyproxyClient.getProxy(upstream);
    return proxy;
  }
}

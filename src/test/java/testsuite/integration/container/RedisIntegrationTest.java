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

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RedisIntegrationTest {

  private static final String REDIS_A_NETWORK_ALIAS = System.getenv("REDIS_A_NETWORK_ALIAS");
  private static final int REDIS_A_PORT = Integer.parseInt(System.getenv("REDIS_A_PORT"));

  private static final String REDIS_B_NETWORK_ALIAS = System.getenv("REDIS_B_NETWORK_ALIAS");
  private static final int REDIS_B_PORT = Integer.parseInt(System.getenv("REDIS_B_PORT"));

  private static final int REDIS_PROXY_PORT = Integer.parseInt(System.getenv("REDIS_PROXY_PORT"));

  private static final String TOXIPROXY_NETWORK_ALIAS = System.getenv("TOXIPROXY_NETWORK_ALIAS");
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
    return toxyproxyClient.getProxy(upstream);
  }
}

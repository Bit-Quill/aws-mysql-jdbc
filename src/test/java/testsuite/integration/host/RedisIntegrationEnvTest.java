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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.*;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;

import java.io.IOException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisIntegrationEnvTest {

  private static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";

  private static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";
  private static final String REDIS_A_NETWORK_ALIAS = "redis-a";
  private static final String REDIS_B_NETWORK_ALIAS = "redis-b";
  private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

  private static final int REDIS_PORT = 6379;
  private static int REDIS_PROXY_PORT;

  private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:5.0.4");
  private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("shopify/toxiproxy:2.1.0");

  private static GenericContainer<?> testContainer;
  private static GenericContainer<?> redisContainer_A;
  private static GenericContainer<?> redisContainer_B;
  private static ToxiproxyContainer toxiproxyContainer;

  @BeforeAll
  public static void setUp() {
    Network network = Network.newNetwork();

    redisContainer_A = new GenericContainer<>(REDIS_IMAGE)
      .withExposedPorts(REDIS_PORT)
      .withNetworkAliases(REDIS_A_NETWORK_ALIAS)
      .withNetwork(network);
    redisContainer_A.start();
    System.out.println("Redis A container name: " + redisContainer_A.getContainerName());

    redisContainer_B = new GenericContainer<>(REDIS_IMAGE)
      .withExposedPorts(REDIS_PORT)
      .withNetworkAliases(REDIS_B_NETWORK_ALIAS)
      .withNetwork(network);
    redisContainer_B.start();
    System.out.println("Redis B container name: " + redisContainer_B.getContainerName());

    toxiproxyContainer = new ToxiproxyContainer(TOXIPROXY_IMAGE)
      .withNetwork(network)
      .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
    toxiproxyContainer.start();
    System.out.println("Toxyproxy container name: " + toxiproxyContainer.getContainerName());

    ToxiproxyContainer.ContainerProxy proxy_a = toxiproxyContainer.getProxy(REDIS_A_NETWORK_ALIAS, REDIS_PORT);
    REDIS_PROXY_PORT = proxy_a.getOriginalProxyPort();

    testContainer = new GenericContainer<>(
      new ImageFromDockerfile("bq/rds-test-container", true)
        .withDockerfileFromBuilder(builder ->
          builder
            .from(TEST_CONTAINER_IMAGE_NAME)
            .run("mkdir", "app")
            .workDir("/app")
            .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
            .build()))
      .withNetworkAliases(TEST_CONTAINER_NETWORK_ALIAS)
      .withNetwork(network)
      .withFileSystemBind("./.git", "/app/.git", BindMode.READ_WRITE)
      .withFileSystemBind("./build", "/app/build", BindMode.READ_WRITE)
      .withFileSystemBind("./config", "/app/config", BindMode.READ_WRITE)
      .withFileSystemBind("./docs", "/app/docs", BindMode.READ_WRITE)
      .withFileSystemBind("./src", "/app/src", BindMode.READ_WRITE)
      .withFileSystemBind("./gradle", "/app/gradle", BindMode.READ_WRITE)
      .withCopyFileToContainer(MountableFile.forHostPath("./gradlew"), "app/gradlew")
      .withCopyFileToContainer(MountableFile.forHostPath("./gradle.properties"), "app/gradle.properties")
      .withCopyFileToContainer(MountableFile.forHostPath("./build.gradle.kts"), "app/build.gradle.kts")
      .withEnv("REDIS_A_NETWORK_ALIAS", REDIS_A_NETWORK_ALIAS)
      .withEnv("REDIS_A_PORT", Integer.toString(REDIS_PORT))
      .withEnv("REDIS_B_NETWORK_ALIAS", REDIS_B_NETWORK_ALIAS)
      .withEnv("REDIS_B_PORT", Integer.toString(REDIS_PORT))
      .withEnv("TOXIPROXY_NETWORK_ALIAS", TOXIPROXY_NETWORK_ALIAS)
      .withEnv("REDIS_PROXY_PORT", Integer.toString(REDIS_PROXY_PORT));
    testContainer.start();
    System.out.println("Test container name: " + testContainer.getContainerName());
  }

  @AfterAll
  public static void tearDown() {
    redisContainer_A.stop();
    redisContainer_B.stop();
    toxiproxyContainer.stop();
    testContainer.stop();
  }

  @Test
  public void testRunTestInContainer() throws UnsupportedOperationException, IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Integer exitCode = ExecInContainerUtility.execInContainer(testContainer, consumer, "./gradlew", "test-integration-container-redis");
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

}

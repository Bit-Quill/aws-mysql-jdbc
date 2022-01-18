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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.shaded.com.google.common.collect.ObjectArrays;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommunityContainerTest extends ContainerBaseTest {
  private static final List<MySQLContainer<?>> communityContainerList = new ArrayList<>();
  private static final String MYSQL_CONTAINER_IMAGE_NAME = "mysql:8.0.21";
  private static final String MYSQL_CONTAINER_NAME = "mysql-container";
  private static GenericContainer<?> communityTestContainer;
  private Network network;

  @BeforeEach
  void setUp() {
    network = Network.newNetwork();
  }

  @AfterEach
  void tearDown() {
    for (final MySQLContainer<?> dockerComposeContainer : communityContainerList) {
      dockerComposeContainer.stop();
    }
    communityTestContainer.stop();
  }

  @Test
  public void testRunCommunityTestInContainer()
      throws UnsupportedOperationException, IOException, InterruptedException {
    communityTestContainer = initializeTestContainer(network);
    communityContainerList.add(initializeMySQLContainers(network));

    runTest(communityTestContainer, "test-non-integration");
  }

  @Override
  GenericContainer<?> initializeTestContainer(final Network network) {
    final GenericContainer<?> container =
        createTestContainerTemplate("bq/community-test-container")
            .withNetworkAliases("community-" + TEST_CONTAINER_NETWORK_ALIAS)
            .withNetwork(network);
    container.addEnv(
        "TEST_MYSQL_PORT",
        String.valueOf(MYSQL_PORT));
    container.addEnv(
        "TEST_MYSQL_DOMAIN",
        MYSQL_CONTAINER_NAME);

    container.start();
    return container;
  }

  private MySQLContainer<?> initializeMySQLContainers(Network network) {
    final MySQLContainer<?> mySQLContainer =
        createMySQLContainer(network, MYSQL_CONTAINER_NAME,
            "--log-error-verbosity=4",
            "--default-authentication-plugin=sha256_password",
            "--sha256_password_public_key_path=/home/certdir/mykey.pub",
            "--sha256_password_private_key_path=/home/certdir/mykey.pem",
            "--caching_sha2_password_public_key_path=/home/certdir/mykey.pub",
            "--caching_sha2_password_private_key_path=/home/certdir/mykey.pem");

    mySQLContainer.start();
    return mySQLContainer;
  }

  private static MySQLContainer<?> createMySQLContainer(
      Network network,
      String networkAlias,
      String... commands) {
    final String[] defaultCommands = new String[] {
        "--local_infile=1",
        "--max_allowed_packet=40M",
        "--max-connections=2048",
        "--secure-file-priv=/var/lib/mysql",
        "--ssl-key=/home/certdir/server-key.pem",
        "--ssl-cert=/home/certdir/server-cert.pem",
        "--ssl-ca=/home/certdir/ca-cert.pem",
        "--plugin_dir=/home/plugin_dir"
    };

    String[] fullCommands = defaultCommands;

    if (commands.length != 0) {
      fullCommands = ObjectArrays.concat(defaultCommands, commands, String.class);
    }

    return new MySQLContainer<>(MYSQL_CONTAINER_IMAGE_NAME)
        .withNetwork(network)
        .withNetworkAliases(networkAlias)
        .withDatabaseName(TEST_DB)
        .withPassword("root")
        .withFileSystemBind(
            "src/test/config/ssl-test-certs/",
            "/home/certdir/",
            BindMode.READ_WRITE)
        .withFileSystemBind(
            "src/test/config/plugins/",
            "/home/plugin_dir/",
            BindMode.READ_WRITE)
        .withFileSystemBind(
            "src/test/config/docker-entrypoint-initdb.d",
            "/docker-entrypoint-initdb.d",
            BindMode.READ_WRITE)
        .withCommand(fullCommands);
  }
}

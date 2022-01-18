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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;

import testsuite.integration.utility.ConsoleConsumer;
import testsuite.integration.utility.ExecInContainerUtility;

import java.io.IOException;
import java.util.function.Consumer;

public abstract class ContainerBaseTest {
  static final String TEST_USERNAME = System.getenv("TEST_USERNAME");
  static final String TEST_PASSWORD = System.getenv("TEST_PASSWORD");
  static final String TEST_DB = "test";
  static final String TEST_CONTAINER_IMAGE_NAME = "openjdk:8-jdk-alpine";
  static final String TEST_CONTAINER_NETWORK_ALIAS = "test-container";
  static final int MYSQL_PORT = 3306;

  void runTest(GenericContainer<?> container, String task)
      throws IOException, InterruptedException {
    System.out.println("==== Container console feed ==== >>>>");
    Consumer<OutputFrame> consumer = new ConsoleConsumer();
    Integer exitCode =
        ExecInContainerUtility.execInContainer(container, consumer, "./gradlew", task);
    System.out.println("==== Container console feed ==== <<<<");
    assertEquals(0, exitCode, "Some tests failed.");
  }

  static GenericContainer<?> createTestContainerTemplate(String dockerImageName) {
    return new GenericContainer<>(
        new ImageFromDockerfile(dockerImageName, true)
            .withDockerfileFromBuilder(builder ->
                builder
                    .from(TEST_CONTAINER_IMAGE_NAME)
                    .run("mkdir", "app")
                    .workDir("/app")
                    .entryPoint("/bin/sh -c \"while true; do sleep 30; done;\"")
                    .build()))
        .withFileSystemBind("./.git", "/app/.git", BindMode.READ_WRITE)
        .withFileSystemBind("./build", "/app/build", BindMode.READ_WRITE)
        .withFileSystemBind("./config", "/app/config", BindMode.READ_WRITE)
        .withFileSystemBind("./docs", "/app/docs", BindMode.READ_WRITE)
        .withFileSystemBind("./src", "/app/src", BindMode.READ_WRITE)
        .withFileSystemBind("./gradle", "/app/gradle", BindMode.READ_WRITE)
        .withCopyFileToContainer(MountableFile.forHostPath("./gradlew"), "app/gradlew")
        .withCopyFileToContainer(MountableFile.forHostPath("./gradle.properties"), "app/gradle.properties")
        .withCopyFileToContainer(MountableFile.forHostPath("./build.gradle.kts"), "app/build.gradle.kts");
  }

  abstract GenericContainer<?> initializeTestContainer(final Network network);
}

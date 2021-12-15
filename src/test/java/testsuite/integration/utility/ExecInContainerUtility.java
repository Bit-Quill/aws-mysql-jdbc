package testsuite.integration.utility;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.DockerException;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.utility.TestEnvironment;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

public class ExecInContainerUtility {
  public static Integer execInContainer(GenericContainer<?> container, Consumer<OutputFrame> consumer, String... command)
    throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container, consumer, StandardCharsets.UTF_8, command);
  }

  public static Integer execInContainer(GenericContainer<?> container, Consumer<OutputFrame> consumer, Charset outputCharset, String... command)
    throws UnsupportedOperationException, IOException, InterruptedException {
    return execInContainer(container.getContainerInfo(), consumer, outputCharset, command);
  }

  public static Integer execInContainer(InspectContainerResponse containerInfo, Consumer<OutputFrame> consumer,
    Charset outputCharset, String... command)
    throws UnsupportedOperationException, IOException, InterruptedException {
    if (!TestEnvironment.dockerExecutionDriverSupportsExec()) {
      // at time of writing, this is the expected result in CircleCI.
      throw new UnsupportedOperationException(
              "Your docker daemon is running the \"lxc\" driver, which doesn't support \"docker exec\".");

    }

    if (!isRunning(containerInfo)) {
      throw new IllegalStateException("execInContainer can only be used while the Container is running");
    }

    String containerId = containerInfo.getId();
    String containerName = containerInfo.getName();

    DockerClient dockerClient = DockerClientFactory.instance().client();

    //log.debug("{}: Running \"exec\" command: {}", containerName, String.join(" ", command));
    final ExecCreateCmdResponse execCreateCmdResponse = dockerClient.execCreateCmd(containerId)
            .withAttachStdout(true).withAttachStderr(true).withCmd(command).exec();

    try (FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
      callback.addConsumer(OutputFrame.OutputType.STDOUT, consumer);
      callback.addConsumer(OutputFrame.OutputType.STDERR, consumer);

      dockerClient.execStartCmd(execCreateCmdResponse.getId()).exec(callback).awaitCompletion();
    }
    Integer exitCode = dockerClient.inspectExecCmd(execCreateCmdResponse.getId()).exec().getExitCode();
    return exitCode;
  }

  private static boolean isRunning(InspectContainerResponse containerInfo) {
    try {
      return containerInfo != null && containerInfo.getState().getRunning();
    } catch (DockerException e) {
      return false;
    }
  }


}

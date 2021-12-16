package testsuite.integration.utility;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class ConsoleConsumer extends BaseConsumer<org.testcontainers.containers.output.Slf4jLogConsumer> {

  private static final String LINE_BREAK_REGEX = "((\\r?\\n)|(\\r))";
  private static final String LINE_BREAK_AT_END_REGEX = LINE_BREAK_REGEX + "$";

  private boolean separateOutputStreams;

  public ConsoleConsumer() {
    this(false);
  }

  public ConsoleConsumer(boolean separateOutputStreams) {
    this.separateOutputStreams = separateOutputStreams;
  }

  public ConsoleConsumer withSeparateOutputStreams() {
    this.separateOutputStreams = true;
    return this;
  }

  @Override
  public void accept(OutputFrame outputFrame) {
    OutputFrame.OutputType outputType = outputFrame.getType();

    String utf8String = outputFrame.getUtf8String();
    //utf8String = utf8String.replaceAll(LINE_BREAK_AT_END_REGEX, "");

    switch (outputType) {
      case END:
        break;
      case STDOUT:
        System.out.print(utf8String);
        break;
      case STDERR:
        if (separateOutputStreams) {
          System.err.print(utf8String);
        } else {
          System.out.print(utf8String);
        }
        break;
      default:
        throw new IllegalArgumentException("Unexpected outputType " + outputType);
    }
  }
}

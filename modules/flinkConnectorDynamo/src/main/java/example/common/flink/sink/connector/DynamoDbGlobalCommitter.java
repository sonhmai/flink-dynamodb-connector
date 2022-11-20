package example.common.flink.sink.connector;

import org.apache.flink.api.connector.sink.GlobalCommitter;

import java.io.IOException;
import java.util.List;

class DynamoDbGlobalCommitter<CommT, GlobalCommT> implements GlobalCommitter<CommT, GlobalCommT> {

  @Override
  public List<GlobalCommT> filterRecoveredCommittables(List<GlobalCommT> globalCommittables) throws IOException {
    return null;
  }

  @Override
  public GlobalCommT combine(List<CommT> committables) throws IOException {
    return null;
  }

  @Override
  public List<GlobalCommT> commit(List<GlobalCommT> globalCommittables) throws IOException {
    return null;
  }

  @Override
  public void endOfInput() throws IOException {

  }

  @Override
  public void close() throws Exception {

  }
}

package example.common.flink.sink.connector;

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.List;

class DynamoDbCommitter<CommT> implements Committer<CommT> {

  @Override
  public List<CommT> commit(List<CommT> committables) throws IOException {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}

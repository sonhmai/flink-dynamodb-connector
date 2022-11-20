package example.common.flink.sink.connector;

import org.apache.flink.api.connector.sink.SinkWriter;

import java.io.IOException;
import java.util.List;

public class DynamoDbSinkWriter<InputT, CommT, WriterStateT>  implements SinkWriter<InputT, CommT, WriterStateT> {
  @Override
  public void write(InputT element, Context context) throws IOException {

  }

  @Override
  public List<CommT> prepareCommit(boolean flush) throws IOException {
    return null;
  }

  @Override
  public List<WriterStateT> snapshotState() throws IOException {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}

package example.common.flink.sink.connector;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

class DynamoDbUnifiedSink<InputT, CommT, WriterStateT, GlobalCommT>
    implements Sink<InputT, CommT, WriterStateT, GlobalCommT> {

  @Override
  public SinkWriter<InputT, CommT, WriterStateT> createWriter(InitContext context, List<WriterStateT> states)
      throws IOException {
    return new DynamoDbSinkWriter<>();
  }

  @Override
  public Optional<Committer<CommT>> createCommitter() throws IOException {
    return Optional.empty();
  }

  @Override
  public Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter() throws IOException {
    return Optional.empty();
  }

  @Override
  public Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer() {
    return Optional.empty();
  }

  @Override
  public Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer() {
    return Optional.empty();
  }

  @Override
  public Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer() {
    return Optional.empty();
  }
}

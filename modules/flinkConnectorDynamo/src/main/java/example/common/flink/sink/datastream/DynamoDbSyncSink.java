package example.common.flink.sink.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

public class DynamoDbSyncSink<In> extends RichSinkFunction<In> implements CheckpointedFunction {

    private final DynamoDbBatchingOutputFormat<In> outputFormat;

    public DynamoDbSyncSink(@Nonnull DynamoDbBatchingOutputFormat<In> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // init DynamoDB connection
    }

    @Override
    public void close() throws Exception {
        // close DynamoDB connection
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void invoke(In value, Context context) throws Exception {
        // this function called for every record. It is of course not optimized
        // if we write to every record to the DB as they come.
        // Maybe can use org.apache.flink.api.common.io.OutputFormat for batching
        // example org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat
        outputFormat.writeRecord(value);
    }
}

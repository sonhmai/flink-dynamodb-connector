package example.common.flink.sink.datastream;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class DynamoDbSyncSink<In>
    extends RichSinkFunction<In>
    implements CheckpointedFunction {

    private final DynamoDbBatchingOutputFormat<In> outputFormat;
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbSyncSink.class);

    public DynamoDbSyncSink(@Nonnull DynamoDbBatchingOutputFormat<In> outputFormat) {
        this.outputFormat = Preconditions.checkNotNull(outputFormat);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Opening dynamodb sink function...");
        RuntimeContext context = getRuntimeContext();
        outputFormat.setRuntimeContext(context);
        outputFormat.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing dynamodb sink function...");
        outputFormat.close();
    }

    @Override
    public void invoke(In value, Context context) throws Exception {
        // this function called for every record. It is of course not optimized
        // if we write to every record to the DB as they come.
        // Maybe can use org.apache.flink.api.common.io.OutputFormat for batching
        // example org.apache.flink.connector.jdbc.internal.JdbcBatchingOutputFormat
        outputFormat.writeRecord(value);
    }

    // checkpointing
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        LOG.info("Snapshotting dynamodb sink state...");
        outputFormat.snapshotState(context);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("Initializing dynamodb sink state...");
        outputFormat.initializeState(context);
    }
}

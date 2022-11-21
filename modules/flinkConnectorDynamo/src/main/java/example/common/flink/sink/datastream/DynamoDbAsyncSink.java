package example.common.flink.sink.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

class DynamoDbAsyncSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

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
    public void invoke(T value, Context context) throws Exception {
        super.invoke(value, context);
    }
}

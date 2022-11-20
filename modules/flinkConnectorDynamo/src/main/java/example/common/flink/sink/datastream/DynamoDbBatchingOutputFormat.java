package example.common.flink.sink.datastream;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A DynamoDB OutputFormat that supports batching records before writing to db
 */
public class DynamoDbBatchingOutputFormat<In>
extends RichOutputFormat<In>
implements Flushable {

    public static final int DEFAULT_FLUSH_MAX_SIZE = 2;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 100L;

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBatchingOutputFormat.class);
    protected transient DynamoDbClient ddb;
    protected transient ClientOverrideConfiguration config;
    private final AtomicInteger recordCount = new AtomicInteger(0);

    public DynamoDbBatchingOutputFormat(ClientOverrideConfiguration config) {
        LOG.info("Creating output format with config: {}", config);
        this.config = config;
    }

    @Override
    public void configure(Configuration parameters) {
        LOG.info("config: {}", config);
        LOG.info("configure() is called: {}", parameters);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            ddb = DynamoDbClient.builder().overrideConfiguration(config).build();
            LOG.info("List tables: {}", ddb.listTables());
        } catch (Exception e) {
            throw new IOException("unable to open DynamoDB connection", e);
        }
    }

    // synchronized to allow only 1 thread to execute this at the same time
    @Override
    public synchronized void writeRecord(In record) throws IOException {
        // this is called for every record
        int count = recordCount.incrementAndGet();
        LOG.info("Record count: {}", count);
        if (count > DEFAULT_FLUSH_MAX_SIZE) {
            flush();
            recordCount.set(0);
        }
    }

    @Override
    public void close() throws IOException {
        // TODO - flush all before closing
        LOG.info("Closing DynamoDb client...");
        ddb.close();
    }

    @Override
    public void flush() throws IOException {
        // TODO - implement retries
        LOG.info("Flushing records to DynamoDB...");
        Map<String, Collection<WriteRequest>> requestItems = new HashMap<>();
        // TODO - add records to Hashmap
        BatchWriteItemRequest bwiRequest = BatchWriteItemRequest.builder()
                .requestItems(requestItems)
                .build();
        ddb.batchWriteItem(bwiRequest);
    }
}

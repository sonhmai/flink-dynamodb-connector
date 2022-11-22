package example.flink.sink.datastream;

import example.common.flink.sink.datastream.DynamoDbBatchingOutputFormat;
import example.flink.sink.TestFixture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.io.IOException;
import java.util.*;

public class DynamoDbBatchingOutputFormatTestEntry
    extends DynamoDbBatchingOutputFormat<TestFixture.TestEntry> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBatchingOutputFormatTestEntry.class);

    public DynamoDbBatchingOutputFormatTestEntry(Properties configProps) {
        super(configProps);
    }

    @Override
    public synchronized void flush() throws IOException {
        // TODO - implement retries
        LOG.info("Flushing records to DynamoDB...");
        Collection<WriteRequest> writeRequests = new ArrayList<>();
        for (TestFixture.TestEntry testEntry : buffer) {
            HashMap<String, AttributeValue> writeAttributes = new HashMap<>();
            // TODO - make this model independent, currently hardcoding the attribute names
            writeAttributes.put("id", AttributeValue
                .builder()
                .s(testEntry.id.toString())
                .build()
            );
            writeAttributes.put("title", AttributeValue
                .builder()
                .s(testEntry.title)
                .build()
            );
            writeAttributes.put("author", AttributeValue
                .builder()
                .s(testEntry.author)
                .build()
            );
            // TODO - how to deal with null price, qty? toString will throw NullPointerException
            writeAttributes.put("price", AttributeValue
                .builder()
                .n(testEntry.price.toString())
                .build()
            );
            writeAttributes.put("qty", AttributeValue
                .builder()
                .n(testEntry.qty.toString())
                .build()
            );
            writeRequests.add(WriteRequest
                .builder()
                .putRequest(PutRequest
                    .builder()
                    .item(writeAttributes)
                    .build())
                .build());
        }
        Map<String, Collection<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put("books", writeRequests);
        BatchWriteItemRequest bwiRequest = BatchWriteItemRequest.builder()
            .requestItems(requestItems)
            .build();
        ddb.batchWriteItem(bwiRequest);
    }
}

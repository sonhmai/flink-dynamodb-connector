package example.flink.sink.fixture;

import example.flink.sink.TestFixture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.*;

public class DynamoDbBooksTable {
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBooksTable.class);
    private static final String TABLE_NAME = "books";

    public static CreateTableResponse createTable(DynamoDbClient ddb) {
        AttributeDefinition attributeDefinition = AttributeDefinition
            .builder()
            .attributeName("id")
            .attributeType(ScalarAttributeType.S)
            .build();
        KeySchemaElement keySchemaElement = KeySchemaElement
            .builder()
            .attributeName("id")
            .keyType(KeyType.HASH)
            .build();
        CreateTableRequest request = CreateTableRequest
            .builder()
            .tableName(TABLE_NAME)
            .attributeDefinitions(attributeDefinition)
            .keySchema(keySchemaElement)
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L)
                .build())
            .build();
        CreateTableResponse response = ddb.createTable(request);

        // wait until table is created, table creation is async per aws doc
        DescribeTableRequest describeTableRequest = DescribeTableRequest
            .builder()
            .tableName(TABLE_NAME)
            .build();
        DynamoDbWaiter waiter = ddb.waiter();
        waiter.waitUntilTableExists(describeTableRequest);

        LOG.info("Created table, response: {}", response);
        return response;
    }

    public static Set<TestFixture.TestEntry> selectBooks(DynamoDbClient ddb) {
        Set<TestFixture.TestEntry> books = new HashSet<>();
        // TODO - query dynamodb
        ScanRequest scanRequest = ScanRequest.builder()
            .tableName(TABLE_NAME)
            .build();
        ScanResponse response = ddb.scan(scanRequest);
        for (Map<String, AttributeValue> item : response.items()) {
            LOG.info("Item: {}", item);
            books.add(new TestFixture.TestEntry(
                Integer.parseInt(item.get("id").s()),
                item.get("title").s(),
                item.get("author").s(),
                Double.parseDouble(item.get("price").n()),
                Integer.parseInt(item.get("qty").n())
            ));
        }
        return books;
    }
}

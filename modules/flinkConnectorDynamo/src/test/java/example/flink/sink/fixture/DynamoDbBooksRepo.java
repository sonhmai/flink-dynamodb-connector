package example.flink.sink.fixture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DynamoDbBooksRepo {

    public static final TableSchema<TestEntry> booksTableSchema = TableSchema.fromBean(TestEntry.class);
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBooksRepo.class);
    private static final String TABLE_NAME = "books";
    private final DynamoDbClient ddb;
    private final DynamoDbEnhancedClient enhancedClient;

    public DynamoDbBooksRepo(DynamoDbClient ddb) {
        this.ddb = ddb;
        this.enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();
    }

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

    public Set<TestEntry> getAllBooks() {
        Set<TestEntry> books = new HashSet<>();
        DynamoDbTable<TestEntry> booksTable = enhancedClient.table(TABLE_NAME, booksTableSchema);
        LOG.info("Describe table: {}", booksTable.describeTable());
        Iterator<TestEntry> results = booksTable.scan().items().stream().iterator();
        while (results.hasNext()) {
            TestEntry testEntry = results.next();
            books.add(testEntry);
        }
        return books;
    }
}

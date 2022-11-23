package example.flink.sink.fixture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DynamoDbBooksRepo {

    public static final TableSchema<TestEntry> booksTableSchema = TableSchema.fromBean(TestEntry.class);
    private static final Logger LOG = LoggerFactory.getLogger(DynamoDbBooksRepo.class);
    private static final String TABLE_NAME = "books";
    private final DynamoDbClient ddb;
    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<TestEntry> booksTable;

    public DynamoDbBooksRepo(DynamoDbClient ddb) {
        this.ddb = ddb;
        this.enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();
        this.booksTable = enhancedClient.table(TABLE_NAME, booksTableSchema);
    }

    public void createTable() {
        booksTable.createTable();
        ddb.waiter().waitUntilTableExists(builder -> builder.tableName(TABLE_NAME));
    }

    public Set<TestEntry> getAllBooks() {
        Set<TestEntry> books = new HashSet<>();
        LOG.info("Describe table: {}", booksTable.describeTable());
        Iterator<TestEntry> results = booksTable.scan().items().stream().iterator();
        while (results.hasNext()) {
            TestEntry testEntry = results.next();
            books.add(testEntry);
        }
        return books;
    }
}

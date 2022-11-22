package example.flink.sink;

import example.flink.sink.fixture.TestEntry;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

public class TestFixture {

    // borrowing from a test in Flink itself
    public static final TestEntry[] TEST_DATA = {
        new TestEntry(1001, ("Java public for dummies"), ("Tan Ah Teck"), 11.11, 11),
        new TestEntry(1002, ("More Java for dummies"), ("Tan Ah Teck"), 22.22, 22),
        new TestEntry(1003, ("More Java for more dummies"), ("Mohammad Ali"), 33.33, 33),
        new TestEntry(1004, ("A Cup of Java"), ("Kumar"), 44.44, 44),
        new TestEntry(1005, ("A Teaspoon of Java"), ("Kevin Jones"), 55.55, 55),
        new TestEntry(1006, ("A Teaspoon of Java 1.4"), ("Kevin Jones"), 66.66, 66),
        new TestEntry(1007, ("A Teaspoon of Java 1.5"), ("Kevin Jones"), 77.77, 77),
        new TestEntry(1008, ("A Teaspoon of Java 1.6"), ("Kevin Jones"), 88.88, 88),
        new TestEntry(1009, ("A Teaspoon of Java 1.7"), ("Kevin Jones"), 99.99, 99),
        new TestEntry(1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), 12.12, 1010)
    };
}

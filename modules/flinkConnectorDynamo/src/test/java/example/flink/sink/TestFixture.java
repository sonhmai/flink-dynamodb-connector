package example.flink.sink;

import java.io.Serializable;
import java.util.Objects;

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
            new TestEntry(1010, ("A Teaspoon of Java 1.8"), ("Kevin Jones"), null, 1010)
    };

    public static class TestEntry implements Serializable {
        public final Integer id;
        public final String title;
        public final String author;
        public final Double price;
        public final Integer qty;

        public TestEntry(Integer id, String title, String author, Double price, Integer qty) {
            this.id = id;
            this.title = title;
            this.author = author;
            this.price = price;
            this.qty = qty;
        }

        @Override
        public String toString() {
            return "TestEntry{"
                    + "id="
                    + id
                    + ", title='"
                    + title
                    + '\''
                    + ", author='"
                    + author
                    + '\''
                    + ", price="
                    + price
                    + ", qty="
                    + qty
                    + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TestEntry)) {
                return false;
            }
            TestEntry testEntry = (TestEntry) o;
            return Objects.equals(id, testEntry.id)
                    && Objects.equals(title, testEntry.title)
                    && Objects.equals(author, testEntry.author)
                    && Objects.equals(price, testEntry.price)
                    && Objects.equals(qty, testEntry.qty);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, title, author, price, qty);
        }
    }
}

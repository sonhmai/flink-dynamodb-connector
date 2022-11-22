package example.flink.sink.fixture;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

import java.io.Serializable;
import java.util.Objects;

@DynamoDbBean
public class TestEntry implements Serializable {
    public Integer id;
    public String title;
    public String author;
    public Double price;
    public Integer qty;

    public TestEntry() {};

    public TestEntry(Integer id, String title, String author, Double price, Integer qty) {
        this.id = id;
        this.title = title;
        this.author = author;
        this.price = price;
        this.qty = qty;
    }

    @DynamoDbPartitionKey
    public Integer getId() {return this.id;}

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getQty() {
        return qty;
    }

    public void setQty(Integer qty) {
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestEntry testEntry = (TestEntry) o;
        return Objects.equals(id, testEntry.id) && Objects.equals(title, testEntry.title) && Objects.equals(author, testEntry.author) && Objects.equals(price, testEntry.price) && Objects.equals(qty, testEntry.qty);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, title, author, price, qty);
    }
}

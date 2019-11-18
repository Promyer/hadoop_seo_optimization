import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class TextTextPair implements WritableComparable<TextTextPair> {
    private Text first;
    private Text second;

    public TextTextPair() {
        set(new Text(), new Text());
    }

    public TextTextPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextTextPair(Text first, Text second) {
        set(first, second);
    }

    private void set(Text a, Text b) {
        first = a;
        second = b;
    }

    public Text getUrl() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(@Nonnull TextTextPair o) {
        int cmp = first.compareTo(o.first);
        return (cmp == 0) ? second.compareTo(o.second) : cmp;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 163 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextTextPair) {
            TextTextPair tp = (TextTextPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
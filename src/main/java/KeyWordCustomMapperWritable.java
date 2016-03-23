import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 3/22/2016.
 */
public class KeyWordCustomMapperWritable implements Writable {
    private int count;
    private int month ;

    public KeyWordCustomMapperWritable() {
    }

    public KeyWordCustomMapperWritable(int count, int month) {
        this.count = count;
        this.month = month;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void readFields(DataInput in) throws IOException {
        this.count = in.readInt();
        this.month = in.readInt();

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.count);
        out.writeInt(this.month);

    }

    @Override
    public String toString() {
        return  this.count + "\t" + this.month;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.count;
        result = prime * result + this.month;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        KeyWordCustomMapperWritable other = (KeyWordCustomMapperWritable) obj;
        if (this.count != other.count)
            return false;
        if (this.month != other.month)
            return false;
        return true;
    }
}


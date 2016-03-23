import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by George on 3/21/2016.
 */
public class KeyWordCustomReduceWritable implements Writable {
    private int countTotal;
    private int countTaxSeason;

    public KeyWordCustomReduceWritable() {
    }

    public KeyWordCustomReduceWritable(int countTotal, int countTaxSeason) {
        this.countTotal = countTotal;
        this.countTaxSeason = countTaxSeason;
    }

    public int getCountTotal() {
        return countTotal;
    }

    public void setCountTotal(int countTotal) {
        this.countTotal = countTotal;
    }

    public int getCountTaxSeason() {
        return countTaxSeason;
    }

    public void setCountTaxSeason(int countTaxSeason) {
        this.countTaxSeason = countTaxSeason;
    }

    public void readFields(DataInput in) throws IOException {
        this.countTotal = in.readInt();
        this.countTaxSeason = in.readInt();

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.countTotal);
        out.writeInt(this.countTaxSeason);

    }

    @Override
    public String toString() {
        return this.countTotal + "\t" + this.countTaxSeason;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.countTotal;
        result = prime * result + this.countTaxSeason;
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
        KeyWordCustomReduceWritable other = (KeyWordCustomReduceWritable) obj;
        if (this.countTotal != other.countTotal)
            return false;
        if (this.countTaxSeason != other.countTaxSeason)
            return false;
        return true;
    }
}

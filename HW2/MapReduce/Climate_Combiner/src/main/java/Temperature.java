import java.io.*;
import org.apache.hadoop.io.*;

public class Temperature implements Writable {
    private long sumTMIN, sumTMAX, countTMIN, countTMAX;

    Temperature() {}

    Temperature(long sumTMIN, long sumTMAX, long countTMIN, long countTMAX) {
        this.sumTMIN = sumTMIN;
        this.sumTMAX = sumTMAX;
        this.countTMIN = countTMIN;
        this.countTMAX = countTMAX;
    }

    public long getSumTMIN() {
        return sumTMIN;
    }

    public long getSumTMAX() {
        return sumTMAX;
    }

    public long getCountTMIN() {
        return countTMIN;
    }

    public long getCountTMAX() {
        return countTMAX;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(sumTMIN);
        out.writeLong(sumTMAX);
        out.writeLong(countTMIN);
        out.writeLong(countTMAX);
    }

    public void readFields(DataInput in) throws IOException {
        sumTMIN = in.readLong();
        sumTMAX = in.readLong();
        countTMIN = in.readLong();
        countTMAX = in.readLong();
    }

    public static Temperature read(DataInput in) throws IOException {
        Temperature temp = new Temperature();
        temp.readFields(in);
        return temp;
    }
}

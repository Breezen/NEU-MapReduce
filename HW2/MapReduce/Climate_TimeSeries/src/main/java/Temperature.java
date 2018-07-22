import java.io.*;
import org.apache.hadoop.io.*;

public class Temperature implements Writable {
    private int year;
    private long sumTMIN, sumTMAX, countTMIN, countTMAX;

    Temperature() {}
    Temperature(int year, long sumTMIN, long sumTMAX, long countTMIN, long countTMAX) {
        set(year, sumTMIN, sumTMAX, countTMIN, countTMAX);
    }

    public void set(int year, long sumTMIN, long sumTMAX, long countTMIN, long countTMAX) {
        this.year = year;
        this.sumTMIN = sumTMIN;
        this.sumTMAX = sumTMAX;
        this.countTMIN = countTMIN;
        this.countTMAX = countTMAX;
    }

    public int getYear() {
        return year;
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
        out.writeInt(year);
        out.writeLong(sumTMIN);
        out.writeLong(sumTMAX);
        out.writeLong(countTMIN);
        out.writeLong(countTMAX);
    }

    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
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

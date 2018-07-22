import java.io.*;
import org.apache.hadoop.io.*;

public class Temperature implements Writable {
    private long t;
    // low: true if TMIN, false if TMAX
    private boolean low;

    Temperature() {}

    Temperature(boolean low, long t) {
        this.t = t;
        this.low = low;
    }

    public long getT() {
        return t;
    }

    public boolean isLow() {
        return low;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(t);
        out.writeBoolean(low);
    }

    public void readFields(DataInput in) throws IOException {
        t = in.readLong();
        low = in.readBoolean();
    }

    public static Temperature read(DataInput in) throws IOException {
        Temperature temp = new Temperature();
        temp.readFields(in);
        return temp;
    }
}

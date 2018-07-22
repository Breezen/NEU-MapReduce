import java.io.*;
import org.apache.hadoop.io.*;

public class StationYear implements WritableComparable<StationYear> {
    private String stationID;
    private int year;

    public StationYear() {}
    public StationYear(String stationID, int year) { set(stationID, year); }

    public void set(String stationID, int year) {
        this.stationID = stationID;
        this.year = year;
    }
    public String getStationID() { return stationID; }
    public int getYear() { return year; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(stationID);
        out.writeInt(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        stationID = in.readUTF();
        year = in.readInt();
    }

    // make (StationYear, Temperature) of the same station go to the same reducer
    @Override
    public int hashCode() { return stationID.hashCode(); }

    @Override
    public int compareTo(StationYear sy) {
        int cmp = stationID.compareTo(sy.stationID);
        if (cmp != 0) {
            return cmp;
        }
        return compare(year, sy.year);
    }

    // for output
    @Override
    public String toString() {
        return stationID;
    }

    // helper
    public static int compare(int a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }
}

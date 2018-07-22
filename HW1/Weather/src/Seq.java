import java.util.List;
import java.util.HashMap;

public class Seq {
    private List<String> rawData;
    private HashMap<String, Integer> temp, count;

    public Seq(List<String> rawData) {
        this.rawData = rawData;
        temp = new HashMap<>();
        count = new HashMap<>();
    }

    private static long fib(int n) {
        if (n <= 1) return n;
        return fib(n - 2) + fib(n - 1);
    }

    public void compute() {
        for (String row : rawData) {
            String[] data = row.split(",");
            String station = data[0];
            if (data[2].equals("TMAX")) {
                if (temp.containsKey(station) && count.containsKey(station)) {
                    temp.put(station, temp.get(station) + Integer.parseInt(data[3]));
                    count.put(station, count.get(station) + 1);
                } else {
                    temp.put(station, Integer.parseInt(data[3]));
                    count.put(station, 1);
                }
                fib(17);
            }
        }
    }

    public HashMap<String, Double> getResult() {
        HashMap<String, Double> avg = new HashMap<>();
        for (String station : temp.keySet()) {
            avg.put(station, (double)temp.get(station) / count.get(station));
        }
        return avg;
    }
}

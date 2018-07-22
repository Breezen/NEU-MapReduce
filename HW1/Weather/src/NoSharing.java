import java.util.HashMap;
import java.util.List;

public class NoSharing implements Runnable{
    private Thread t;
    private String threadName;
    private List<String> rawData;
    private HashMap<String, Integer> temp, count;

    public NoSharing(String threadName, List<String> rawData) {
        this.threadName = threadName;
        this.rawData = rawData;
        temp = new HashMap<>();
        count = new HashMap<>();
    }

    private static long fib(int n) {
        if (n <= 1) return n;
        return fib(n - 2) + fib(n - 1);
    }

    public void run() {
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

    public void start() {
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

    public void join() {
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, Integer> getTemp() {
        return temp;
    }

    public HashMap<String, Integer> getCount() {
        return count;
    }
}

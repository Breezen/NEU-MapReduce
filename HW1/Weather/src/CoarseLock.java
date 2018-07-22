import java.util.HashMap;
import java.util.List;

public class CoarseLock implements Runnable {
    private Thread t;
    private String threadName;
    private List<String> rawData;
    private static HashMap<String, Integer> temp = new HashMap<>(), count = new HashMap<>();

    public CoarseLock(String threadName, List<String> rawData) {
        this.threadName = threadName;
        this.rawData = rawData;
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
                synchronized (temp) {
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
        getResult();
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

    public HashMap<String, Double> getResult() {
        HashMap<String, Double> avg = new HashMap<>();
        for (String station : temp.keySet()) {
            avg.put(station, (double)temp.get(station) / count.get(station));
        }
        return avg;
    }
}

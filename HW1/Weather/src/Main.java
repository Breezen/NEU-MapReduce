import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Main {
    private static ArrayList<String> rawData;

    private static void readData(String filename) {
        rawData = new ArrayList<>();
        try {
            Scanner sc = new Scanner(new File(filename));
            while (sc.hasNextLine()) {
                rawData.add(sc.nextLine());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(rawData.size() + " lines read.");
    }

    private static void seq() {
        long startTime = System.currentTimeMillis();
        Seq seq = new Seq(rawData);
        seq.compute();
        seq.getResult();
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time of Sequential: " + String.valueOf(elapsed) + "ms");
    }

    private static void noLock() {
        long startTime = System.currentTimeMillis();
        NoLock t1 = new NoLock("Thread1", rawData.subList(0, rawData.size() / 3));
        NoLock t2 = new NoLock("Thread2", rawData.subList(rawData.size() / 3, rawData.size() / 3 * 2));
        NoLock t3 = new NoLock("Thread3", rawData.subList(rawData.size() / 3 * 2, rawData.size()));
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time of NoLock: " + String.valueOf(elapsed) + "ms");
    }

    private static void coarseLock() {
        long startTime = System.currentTimeMillis();
        CoarseLock t1 = new CoarseLock("Thread1", rawData.subList(0, rawData.size() / 3));
        CoarseLock t2 = new CoarseLock("Thread2", rawData.subList(rawData.size() / 3, rawData.size() / 3 * 2));
        CoarseLock t3 = new CoarseLock("Thread3", rawData.subList(rawData.size() / 3 * 2, rawData.size()));
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time of CoarseLock: " + String.valueOf(elapsed) + "ms");
    }

    private static void fineLock() {
        long startTime = System.currentTimeMillis();
        FineLock t1 = new FineLock("Thread1", rawData.subList(0, rawData.size() / 3));
        FineLock t2 = new FineLock("Thread2", rawData.subList(rawData.size() / 3, rawData.size() / 3 * 2));
        FineLock t3 = new FineLock("Thread3", rawData.subList(rawData.size() / 3 * 2, rawData.size()));
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time of FineLock: " + String.valueOf(elapsed) + "ms");
    }

    private static void noSharing() {
        long startTime = System.currentTimeMillis();
        NoSharing t1 = new NoSharing("Thread1", rawData.subList(0, rawData.size() / 3));
        NoSharing t2 = new NoSharing("Thread2", rawData.subList(rawData.size() / 3, rawData.size() / 3 * 2));
        NoSharing t3 = new NoSharing("Thread3", rawData.subList(rawData.size() / 3 * 2, rawData.size()));
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        HashMap<String, Integer> tmp1 = t1.getTemp(), tmp2 = t2.getTemp(), tmp3 = t3.getTemp();
        HashMap<String, Integer> cnt1 = t1.getCount(), cnt2 = t2.getCount(), cnt3 = t3.getCount();
        HashMap<String, Integer> allTemp = new HashMap<>(), allCount = new HashMap<>();
        HashMap<String, Double> avg = new HashMap<>();
        for (String station : tmp1.keySet()) {
            if (allTemp.containsKey(station)) {
                allTemp.put(station, allTemp.get(station) + tmp1.get(station));
                allCount.put(station, allCount.get(station) + cnt1.get(station));
            } else {
                allTemp.put(station, tmp1.get(station));
                allCount.put(station, cnt1.get(station));
            }
        }
        for (String station : tmp2.keySet()) {
            if (allTemp.containsKey(station)) {
                allTemp.put(station, allTemp.get(station) + tmp2.get(station));
                allCount.put(station, allCount.get(station) + cnt2.get(station));
            } else {
                allTemp.put(station, tmp2.get(station));
                allCount.put(station, cnt2.get(station));
            }
        }
        for (String station : tmp3.keySet()) {
            if (allTemp.containsKey(station)) {
                allTemp.put(station, allTemp.get(station) + tmp3.get(station));
                allCount.put(station, allCount.get(station) + cnt3.get(station));
            } else {
                allTemp.put(station, tmp3.get(station));
                allCount.put(station, cnt3.get(station));
            }
        }
        for (String station : allTemp.keySet()) {
            avg.put(station, (double)allTemp.get(station) / allCount.get(station));
        }
        long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("Time of NoSharing: " + String.valueOf(elapsed) + "ms");
    }

    public static void main(String[] args){
        readData("1912.csv");
        seq();
        noLock();
        coarseLock();
        fineLock();
        noSharing();
    }
}

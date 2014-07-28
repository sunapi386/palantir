package com.palantir.GoodAgg;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Singleton - this file makes NUMCORES number of DsvAggregators,
 * each of which runs concurrently on a smaller set of files.
 * We split a given file (original) into NUMCORES number of files.
 * <p/>
 * Created by jsun on 7/26/14.
 */
public class ConcurrentAggregator {
    private final static int NUMCORES = Runtime.getRuntime().availableProcessors();
    private static ArrayList<FileWriter> fileWriters;
    private static List<Aggregator> aggregators;
    private static ExecutorService pool;
    private final List<String> filenames;

    public ConcurrentAggregator(String filename) throws IOException {

        // break the large file into smaller pieces
        fileWriters = new ArrayList<FileWriter>();
        filenames = new ArrayList<String>();
        System.out.println("Using " + NUMCORES + "cores");
        for (int i = 0; i < NUMCORES; i++) {
            File temp = File.createTempFile("temp-file-" + i, ".tmp");
            filenames.add(temp.getAbsolutePath());
            fileWriters.add(new FileWriter(temp.getAbsolutePath()));
            System.out.println("created temp file " + temp.getAbsolutePath());
        }
        int lineNum = 0;
        InputStreamReader isr = new FileReader(new File(filename));
        BufferedReader br = new BufferedReader(isr);
        while (br.ready()) {
            int filenum = (lineNum % NUMCORES);
            fileWriters.get(filenum).write(br.readLine() + "\n");
            lineNum++;
        }

        // make multiple instances of DsvAggregator
        aggregators = new ArrayList<Aggregator>();
        for (int i = 0; i < NUMCORES; i++) {
            String filepath = filenames.get(i).toString();
            System.out.println("Creating aggregator #" + i + " for file " + filepath);
            aggregators.add(new Aggregator(filepath));
        }

        // initialize executors
        pool = Executors.newFixedThreadPool(NUMCORES);
    }


    public int getPrefixAverage(String prefix) throws InterruptedException, ExecutionException {
        List<Future<?>> futures = new ArrayList<Future<?>>();
        for (Aggregator aggregator : aggregators) {
            aggregator.calltype = Aggregator.CALLTYPE.PREFIX;
            aggregator.prefix = prefix;
            futures.add(pool.submit(aggregator));
        }

        int results = 0;
        for (Future<?> futureAgg : futures) {
            while (!futureAgg.isDone());
            System.out.print(".");
            results += ((Aggregator) futureAgg.get()).prefix_result;
        }
        System.out.print("\n");
        pool.shutdown();
        pool.awaitTermination(12, TimeUnit.SECONDS);
        return results;
    }

    public List<String> getTop10OldestByState(String state) throws InterruptedException {
        for (Aggregator aggregator : aggregators) {
            aggregator.calltype = Aggregator.CALLTYPE.STATE;
            aggregator.state = state;
            pool.submit(aggregator);
        }
        pool.awaitTermination(12, TimeUnit.SECONDS);
        List<List<String>> results = new ArrayList<List<String>>();
        for (Aggregator aggregator : aggregators) {
            results.add(aggregator.state_result);
        }
        // TODO: collect them together and return results
        for (List<String> localtop : results) {
        }

        return null;
    }

    public String getRangeMax(int startAge, int endAge) throws InterruptedException {
        for (Aggregator aggregator : aggregators) {
            aggregator.calltype = Aggregator.CALLTYPE.RANGEMAX;
            aggregator.startAge = startAge;
            aggregator.endAge = endAge;
            pool.submit(aggregator);
        }
        pool.awaitTermination(12, TimeUnit.SECONDS);
        List<String> results = new ArrayList<String>();
        for (Aggregator aggregator : aggregators) {
            results.add(aggregator.rangemax_result);
        }
        // TODO: collect them together and return results

        return null;
    }
}

package com.palantir.BadAgg;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Singleton - this file makes NUMCORES number of DsvAggregators,
 * each of which runs concurrently on a smaller set of files.
 * We split a given file (original) into NUMCORES number of files.
 * <p/>
 * Created by jsun on 7/26/14.
 */
public class ConcurrentAggregator {
    private final static int NUMCORES = Runtime.getRuntime().availableProcessors();
    private static ArrayList<FileWriter> files;
    private static List<Aggregator> aggregators;
    private static ExecutorService pool;

    public ConcurrentAggregator(String filename) throws IOException {

        // break the large file into smaller pieces
        files = new ArrayList<FileWriter>();
        System.out.println("Using " + NUMCORES + "cores");
        for (int i = 0; i < NUMCORES; i++) {
            File temp = File.createTempFile("temp-file-" + i, ".tmp");
            files.add(new FileWriter(temp.getAbsolutePath()));
            System.out.println("created temp file " + temp.getAbsolutePath());
        }
        int lineNum = 0;
        InputStreamReader isr = new FileReader(new File(filename));
        BufferedReader br = new BufferedReader(isr);
        while (br.ready()) {
            int filenum = (lineNum % NUMCORES);
            files.get(filenum).write(br.readLine());
            lineNum++;
        }

        // make multiple instances of DsvAggregator
        aggregators = new ArrayList<Aggregator>();
        for (int i = 0; i < NUMCORES; i++) {
            String filepath = files.get(i).toString();
            System.out.println("Creating aggregator #" + i + " for file " + filepath);
            aggregators.add(new Aggregator(filepath));
        }

        // initialize executors
        pool = Executors.newFixedThreadPool(NUMCORES);
    }


    public int getPrefixAverage(String prefix) throws InterruptedException {
        for (Aggregator aggregator : aggregators) {
            aggregator.calltype = Aggregator.CALLTYPE.PREFIX;
            aggregator.prefix = prefix;
            pool.submit(aggregator);
        }
        pool.awaitTermination(60, TimeUnit.SECONDS);
        List<Integer> results = new LinkedList<Integer>();
        for (Aggregator aggregator : aggregators) {
            results.add(aggregator.prefix_result);
        }
        // TODO: collect them together and return results
        return 0;
    }

    public List<String> getTop10OldestByState(String state) throws InterruptedException {
        for (Aggregator aggregator : aggregators) {
            aggregator.calltype = Aggregator.CALLTYPE.STATE;
            aggregator.state = state;
            pool.submit(aggregator);
        }
        pool.awaitTermination(60, TimeUnit.SECONDS);
        List<List<String>> results = new ArrayList<List<String>>();
        for (Aggregator aggregator : aggregators) {
            results.add(aggregator.state_result);
        }
        // TODO: collect them together and return results


        return null;
    }

    public String getRangeMax(int startAge, int endAge) throws InterruptedException {
        for (Aggregator aggregator : aggregators) {
            aggregator.calltype = Aggregator.CALLTYPE.RANGEMAX;
            aggregator.startAge = startAge;
            aggregator.endAge = endAge;
            pool.submit(aggregator);
        }
        pool.awaitTermination(60, TimeUnit.SECONDS);
        List<String> results = new ArrayList<String>();
        for (Aggregator aggregator : aggregators) {
            results.add(aggregator.rangemax_result);
        }
        // TODO: collect them together and return results

        return null;
    }
}

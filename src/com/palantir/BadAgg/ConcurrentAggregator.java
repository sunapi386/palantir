package com.palantir.BadAgg;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
    List<DsvAggregator> aggregators;

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
        aggregators = new ArrayList<DsvAggregator>();
        for (int i = 0; i < NUMCORES; i++) {
            String filepath = files.get(i).toString();
            System.out.println("Creating aggregator #" + i + " for file " + filepath);
            aggregators.add(new DsvAggregator(filepath));
        }

    }


    public int getPrefixAverage(String prefix) {
        //TODO: aggregate and summarize
        return 0;
    }

    public List<String> getTop10OldestByState(String state) {
        return null;
    }

    public String getRangeMax(int startAge, int endAge) {
        return null;
    }
}

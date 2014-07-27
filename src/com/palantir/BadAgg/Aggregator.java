package com.palantir.BadAgg;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class Aggregator implements Runnable {
    static final String DELIM = ",";
    private List<List<String>> data = new ArrayList<List<String>>();
    public CALLTYPE calltype;
    public String prefix;
    public int prefix_result;
    public List<String> state_result;
    public String state;
    public String rangemax_result;
    public int startAge;
    public int endAge;

    Aggregator(String filename) throws IOException {

        BufferedReader br = null;
        InputStreamReader isr = null;
        int loaded = 0;
        try {
            isr = new FileReader(new File(filename));
            br = new BufferedReader(isr);
            while (br.ready()) {
                data.add(Arrays.asList(br.readLine().split(DELIM)));
                loaded++;
                if (loaded % 1000 == 0) {
                    System.out.print("\r" + loaded / 1000 + "K entries");
                }
            }
            System.out.println("\nFinished loading " + filename);

        } finally {
            if (isr != null) {
                isr.close();
            }
            if (br != null) {
                br.close();
            }
        }
    }

    public int getCount(String value) {
        Integer count = new Integer(0);
        for (List<String> row : data) {
            for (String column : row) {
                if (column.equalsIgnoreCase(value)) {
                    count++;
                }
            }
        }
        return count;
    }

    // hack expecting name, age, state csv format.
    public int getPrefixAverage(String prefix) {
        int sum = 0;
        int count = 0;
        prefix = prefix.toLowerCase();
        for (List<String> row : data) {
            String name = row.get(0);
            name = name.toLowerCase();
            int age = Integer.parseInt(row.get(1));
            if (name.startsWith(prefix)) {
                count++;
                sum += age;
            }
        }
        return count > 0 ? sum / count : 0;
    }

    public List<String> getTop10OldestByState(String state) {
        TreeMap<Integer, String> tm = new TreeMap<Integer, String>();
        for (List<String> row : data) {
            if (state.equalsIgnoreCase(row.get(2))) {
                tm.put(Integer.parseInt(row.get(1)), row.get(0));
            }
        }

        int i = 10;
        List<String> names = new ArrayList<String>(10);
        for (String name : tm.values()) {
            names.add(name);
            if (--i <= 0) {
                break;
            }
        }
        return names;
    }

    public String getRangeMax(int startAge, int endAge) {
        Map<String, Integer> countsByState = new HashMap<String, Integer>();
        for (List<String> row : data) {
            int age = Integer.parseInt(row.get(1));
            String state = row.get(2);
            if (age >= startAge && age <= endAge) {
                Integer count = countsByState.get(state);
                if (count != null) {
                    countsByState.put(state, count + 1);
                } else {
                    countsByState.put(state, 1);
                }
            }
        }

        String maxState = "";
        int maxCount = 0;
        for (Entry<String, Integer> ent : countsByState.entrySet()) {
            if (ent.getValue() > maxCount) {
                maxState = ent.getKey();
                maxCount = ent.getValue();
            }
        }

        return maxState;
    }

    @Override
    public void run() {
        switch (calltype) {
            case PREFIX:
                prefix_result = getPrefixAverage(prefix);
                break;
            case STATE:
                state_result = getTop10OldestByState(state);
                break;
            case RANGEMAX:
                rangemax_result = getRangeMax(startAge, endAge);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    static enum CALLTYPE {
        PREFIX, STATE, RANGEMAX;
    }
}

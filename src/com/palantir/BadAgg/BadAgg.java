package com.palantir.BadAgg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class BadAgg {

    public static void main(String[] args) throws IOException {
        int mb = 1024 * 1024;
        System.out.println("BadAgg. Total memory: " + Runtime.getRuntime().totalMemory() / mb + "MB");
//        	Scanner sc = new Scanner(System.in);
        System.out.println("Loading...");
        DsvAggregator agg = new DsvAggregator("rand.data"/*sc.nextLine()*/);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("average <prefix of name>            => find all names that have that prefix and find the average of those");
        System.out.println("top10 <state>                       => find top 10 oldest names by age");
        System.out.println("rangemax <start age> <end age>      => finds a state that has the most number of people between the start and end age");
        while (true) {
            System.out.print(">>> ");
            String[] command = br.readLine().split(" ");
            if (command[0].equalsIgnoreCase("average")) {
                System.out.println(agg.getPrefixAverage(command[1]));
            } else if (command[0].equalsIgnoreCase("top10")) {
                System.out.println(agg.getTop10OldestByState(command[1]));
            } else if (command[0].equalsIgnoreCase("rangemax")) {
                System.out.println(agg.getRangeMax(Integer.parseInt(command[1]), Integer.parseInt(command[2])));
            } else {
                System.out.println("Unknown command, try again");
            }
        }
    }
}

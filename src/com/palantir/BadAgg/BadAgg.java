package com.palantir.BadAgg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class BadAgg {

    public static void main(String[] args) {
        try {
        	Scanner sc = new Scanner(System.in);
            DsvAggregator agg = new DsvAggregator(sc.nextLine());
            System.out.println("read whole csv");
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String [] command = br.readLine().split(" ");
                if (command[0].equalsIgnoreCase("average")) {
                    System.out.println(agg.getPrefixAverage(command[1]));
                } else if (command[0].equalsIgnoreCase("top10")) {
                    System.out.println(agg.getTop10OldestByState(command[1]));
                } else if (command[0].equalsIgnoreCase("rangemax")) {
                    System.out.println(agg.getRangeMax(Integer.parseInt(command[1]), Integer.parseInt(command[2])));
                } else {
                    return;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}

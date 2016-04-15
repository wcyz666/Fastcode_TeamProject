package main;

import util.SimpleParser;

public class Entry {
    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);
        String program = parser.get("program");

        System.out.println("Running program " + program + "");

        long start = System.currentTimeMillis();

        if (program.equals("itemcount"))
            itemcount.Driver.main(args);
        else if (program.equals("SON")) {
            itemcountSON.Driver.main(args);
        }
        else {
            System.out.println("Unknown program!");
            System.exit(1);
        }

        long end = System.currentTimeMillis();

        System.out.println(String.format("Runtime for program %s: %d ms", program,
                end - start));
    }
}

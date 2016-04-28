package aprioriMapreduce;

import job.Optimizedjob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import util.SimpleParser;

import java.io.IOException;

public class Driver {


    public static final int MIN_SUPPORT= 1500;

    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        //String pass1File = parser.get("pass");
        //String N = parser.get("n");

        getPass1Item(input, "tmp/pass1");
        //getPass2Item(input, "tmp/pass1", "tmp/pass2");
        //getPass3Item(input, "tmp/pass2", output);
    }

    private static void getPass1Item(String input, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Compute NGram Count using SON");
        //job.setMapJobs(15);
        job.setClasses(AprioriPass1Mapper.class, AprioriPass1Reducer.class, AprioriPass1Reducer.class);
        //job.setClasses(ItemCountSONPass2Mapper.class, ItemCountSONPass2Reducer.class, ItemCountSONPass2Reducer.class);
        job.setMapOutputClasses(IntWritable.class, NullWritable.class);
        //job.setParameter("n", N);
        //job.setParameter("pass1File", pass1File);
        job.setParameter("mapreduce.map.memory.mb", "6000");
        job.run();
    }


    /*
    private static void getPass2Item(String input, String preSet, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Compute NGram Count using SON");
        //job.setMapJobs(15);
        job.setClasses(AprioriPass2Mapper.class, AprioriPass2Reducer.class, AprioriPass2Reducer.class);
        job.setMapOutputClasses(IntWritable.class, NullWritable.class);
        //job.setParameter("n", N);
        //job.setParameter("pass1File", pass1File);
        job.setParameter("mapreduce.map.memory.mb", "6000");
        job.run();
    }

    private static void getPass3Item(String input, String preSet, String output)
            throws IOException, ClassNotFoundException, InterruptedException {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Compute NGram Count using SON");
        //job.setMapJobs(15);
        job.setClasses(AprioriPass3Mapper.class, AprioriPass3Reducer.class, AprioriPass3Reducer.class);
        job.setMapOutputClasses(IntWritable.class, NullWritable.class);
        //job.setParameter("n", N);
        //job.setParameter("pass1File", pass1File);
        job.setParameter("mapreduce.map.memory.mb", "6000");
        job.run();
    }
*/


}

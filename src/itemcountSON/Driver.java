package itemcountSON;

import job.Optimizedjob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import util.SimpleParser;

import java.io.IOException;

public class Driver {

    public static void main(String args[]) throws Exception {
        SimpleParser parser = new SimpleParser(args);

        String input = parser.get("input");
        String output = parser.get("output");
        String pass1File = parser.get("pass");
        String N = parser.get("n");

        getJobFeatureVector(input, output, N, pass1File);
    }

    private static void getJobFeatureVector(String input, String output, String N, String pass1File)
            throws IOException, ClassNotFoundException, InterruptedException {
        Optimizedjob job = new Optimizedjob(new Configuration(), input, output,
                "Compute NGram Count using SON");
        //job.setMapJobs(15);
        job.setClasses(ItemCountSONMapper.class, ItemCountSONReducer.class, ItemCountSONReducer.class);
        //job.setClasses(ItemCountSONPass2Mapper.class, ItemCountSONPass2Reducer.class, ItemCountSONPass2Reducer.class);
        job.setMapOutputClasses(IntWritable.class, NullWritable.class);
        job.setParameter("n", N);
        job.setParameter("pass1File", pass1File);
        job.setParameter("mapreduce.map.memory.mb", "6000");
        job.run();
    }


}
